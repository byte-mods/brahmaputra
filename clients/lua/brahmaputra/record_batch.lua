-- Record batch codec.
--
-- The broker never re-encodes a batch: it validates the header, stamps
-- base_offset and leader_epoch in place (both precede the CRC, so it stays
-- valid) and writes these exact bytes to disk. An encoding slip here
-- corrupts the log rather than failing a request.
--
-- Layout (big-endian): base_offset i64, batch_length i32, leader_epoch
-- i32, magic i8, crc u32 (CRC32C over everything after it), attributes
-- u16, last_offset_delta i32, max_timestamp i64, [v2: producer_id i64,
-- producer_epoch i16, base_sequence i32], then the (possibly compressed)
-- records. Inside each record varints are *plain*, not zigzag, except the
-- timestamp delta.
--
-- Records are tables {key=?string, value=?string, timestampDelta=int,
-- headers={{key=string, value=?string}, ...}}. A nil value is a tombstone
-- and stays distinct from "" all the way through; likewise nil and "" keys
-- and header values.

local protocol = require("brahmaputra.protocol")
local hash = require("brahmaputra.hash")
local compression = require("brahmaputra.compression")

local M = {}

M.HEADER_LEN = 12
M.MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8
local PRODUCER_EXTENSION_LEN = 8 + 2 + 4
M.MAGIC_V1 = 1
M.MAGIC_V2 = 2

local COMPRESSION_MASK = 0x0007
local HEADERS_BIT = 0x0008
-- Some record has a null value (a tombstone). Set only when one is present,
-- so a batch without one encodes as it always did; it widens value lengths
-- to length+1 with 0 meaning null.
local NULL_VALUE_BIT = 0x0040

local putUvarint = protocol.putUvarint
local getUvarint = protocol.getUvarint
local protoError = protocol.protoError

--- Encode records into one batch. Returns the batch bytes.
function M.encode(records, maxTimestamp, codec)
  codec = codec or compression.NONE
  local hasHeaders, hasNullValues = false, false
  for _, r in ipairs(records) do
    if r.headers and #r.headers > 0 then hasHeaders = true end
    if r.value == nil then hasNullValues = true end
  end

  local payload = {}
  for _, r in ipairs(records) do
    local rec = {}
    if r.key == nil then
      putUvarint(rec, 0)
    else
      putUvarint(rec, #r.key + 1)
      rec[#rec + 1] = r.key
    end
    local value = r.value
    if hasNullValues then
      if value == nil then
        putUvarint(rec, 0)
      else
        putUvarint(rec, #value + 1)
        rec[#rec + 1] = value
      end
    else
      putUvarint(rec, #value)
      rec[#rec + 1] = value
    end
    putUvarint(rec, protocol.zigzag64(r.timestampDelta or 0))
    if hasHeaders then
      local headers = r.headers or {}
      putUvarint(rec, #headers)
      for _, h in ipairs(headers) do
        putUvarint(rec, #h.key)
        rec[#rec + 1] = h.key
        if h.value == nil then
          putUvarint(rec, 0)
        else
          putUvarint(rec, #h.value + 1)
          rec[#rec + 1] = h.value
        end
      end
    end
    local body = table.concat(rec)
    putUvarint(payload, #body)
    payload[#payload + 1] = body
  end

  local compressed = compression.compress(codec, table.concat(payload))
  local attributes = codec & COMPRESSION_MASK
  if hasHeaders then attributes = attributes | HEADERS_BIT end
  if hasNullValues then attributes = attributes | NULL_VALUE_BIT end

  local afterCrc = string.pack(">I2i4i8", attributes, math.max(#records - 1, 0), maxTimestamp) .. compressed
  return string.pack(">i8i4i4B", 0, M.MIN_BATCH_LENGTH + #compressed, 0, M.MAGIC_V1)
    .. string.pack(">I4", hash.crc32c(afterCrc))
    .. afterCrc
end

local function take(data, pos, length, stop)
  if length < 0 or pos + length - 1 > stop then
    protoError("record field runs past its record")
  end
  return string.sub(data, pos, pos + length - 1), pos + length
end

local function decodeRecords(payload, hasHeaders, hasNullValues)
  local records = {}
  local pos, total = 1, #payload
  while pos <= total do
    local size
    size, pos = getUvarint(payload, pos)
    -- compared unsigned: a length near 2^64 must not wrap negative
    if math.ult(total - pos + 1, size) then protoError("truncated record") end
    local stop = pos + size - 1

    local key, value, n
    n, pos = getUvarint(payload, pos, stop)
    if n ~= 0 then
      if math.ult(stop - pos + 2, n) then protoError("record key runs past its record") end
      key, pos = take(payload, pos, n - 1, stop)
    end

    n, pos = getUvarint(payload, pos, stop)
    if hasNullValues and n == 0 then
      value = nil -- a tombstone: what distinguishes a deletion from ""
    else
      if hasNullValues then n = n - 1 end
      if math.ult(stop - pos + 1, n) then protoError("record value runs past its record") end
      value, pos = take(payload, pos, n, stop)
    end

    local delta
    delta, pos = getUvarint(payload, pos, stop)
    delta = protocol.unzigzag64(delta)

    local headers = {}
    if hasHeaders then
      local count
      count, pos = getUvarint(payload, pos, stop)
      -- a count larger than the bytes left is corrupt; allocating on it
      -- would let a two-byte record ask for gigabytes
      if math.ult(stop - pos + 1, count) then protoError("record header count exceeds record") end
      for i = 1, count do
        local hk, hv
        n, pos = getUvarint(payload, pos, stop)
        if math.ult(stop - pos + 1, n) then protoError("header key runs past its record") end
        hk, pos = take(payload, pos, n, stop)
        n, pos = getUvarint(payload, pos, stop)
        if n ~= 0 then
          if math.ult(stop - pos + 2, n) then protoError("header value runs past its record") end
          hv, pos = take(payload, pos, n - 1, stop)
        end
        headers[i] = { key = hk, value = hv }
      end
    end
    if pos ~= stop + 1 then protoError("trailing bytes in record") end
    records[#records + 1] = { key = key, value = value, timestampDelta = delta, headers = headers }
  end
  return records
end

--- Decode one batch starting at 1-based `offset` of `data`. Returns the
--- batch {baseOffset, maxTimestamp, records} and the position just past it.
function M.decode(data, offset)
  local length = #data
  if length - offset + 1 < M.HEADER_LEN then protoError("truncated batch header") end
  local baseOffset, batchLength = string.unpack(">i8i4", data, offset)
  if batchLength < M.MIN_BATCH_LENGTH then
    -- also rejects a negative length
    protoError("batch_length " .. batchLength .. " too small")
  end
  local bodyAt = offset + M.HEADER_LEN
  local stop = bodyAt + batchLength - 1
  if stop > length then protoError("truncated batch body") end

  local magic = string.byte(data, bodyAt + 4)
  if magic ~= M.MAGIC_V1 and magic ~= M.MAGIC_V2 then protoError("unsupported magic " .. magic) end
  local crcAt = bodyAt + 5
  local stored = string.unpack(">I4", data, crcAt)
  local computed = hash.crc32c(data, crcAt + 4, stop)
  if stored ~= computed then
    protoError(string.format("crc mismatch: stored 0x%08x, computed 0x%08x", stored, computed))
  end

  local cursor = crcAt + 4
  local attributes = string.unpack(">I2", data, cursor)
  local maxTimestamp = string.unpack(">i8", data, cursor + 6)
  cursor = cursor + 14
  if magic == M.MAGIC_V2 then cursor = cursor + PRODUCER_EXTENSION_LEN end
  if cursor > stop + 1 then protoError("batch header runs past its body") end

  local payload = compression.decompress(attributes & COMPRESSION_MASK, string.sub(data, cursor, stop))
  local records = decodeRecords(payload, attributes & HEADERS_BIT ~= 0, attributes & NULL_VALUE_BIT ~= 0)
  return { baseOffset = baseOffset, maxTimestamp = maxTimestamp, records = records }, stop + 1
end

return M

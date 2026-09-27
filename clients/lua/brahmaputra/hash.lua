-- CRC32C (Castagnoli) and Kafka's murmur2.
--
-- Record batches are checksummed with CRC32C, not zlib's CRC32, so
-- zlib.crc32 is the wrong function. murmur2 is transcribed from Kafka so a
-- key lands on the same partition as from any other client:
-- murmur2("") == 275646681.
--
-- Both keep every intermediate as an unsigned 32-bit value in a 64-bit Lua
-- integer by masking. Multiplication may wrap past 2^63 but wrapping keeps
-- the low 32 bits exact, and `>>` on a masked value is Java's `>>>`.

local M = {}

local MASK = 0xffffffff
local byte = string.byte

local T0, T1, T2, T3 = {}, {}, {}, {}
for i = 0, 255 do
  local c = i
  for _ = 1, 8 do
    if c & 1 == 1 then c = (c >> 1) ~ 0x82f63b78 else c = c >> 1 end
  end
  T0[i] = c
end
for i = 0, 255 do
  T1[i] = (T0[i] >> 8) ~ T0[T0[i] & 0xff]
  T2[i] = (T1[i] >> 8) ~ T0[T1[i] & 0xff]
  T3[i] = (T2[i] >> 8) ~ T0[T2[i] & 0xff]
end

--- CRC32C of data[i..j] (1-based, inclusive; defaults to the whole string).
function M.crc32c(data, i, j)
  i = i or 1
  j = j or #data
  local crc = MASK
  local k = i
  -- slicing-by-4: four bytes per iteration
  local stop4 = j - 3
  while k <= stop4 do
    local a, b, c, d = byte(data, k, k + 3)
    crc = crc ~ (a | (b << 8) | (c << 16) | (d << 24))
    crc = T3[crc & 0xff] ~ T2[(crc >> 8) & 0xff] ~ T1[(crc >> 16) & 0xff] ~ T0[(crc >> 24) & 0xff]
    k = k + 4
  end
  while k <= j do
    crc = T0[(crc ~ byte(data, k)) & 0xff] ~ (crc >> 8)
    k = k + 1
  end
  return crc ~ MASK
end

local SEED = 0x9747b28c
local M2 = 0x5bd1e995

--- Kafka's murmur2 as an unsigned 32-bit value.
function M.murmur2(data)
  local length = #data
  local h = (SEED ~ length) & MASK
  local chunks = length // 4
  for c = 0, chunks - 1 do
    local k = string.unpack("<I4", data, c * 4 + 1)
    k = (k * M2) & MASK
    k = k ~ (k >> 24)
    k = (k * M2) & MASK
    h = (h * M2) & MASK
    h = h ~ k
  end
  local tail = chunks * 4
  local rest = length - tail
  if rest >= 3 then h = h ~ (byte(data, tail + 3) << 16) end
  if rest >= 2 then h = h ~ (byte(data, tail + 2) << 8) end
  if rest >= 1 then
    h = h ~ byte(data, tail + 1)
    h = (h * M2) & MASK
  end
  h = h ~ (h >> 13)
  h = (h * M2) & MASK
  h = h ~ (h >> 15)
  return h
end

--- Kafka's default partitioner: positive(murmur2(key)) % count, as an index
--- into a partition list sorted ascending.
function M.partitionIndex(key, count)
  return (M.murmur2(key) & 0x7fffffff) % count
end

return M

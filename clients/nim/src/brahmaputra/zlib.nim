## Minimal binding to the system zlib, used for the built-in gzip codec.
##
## Nim's standard library has no deflate implementation, so this binds
## `libz.so.1` directly (loaded at program start through `dynlib`). Build
## with `-d:brahmaputraZlib=false` to leave it out entirely; gzip then has
## to be registered with `registerCodec` like lz4/zstd/snappy.

const zlibLib = "libz.so.1"

type
  ZStream {.pure, final.} = object
    nextIn: ptr uint8
    availIn: cuint
    totalIn: culong
    nextOut: ptr uint8
    availOut: cuint
    totalOut: culong
    msg: cstring
    state: pointer
    zalloc: pointer
    zfree: pointer
    opaque: pointer
    dataType: cint
    adler: culong
    reserved: culong

  ZlibError* = object of CatchableError

const
  zOk = 0.cint
  zStreamEnd = 1.cint
  zBufError = -5.cint
  zFinish = 4.cint
  zNoFlush = 0.cint
  zDeflated = 8.cint
  zDefaultCompression = -1.cint
  zDefaultStrategy = 0.cint

proc zlibVersion(): cstring {.cdecl, importc: "zlibVersion", dynlib: zlibLib.}
proc deflateInit2u(strm: var ZStream, level, meth, windowBits, memLevel,
                   strategy: cint, version: cstring, streamSize: cint): cint
  {.cdecl, importc: "deflateInit2_", dynlib: zlibLib.}
proc deflate(strm: var ZStream, flush: cint): cint
  {.cdecl, importc: "deflate", dynlib: zlibLib.}
proc deflateEnd(strm: var ZStream): cint
  {.cdecl, importc: "deflateEnd", dynlib: zlibLib.}
proc deflateBound(strm: var ZStream, sourceLen: culong): culong
  {.cdecl, importc: "deflateBound", dynlib: zlibLib.}
proc inflateInit2u(strm: var ZStream, windowBits: cint, version: cstring,
                   streamSize: cint): cint
  {.cdecl, importc: "inflateInit2_", dynlib: zlibLib.}
proc inflate(strm: var ZStream, flush: cint): cint
  {.cdecl, importc: "inflate", dynlib: zlibLib.}
proc inflateReset(strm: var ZStream): cint
  {.cdecl, importc: "inflateReset", dynlib: zlibLib.}
proc inflateEnd(strm: var ZStream): cint
  {.cdecl, importc: "inflateEnd", dynlib: zlibLib.}

proc gzipCompress*(data: string): string =
  ## Compresses `data` into a single gzip member (windowBits 15 + 16).
  var strm: ZStream
  if deflateInit2u(strm, zDefaultCompression, zDeflated, 31, 8,
                   zDefaultStrategy, zlibVersion(), sizeof(ZStream).cint) != zOk:
    raise newException(ZlibError, "deflateInit2 failed")
  try:
    let bound = int(deflateBound(strm, culong(data.len)))
    result = newString(bound + 64)
    strm.nextIn = if data.len > 0: cast[ptr uint8](unsafeAddr data[0]) else: nil
    strm.availIn = cuint(data.len)
    strm.nextOut = cast[ptr uint8](addr result[0])
    strm.availOut = cuint(result.len)
    let rc = deflate(strm, zFinish)
    if rc != zStreamEnd:
      raise newException(ZlibError, "deflate did not finish (" & $rc & ")")
    result.setLen(int(strm.totalOut))
  finally:
    discard deflateEnd(strm)

proc gzipDecompress*(data: string, limit: int): string =
  ## Inflates one or more concatenated gzip members, refusing to produce
  ## more than `limit` bytes so a corrupt or hostile batch cannot make this
  ## process allocate without bound.
  var strm: ZStream
  if inflateInit2u(strm, 15 + 16, zlibVersion(), sizeof(ZStream).cint) != zOk:
    raise newException(ZlibError, "inflateInit2 failed")
  try:
    result = newString(max(64, min(limit, data.len * 4)))
    var produced = 0
    strm.nextIn = if data.len > 0: cast[ptr uint8](unsafeAddr data[0]) else: nil
    strm.availIn = cuint(data.len)
    while true:
      if produced == result.len:
        if result.len >= limit:
          raise newException(ZlibError, "gzip payload exceeds the decompression limit")
        result.setLen(min(limit, result.len * 2))
      strm.nextOut = cast[ptr uint8](addr result[produced])
      strm.availOut = cuint(result.len - produced)
      let before = strm.availOut
      let rc = inflate(strm, zNoFlush)
      produced += int(before - strm.availOut)
      if rc == zStreamEnd:
        if strm.availIn == 0:
          break
        # Another gzip member follows.
        if inflateReset(strm) != zOk:
          raise newException(ZlibError, "inflateReset failed")
        continue
      if rc == zBufError and strm.availIn == 0 and strm.availOut > 0:
        raise newException(ZlibError, "truncated gzip payload")
      if rc != zOk and rc != zBufError:
        raise newException(ZlibError, "corrupt gzip payload (" & $rc & ")")
    result.setLen(produced)
  finally:
    discard inflateEnd(strm)

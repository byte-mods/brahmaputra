// Compression codec registry.
//
// none is always available. gzip is built in when the library is compiled
// with BRAHMAPUTRA_WITH_GZIP (zlib). lz4, zstd and snappy are opt-in via
// registerCodec so an application that does not want those dependencies
// does not acquire them by using this client.
#include <map>
#include <mutex>

#include "brahmaputra/protocol.hpp"

#ifdef BRAHMAPUTRA_HAVE_ZLIB
#include <zlib.h>
#endif

namespace brahmaputra {

namespace {

// Capped so a corrupt or hostile batch cannot name gigabytes of output that
// this process allocates before it can reject it.
constexpr std::size_t kMaxDecompressedBytes = 256u * 1024u * 1024u;

struct CodecPair {
    CodecFn compress;
    CodecFn decompress;
};

#ifdef BRAHMAPUTRA_HAVE_ZLIB
Bytes gzipCompress(const Bytes& input) {
    z_stream zs{};
    // windowBits 15 + 16 selects the gzip wrapper rather than raw zlib.
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        throw Error("gzip: deflateInit2 failed");
    }
    Bytes out(deflateBound(&zs, static_cast<uLong>(input.size())) + 32);
    zs.next_in = const_cast<Bytef*>(input.data());
    zs.avail_in = static_cast<uInt>(input.size());
    zs.next_out = out.data();
    zs.avail_out = static_cast<uInt>(out.size());
    int rc = deflate(&zs, Z_FINISH);
    std::size_t produced = zs.total_out;
    deflateEnd(&zs);
    if (rc != Z_STREAM_END) throw Error("gzip: deflate did not finish");
    out.resize(produced);
    return out;
}

Bytes gzipDecompress(const Bytes& input) {
    z_stream zs{};
    // 15 + 32 auto-detects the gzip or zlib wrapper.
    if (inflateInit2(&zs, 15 + 32) != Z_OK) throw Error("gzip: inflateInit2 failed");
    Bytes out;
    std::uint8_t chunk[64 * 1024];
    zs.next_in = const_cast<Bytef*>(input.data());
    zs.avail_in = static_cast<uInt>(input.size());
    int rc = Z_OK;
    while (rc != Z_STREAM_END) {
        zs.next_out = chunk;
        zs.avail_out = sizeof chunk;
        rc = inflate(&zs, Z_NO_FLUSH);
        if (rc != Z_OK && rc != Z_STREAM_END) {
            inflateEnd(&zs);
            throw Error("gzip: corrupt stream");
        }
        out.insert(out.end(), chunk, chunk + (sizeof chunk - zs.avail_out));
        if (out.size() > kMaxDecompressedBytes) {
            inflateEnd(&zs);
            throw Error("gzip: decompressed batch exceeds 256 MiB");
        }
        if (rc != Z_STREAM_END && zs.avail_in == 0 && zs.avail_out != 0) {
            inflateEnd(&zs);
            throw Error("gzip: truncated stream");
        }
    }
    inflateEnd(&zs);
    return out;
}
#endif

std::mutex& registryMutex() {
    static std::mutex m;
    return m;
}

std::map<Compression, CodecPair>& registry() {
    static std::map<Compression, CodecPair> codecs = [] {
        std::map<Compression, CodecPair> m;
#ifdef BRAHMAPUTRA_HAVE_ZLIB
        m[Compression::Gzip] = CodecPair{gzipCompress, gzipDecompress};
#endif
        return m;
    }();
    return codecs;
}

CodecPair lookup(Compression codec) {
    std::lock_guard<std::mutex> lock(registryMutex());
    auto it = registry().find(codec);
    if (it == registry().end()) {
        std::string name = compressionName(codec);
        if (codec == Compression::Gzip) {
            throw Error("gzip support was not compiled in (BRAHMAPUTRA_WITH_GZIP=OFF); "
                        "register a gzip codec with registerCodec or use none");
        }
        throw Error(name + " compression is not registered; call registerCodec(Compression::" +
                    name + ", ...) or use none/gzip");
    }
    return it->second;
}

}  // namespace

Compression parseCompression(const std::string& name) {
    if (name == "none") return Compression::None;
    if (name == "lz4") return Compression::Lz4;
    if (name == "zstd") return Compression::Zstd;
    if (name == "snappy") return Compression::Snappy;
    if (name == "gzip") return Compression::Gzip;
    throw Error("unknown compression \"" + name + "\" (none, lz4, zstd, snappy, gzip)");
}

std::string compressionName(Compression codec) {
    switch (codec) {
        case Compression::None: return "none";
        case Compression::Lz4: return "lz4";
        case Compression::Zstd: return "zstd";
        case Compression::Snappy: return "snappy";
        case Compression::Gzip: return "gzip";
    }
    return "unknown(" + std::to_string(static_cast<int>(codec)) + ")";
}

void registerCodec(Compression codec, CodecFn compressFn, CodecFn decompressFn) {
    if (codec == Compression::None) throw Error("the none codec cannot be replaced");
    std::lock_guard<std::mutex> lock(registryMutex());
    registry()[codec] = CodecPair{std::move(compressFn), std::move(decompressFn)};
}

bool codecAvailable(Compression codec) {
    if (codec == Compression::None) return true;
    std::lock_guard<std::mutex> lock(registryMutex());
    return registry().count(codec) != 0;
}

Bytes compress(Compression codec, const Bytes& payload) {
    if (codec == Compression::None) return payload;
    return lookup(codec).compress(payload);
}

Bytes decompress(Compression codec, const Bytes& payload) {
    if (codec == Compression::None) return payload;
    Bytes out = lookup(codec).decompress(payload);
    if (out.size() > kMaxDecompressedBytes) throw Error("decompressed batch exceeds 256 MiB");
    return out;
}

}  // namespace brahmaputra

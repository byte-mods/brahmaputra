// C++ target, edge.buff. run.sh generates gen/edge/cpp first.
#include "edge.hpp"
#include "check.hpp"
#include <cmath>
#include <sstream>

static Edge makeEdge() {
    Edge e;
    e.i_min = INT32_MIN; e.i_max = 2147483647; e.i_zero = 0; e.i_neg = -1;
    e.l_min = INT64_MIN; e.l_max = INT64_MAX; e.l_neg = -300;
    e.f = -1.25f; e.d = 1234.5625; e.d_neg = -0.5;
    e.yes = true; e.no = false;
    e.empty = ""; e.unicode = u8"héllo wörld ✓ 日本 \U0001F680";
    e.ints = {0, -1, 1, -64, 64, INT32_MIN, 2147483647};
    e.longs = {0, -1, INT64_MAX, INT64_MIN, 4294967296LL};
    e.floats = {0.0f, 0.5f, -2.25f};
    e.doubles = {0.0, 3.5, -1000000.25};
    e.bools = {true, false, true};
    e.strings = {"", "a", u8"日本語"};
    e.no_ints = {};
    e.inner = Inner{1099511627776LL, "inner"};
    e.inners = {Inner{-1, ""}, Inner{0, "x"}};
    e.no_inners = {};
    return e;
}

template <typename T> static std::string show(const T& v) { std::ostringstream s; s << v; return s.str(); }
static std::string show(const Inner& i) { return "Inner{" + std::to_string(i.big) + "," + i.label + "}"; }
template <typename T> static std::string show(const std::vector<T>& v) {
    std::string s = "[";
    for (const auto& x : v) s += show(x) + ",";
    return s + "]";
}
static std::string show(const std::vector<bool>& v) {
    std::string s = "[";
    for (bool x : v) s += x ? "1," : "0,";
    return s + "]";
}

#define FIELD(f) check("edge decode " #f, show(e.f) == show(w.f), "got " + show(e.f) + ", want " + show(w.f))
#define EXACT(f) check("edge decode " #f, e.f == w.f, "got " + show(e.f))

int main(int argc, char** argv) {
    std::string root = argv[1];
    auto ref = readFile(root + "/edge/edge_ref.bin");
    auto enc = makeEdge().encode();
    check("edge encode == edge_ref.bin", enc == ref, "bytes differ");
    try {
        Edge e = Edge::decode(ref);
        Edge w = makeEdge();
        FIELD(i_min); FIELD(i_max); FIELD(i_zero); FIELD(i_neg);
        FIELD(l_min); FIELD(l_max); FIELD(l_neg);
        EXACT(f); EXACT(d); EXACT(d_neg);
        FIELD(yes); FIELD(no); FIELD(empty); FIELD(unicode);
        FIELD(ints); FIELD(longs); EXACT(floats); EXACT(doubles);
        FIELD(bools); FIELD(strings); FIELD(no_ints);
        FIELD(inner); FIELD(inners); FIELD(no_inners);
        check("edge re-encode == edge_ref.bin", e.encode() == ref, "bytes differ");
    } catch (const std::exception& ex) { check("edge decode edge_ref.bin", false, ex.what()); }
    auto dec = [](const std::vector<uint8_t>& b) { Edge::decode(b); };
    check("edge wrong version rejected", rejects(dec, badVersion(ref)), "decoded");
    truncations("edge every truncation rejected", ref, dec);
    // float32 variant: x10000 must be computed in single precision
    auto f32ref = readFile(root + "/edge/edge_float32_ref.bin");
    Edge fv = makeEdge();
    fv.f = 0.29f; fv.floats = {0.7f, 16777.217f, -0.29f};
    check("edge float32 encode == edge_float32_ref.bin", fv.encode() == f32ref, "bytes differ");
    try {
        Edge e = Edge::decode(f32ref);
        check("edge float32 decode", e.f == fv.f && e.floats == fv.floats, show(e.f) + " " + show(e.floats));
    } catch (const std::exception& ex) { check("edge float32 decode", false, ex.what()); }
    // trailing bytes after the root class are ignored
    try {
        auto trailing = ref;
        trailing.push_back(0x00); trailing.push_back(0xff);
        check("edge trailing bytes ignored", Edge::decode(trailing).encode() == ref, "decoded value differs");
    } catch (const std::exception& ex) { check("edge trailing bytes ignored", false, ex.what()); }
    // unencodable floats are errors, not saturated (conversion would be UB)
    Edge nan = makeEdge(); nan.d = std::nan("");
    check("edge encode NaN double rejected", rejects([&](const std::vector<uint8_t>&) { nan.encode(); }, {}), "encoded");
    Edge big = makeEdge(); big.f = 1e30f;
    check("edge encode out-of-range float rejected", rejects([&](const std::vector<uint8_t>&) { big.encode(); }, {}), "encoded");

    auto idec = [](const std::vector<uint8_t>& b) { Inner::decode(b); };
    check("edge hostile huge string length rejected", rejects(idec, unhex("0a322e312e30008080808010")), "decoded");
    check("edge hostile negative string length rejected", rejects(idec, unhex("0a322e312e300001")), "decoded");
    check("edge hostile endless varint rejected", rejects(idec, unhex("0a322e312e30ffffffffffffffffffffff")), "decoded");
    check("edge hostile invalid UTF-8 string rejected", rejects(idec, unhex("0a322e312e300002ff")), "decoded");
    return g_failed > 0 ? 1 : 0;
}

package main

// C11 target: one header and one source file per schema.
//
//   <schema>.h  structs (arrays as pointer + explicit length), the shared
//               bp_buffer / bp_str / bp_status runtime types, prototypes
//   <schema>.c  encoder/decoder; every helper is static, so two schemas can
//               be linked into one program
//
// See docs/c.md for the API.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() { registerGenerator(genC, "c") }

var cgenKeywords = map[string]bool{
	"auto": true, "break": true, "case": true, "char": true, "const": true, "continue": true,
	"default": true, "do": true, "double": true, "else": true, "enum": true, "extern": true,
	"float": true, "for": true, "goto": true, "if": true, "inline": true, "int": true, "long": true,
	"register": true, "restrict": true, "return": true, "short": true, "signed": true,
	"sizeof": true, "static": true, "struct": true, "switch": true, "typedef": true,
	"union": true, "unsigned": true, "void": true, "volatile": true, "while": true,
	"_Alignas": true, "_Alignof": true, "_Atomic": true, "_Bool": true, "_Complex": true,
	"_Generic": true, "_Imaginary": true, "_Noreturn": true, "_Static_assert": true,
	"_Thread_local": true, "bool": true, "true": true, "false": true, "NULL": true,
	// C++ keywords too: the header is meant to be includable from C++.
	"alignas": true, "alignof": true, "and": true, "and_eq": true, "asm": true, "bitand": true,
	"bitor": true, "catch": true, "char8_t": true, "char16_t": true, "char32_t": true,
	"class": true, "compl": true, "concept": true, "const_cast": true, "consteval": true,
	"constexpr": true, "constinit": true, "co_await": true, "co_return": true, "co_yield": true,
	"decltype": true, "delete": true, "dynamic_cast": true, "explicit": true, "export": true,
	"friend": true, "mutable": true, "namespace": true, "new": true, "noexcept": true, "not": true,
	"not_eq": true, "nullptr": true, "operator": true, "or": true, "or_eq": true, "private": true,
	"protected": true, "public": true, "reinterpret_cast": true, "requires": true,
	"static_assert": true, "static_cast": true, "template": true, "this": true,
	"thread_local": true, "throw": true, "try": true, "typeid": true, "typename": true,
	"using": true, "virtual": true, "wchar_t": true, "xor": true, "xor_eq": true,
}

func cgenIdent(name string) string {
	if cgenKeywords[name] || strings.HasPrefix(name, "bp_") {
		return name + "_"
	}
	return name
}

func cgenIsScalar(t string) bool {
	switch t {
	case "int", "long", "float", "double", "bool", "string":
		return true
	}
	return false
}

func cgenCType(t string) string {
	switch t {
	case "int":
		return "int32_t"
	case "long":
		return "int64_t"
	case "float":
		return "float"
	case "double":
		return "double"
	case "bool":
		return "bool"
	case "string":
		return "bp_str"
	}
	return cgenIdent(t)
}

// cgenOrder returns the classes ordered so that a class embedded by value
// (a non-array class field) is defined before the class that embeds it.
func cgenOrder(classes []Class) ([]Class, error) {
	byName := map[string]Class{}
	for _, c := range classes {
		byName[c.Name] = c
	}
	state := map[string]int{} // 1 = visiting, 2 = done
	var out []Class
	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("class %s contains itself by value (infinite size); use an array field", name)
		case 2:
			return nil
		}
		state[name] = 1
		for _, f := range byName[name].Fields {
			if !f.IsArray && !cgenIsScalar(f.Type) {
				if err := visit(f.Type); err != nil {
					return err
				}
			}
		}
		state[name] = 2
		out = append(out, byName[name])
		return nil
	}
	for _, c := range classes {
		if err := visit(c.Name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// cgenMinSize is the fewest bytes an encoded class body can occupy; used to
// bound array counts before allocating.
func cgenMinSize(classes []Class, name string, seen map[string]bool) int {
	if seen[name] {
		return 0
	}
	seen[name] = true
	defer delete(seen, name)
	n := 0
	for _, c := range classes {
		if c.Name != name {
			continue
		}
		for _, f := range c.Fields {
			if f.IsArray || cgenIsScalar(f.Type) {
				n++
			} else {
				n += cgenMinSize(classes, f.Type, seen)
			}
		}
	}
	return n
}

func cgenMacroName(s string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(s) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

const cgenRuntimeHeader = `#ifndef BITPACKER_C_RUNTIME_V1
#define BITPACKER_C_RUNTIME_V1
/* Shared by every BitPacker C header; identical in each, so guarded once. */

typedef enum bp_status {
    BP_OK = 0,
    BP_ERR_TRUNCATED = 1, /* input ended early, or a length runs past the end */
    BP_ERR_VERSION = 2,   /* version prefix does not match the schema */
    BP_ERR_OVERFLOW = 3,  /* varint longer than 10 bytes / wider than 64 bits */
    BP_ERR_LENGTH = 4,    /* negative string length or array count */
    BP_ERR_NOMEM = 5,     /* allocation failed */
    BP_ERR_DEPTH = 6,     /* nesting deeper than BP_MAX_DEPTH */
    BP_ERR_INVALID = 7    /* encode: NULL pointer with non-zero length, or array longer than INT32_MAX */
} bp_status;

#define BP_MAX_DEPTH 256

/* A byte string. Decoded strings are heap-allocated, always non-NULL and
 * NUL-terminated (data[len] == 0); len excludes the terminator. For encoding,
 * data may be NULL when len is 0. Strings may contain embedded NULs. */
typedef struct bp_str {
    char *data;
    size_t len;
} bp_str;

/* Non-owning views, for building values to encode. Do not pass a value
 * holding these to a *_free function. */
#define BP_STR_LIT(s) ((bp_str){ (char *)(s), sizeof(s) - 1 })
static inline bp_str bp_str_from(const char *s) {
    bp_str r;
    r.data = (char *)s;
    r.len = s ? strlen(s) : 0;
    return r;
}

/* Growable output buffer. Zero-initialise ({0}) before first use; encoders
 * append to it. Release with bp_buffer_free. */
typedef struct bp_buffer {
    uint8_t *data;
    size_t len;
    size_t cap;
} bp_buffer;

static inline void bp_buffer_free(bp_buffer *b) {
    if (!b) return;
    free(b->data);
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

static inline const char *bp_status_str(bp_status s) {
    switch (s) {
    case BP_OK: return "ok";
    case BP_ERR_TRUNCATED: return "truncated input";
    case BP_ERR_VERSION: return "version mismatch";
    case BP_ERR_OVERFLOW: return "varint overflow";
    case BP_ERR_LENGTH: return "invalid length";
    case BP_ERR_NOMEM: return "out of memory";
    case BP_ERR_DEPTH: return "nesting too deep";
    case BP_ERR_INVALID: return "invalid value";
    }
    return "unknown error";
}
#endif /* BITPACKER_C_RUNTIME_V1 */
`

const cgenRuntimeSource = `
/* ---- runtime (static: private to this translation unit) ---- */

#if defined(__GNUC__) || defined(__clang__)
#define BP_UNUSED __attribute__((unused))
#else
#define BP_UNUSED
#endif

#define BP_TRY(expr) do { bp_status bp_s_ = (expr); if (bp_s_ != BP_OK) return bp_s_; } while (0)

BP_UNUSED static inline bp_status bp_reserve(bp_buffer *b, size_t extra) {
    if (b->cap - b->len >= extra) return BP_OK;
    if (extra > SIZE_MAX - b->len) return BP_ERR_NOMEM;
    size_t need = b->len + extra;
    size_t cap = b->cap ? b->cap : 64;
    while (cap < need) {
        if (cap > SIZE_MAX / 2) { cap = need; break; }
        cap *= 2;
    }
    uint8_t *p = (uint8_t *)realloc(b->data, cap);
    if (!p) return BP_ERR_NOMEM;
    b->data = p;
    b->cap = cap;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_put_uvarint(bp_buffer *b, uint64_t v) {
    BP_TRY(bp_reserve(b, 10));
    while (v >= 0x80) {
        b->data[b->len++] = (uint8_t)((v & 0x7F) | 0x80);
        v >>= 7;
    }
    b->data[b->len++] = (uint8_t)v;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_put_i32(bp_buffer *b, int32_t v) {
    uint32_t u = (uint32_t)v;
    return bp_put_uvarint(b, (uint64_t)((u << 1) ^ (0u - (u >> 31))));
}

BP_UNUSED static inline bp_status bp_put_i64(bp_buffer *b, int64_t v) {
    uint64_t u = (uint64_t)v;
    return bp_put_uvarint(b, (u << 1) ^ ((uint64_t)0 - (u >> 63)));
}

/* double: trunc(v * 10000) as a long, in double precision. NaN encodes as 0; values out
 * of int64 range saturate. */
BP_UNUSED static inline bp_status bp_put_fixed(bp_buffer *b, double v) {
    double x = v * 10000.0;
    int64_t n;
    if (x != x) n = 0;
    else if (x >= 9223372036854775808.0) n = INT64_MAX;
    else if (x < -9223372036854775808.0) n = INT64_MIN;
    else n = (int64_t)x;
    return bp_put_i64(b, n);
}

/* float: single precision throughout, like the C++/Java/C#/Go targets: the
 * product v * 10000 is rounded to float (the cast discards any excess
 * precision the compiler might keep, whatever FLT_EVAL_METHOD is), then
 * truncated toward zero. NaN encodes as 0; out-of-range values saturate. */
BP_UNUSED static inline bp_status bp_put_f32(bp_buffer *b, float v) {
    float x = (float)(v * 10000.0f);
    int64_t n;
    if (x != x) n = 0;
    else if (x >= 9223372036854775808.0f) n = INT64_MAX;
    else if (x < -9223372036854775808.0f) n = INT64_MIN;
    else n = (int64_t)x;
    return bp_put_i64(b, n);
}

BP_UNUSED static inline bp_status bp_put_bool(bp_buffer *b, bool v) {
    BP_TRY(bp_reserve(b, 1));
    b->data[b->len++] = v ? 1 : 0;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_put_raw(bp_buffer *b, const void *p, size_t n) {
    if (n == 0) return BP_OK;
    BP_TRY(bp_reserve(b, n));
    memcpy(b->data + b->len, p, n);
    b->len += n;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_put_str(bp_buffer *b, bp_str s) {
    if (s.len && !s.data) return BP_ERR_INVALID;
    if (s.len > (size_t)INT64_MAX) return BP_ERR_INVALID;
    BP_TRY(bp_put_i64(b, (int64_t)s.len));
    return bp_put_raw(b, s.data, s.len);
}

BP_UNUSED static inline bp_status bp_put_count(bp_buffer *b, const void *p, size_t n) {
    if (n && !p) return BP_ERR_INVALID;
    if (n > (size_t)INT32_MAX) return BP_ERR_INVALID;
    return bp_put_i32(b, (int32_t)n);
}

typedef struct bp_reader {
    const uint8_t *p;
    size_t len;
    size_t pos;
} bp_reader;

BP_UNUSED static inline bp_status bp_get_uvarint(bp_reader *r, uint64_t *out) {
    uint64_t v = 0;
    for (unsigned i = 0; i < 10; i++) {
        if (r->pos >= r->len) return BP_ERR_TRUNCATED;
        uint8_t byte = r->p[r->pos++];
        if (i == 9 && byte > 1) return BP_ERR_OVERFLOW;
        v |= (uint64_t)(byte & 0x7F) << (7 * i);
        if (!(byte & 0x80)) {
            *out = v;
            return BP_OK;
        }
    }
    return BP_ERR_OVERFLOW;
}

BP_UNUSED static inline bp_status bp_get_i32(bp_reader *r, int32_t *out) {
    uint64_t v;
    BP_TRY(bp_get_uvarint(r, &v));
    uint32_t u = (uint32_t)v; /* like every other target: keep the low 32 bits */
    *out = (int32_t)((u >> 1) ^ (0u - (u & 1)));
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_get_i64(bp_reader *r, int64_t *out) {
    uint64_t u;
    BP_TRY(bp_get_uvarint(r, &u));
    *out = (int64_t)((u >> 1) ^ ((uint64_t)0 - (u & 1)));
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_get_f32(bp_reader *r, float *out) {
    int64_t n;
    BP_TRY(bp_get_i64(r, &n));
    *out = (float)((float)n / 10000.0f); /* float32(n) / 10000, rounded to float */
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_get_f64(bp_reader *r, double *out) {
    int64_t n;
    BP_TRY(bp_get_i64(r, &n));
    *out = (double)n / 10000.0;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_get_bool(bp_reader *r, bool *out) {
    if (r->pos >= r->len) return BP_ERR_TRUNCATED;
    *out = r->p[r->pos++] != 0;
    return BP_OK;
}

/* Reads a string length and checks the bytes are present. */
BP_UNUSED static inline bp_status bp_get_strlen(bp_reader *r, size_t *out) {
    int64_t n;
    BP_TRY(bp_get_i64(r, &n));
    if (n < 0) return BP_ERR_LENGTH;
    if ((uint64_t)n > (uint64_t)(r->len - r->pos)) return BP_ERR_TRUNCATED;
    *out = (size_t)n;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_get_str(bp_reader *r, bp_str *out) {
    size_t n;
    BP_TRY(bp_get_strlen(r, &n));
    char *d = (char *)malloc(n + 1);
    if (!d) return BP_ERR_NOMEM;
    if (n) memcpy(d, r->p + r->pos, n);
    d[n] = 0;
    r->pos += n;
    out->data = d;
    out->len = n;
    return BP_OK;
}

/* Reads an array count. Each element takes at least min_size bytes, so a
 * count the remaining input cannot hold is rejected before allocating. */
BP_UNUSED static inline bp_status bp_get_count(bp_reader *r, size_t min_size, size_t *out) {
    int32_t n;
    BP_TRY(bp_get_i32(r, &n));
    if (n < 0) return BP_ERR_LENGTH;
    size_t left = r->len - r->pos;
    if (min_size > 0) {
        if ((size_t)n > left / min_size) return BP_ERR_TRUNCATED;
    } else if ((size_t)n > left + ((size_t)1 << 20)) {
        return BP_ERR_LENGTH;
    }
    *out = (size_t)n;
    return BP_OK;
}

BP_UNUSED static inline bp_status bp_check_version(bp_reader *r, const char *version, size_t vlen) {
    size_t n;
    BP_TRY(bp_get_strlen(r, &n));
    if (n != vlen || memcmp(r->p + r->pos, version, vlen) != 0) return BP_ERR_VERSION;
    r->pos += n;
    return BP_OK;
}
`

func genC(classes []Class, cfg GeneratorConfig) error {
	ordered, err := cgenOrder(classes)
	if err != nil {
		return err
	}
	base := cfg.InputFileName
	macro := cgenMacroName(base)

	// member names per class, checked for collisions (e.g. "foo[]" + "foo_len")
	for _, c := range classes {
		seen := map[string]bool{}
		for _, f := range c.Fields {
			names := []string{cgenIdent(f.Name)}
			if f.IsArray {
				names = append(names, cgenIdent(f.Name)+"_len")
			}
			for _, n := range names {
				if seen[n] {
					return fmt.Errorf("class %s: C member name %q collides", c.Name, n)
				}
				seen[n] = true
			}
		}
	}

	var h strings.Builder
	guard := "BITPACKER_GEN_" + macro + "_H"
	fmt.Fprintf(&h, "/* Generated by BitPacker from %s.buff. Do not edit. */\n", base)
	fmt.Fprintf(&h, "#ifndef %s\n#define %s\n\n", guard, guard)
	h.WriteString("#include <stdbool.h>\n#include <stddef.h>\n#include <stdint.h>\n#include <stdlib.h>\n#include <string.h>\n\n")
	h.WriteString("#ifdef __cplusplus\nextern \"C\" {\n#endif\n\n")
	h.WriteString(cgenRuntimeHeader)
	fmt.Fprintf(&h, "\n#define %s_SCHEMA_VERSION %q\n\n", macro, cfg.Version)
	for _, c := range classes {
		n := cgenIdent(c.Name)
		fmt.Fprintf(&h, "typedef struct %s %s;\n", n, n)
	}
	h.WriteString("\n")
	for _, c := range ordered {
		n := cgenIdent(c.Name)
		fmt.Fprintf(&h, "struct %s {\n", n)
		if len(c.Fields) == 0 {
			h.WriteString("    char bp_empty_; /* ISO C forbids empty structs */\n")
		}
		for _, f := range c.Fields {
			fn := cgenIdent(f.Name)
			if f.IsArray {
				fmt.Fprintf(&h, "    %s *%s;\n    size_t %s_len;\n", cgenCType(f.Type), fn, fn)
			} else {
				fmt.Fprintf(&h, "    %s %s;\n", cgenCType(f.Type), fn)
			}
		}
		h.WriteString("};\n\n")
	}
	for _, c := range classes {
		n := cgenIdent(c.Name)
		fmt.Fprintf(&h, "/* %s */\n", n)
		fmt.Fprintf(&h, "void %s_init(%s *o);\n", n, n)
		fmt.Fprintf(&h, "void %s_free(%s *o);\n", n, n)
		fmt.Fprintf(&h, "bp_status %s_encode(const %s *o, bp_buffer *out);\n", n, n)
		fmt.Fprintf(&h, "bp_status %s_decode(%s *out, const uint8_t *data, size_t len);\n\n", n, n)
	}
	h.WriteString("#ifdef __cplusplus\n}\n#endif\n\n")
	fmt.Fprintf(&h, "#endif /* %s */\n", guard)

	var s strings.Builder
	fmt.Fprintf(&s, "/* Generated by BitPacker from %s.buff. Do not edit. */\n", base)
	fmt.Fprintf(&s, "#include \"%s.h\"\n", base)
	s.WriteString(cgenRuntimeSource)
	fmt.Fprintf(&s, "\nstatic const char bp_version_[] = %q;\n\n", cfg.Version)
	s.WriteString("/* ---- prototypes ---- */\n")
	for _, c := range classes {
		n := cgenIdent(c.Name)
		fmt.Fprintf(&s, "static bp_status %s_encode_body_(const %s *o, bp_buffer *b);\n", n, n)
		fmt.Fprintf(&s, "static bp_status %s_decode_body_(%s *o, bp_reader *r, unsigned depth);\n", n, n)
	}
	s.WriteString("\n")

	for _, c := range classes {
		n := cgenIdent(c.Name)
		// init / free
		fmt.Fprintf(&s, "void %s_init(%s *o) {\n    memset(o, 0, sizeof *o);\n}\n\n", n, n)
		fmt.Fprintf(&s, "void %s_free(%s *o) {\n    if (!o) return;\n", n, n)
		for _, f := range c.Fields {
			fn := cgenIdent(f.Name)
			switch {
			case f.IsArray && f.Type == "string":
				fmt.Fprintf(&s, "    for (size_t i = 0; i < o->%s_len; i++) free(o->%s[i].data);\n", fn, fn)
			case f.IsArray && !cgenIsScalar(f.Type):
				fmt.Fprintf(&s, "    for (size_t i = 0; i < o->%s_len; i++) %s_free(&o->%s[i]);\n", fn, cgenIdent(f.Type), fn)
			case !f.IsArray && f.Type == "string":
				fmt.Fprintf(&s, "    free(o->%s.data);\n", fn)
			case !f.IsArray && !cgenIsScalar(f.Type):
				fmt.Fprintf(&s, "    %s_free(&o->%s);\n", cgenIdent(f.Type), fn)
			}
			if f.IsArray {
				fmt.Fprintf(&s, "    free(o->%s);\n", fn)
			}
		}
		s.WriteString("    memset(o, 0, sizeof *o);\n}\n\n")

		// encode body
		fmt.Fprintf(&s, "static bp_status %s_encode_body_(const %s *o, bp_buffer *b) {\n", n, n)
		if len(c.Fields) == 0 {
			s.WriteString("    (void)o;\n    (void)b;\n")
		}
		for _, f := range c.Fields {
			fn := cgenIdent(f.Name)
			if f.IsArray {
				fmt.Fprintf(&s, "    BP_TRY(bp_put_count(b, o->%s, o->%s_len));\n", fn, fn)
				fmt.Fprintf(&s, "    for (size_t i = 0; i < o->%s_len; i++) BP_TRY(%s);\n", fn, cgenPut(f.Type, fmt.Sprintf("o->%s[i]", fn)))
			} else {
				fmt.Fprintf(&s, "    BP_TRY(%s);\n", cgenPut(f.Type, "o->"+fn))
			}
		}
		s.WriteString("    return BP_OK;\n}\n\n")

		// decode body
		fmt.Fprintf(&s, "static bp_status %s_decode_body_(%s *o, bp_reader *r, unsigned depth) {\n", n, n)
		s.WriteString("    (void)o;\n    (void)r;\n    if (depth > BP_MAX_DEPTH) return BP_ERR_DEPTH;\n")
		for _, f := range c.Fields {
			fn := cgenIdent(f.Name)
			if f.IsArray {
				minSz := 1
				if !cgenIsScalar(f.Type) {
					minSz = cgenMinSize(classes, f.Type, map[string]bool{})
				}
				s.WriteString("    {\n        size_t n;\n")
				fmt.Fprintf(&s, "        BP_TRY(bp_get_count(r, %d, &n));\n", minSz)
				s.WriteString("        if (n) {\n")
				fmt.Fprintf(&s, "            o->%s = calloc(n, sizeof *o->%s);\n", fn, fn)
				fmt.Fprintf(&s, "            if (!o->%s) return BP_ERR_NOMEM;\n", fn)
				fmt.Fprintf(&s, "            o->%s_len = n;\n", fn)
				fmt.Fprintf(&s, "            for (size_t i = 0; i < n; i++) BP_TRY(%s);\n", cgenGet(f.Type, fmt.Sprintf("o->%s[i]", fn)))
				s.WriteString("        }\n    }\n")
			} else {
				fmt.Fprintf(&s, "    BP_TRY(%s);\n", cgenGet(f.Type, "o->"+fn))
			}
		}
		s.WriteString("    return BP_OK;\n}\n\n")

		// public encode / decode
		fmt.Fprintf(&s, `bp_status %[1]s_encode(const %[1]s *o, bp_buffer *out) {
    size_t start = out->len;
    bp_status st = bp_put_str(out, bp_str_from(bp_version_));
    if (st == BP_OK) st = %[1]s_encode_body_(o, out);
    if (st != BP_OK) out->len = start;
    return st;
}

bp_status %[1]s_decode(%[1]s *out, const uint8_t *data, size_t len) {
    bp_reader r;
    r.p = data;
    r.len = data ? len : 0;
    r.pos = 0;
    memset(out, 0, sizeof *out);
    bp_status st = bp_check_version(&r, bp_version_, sizeof bp_version_ - 1);
    if (st == BP_OK) st = %[1]s_decode_body_(out, &r, 0);
    if (st != BP_OK) %[1]s_free(out);
    return st;
}

`, n)
	}

	if err := os.WriteFile(filepath.Join(cfg.OutDir, base+".h"), []byte(h.String()), 0644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(cfg.OutDir, base+".c"), []byte(s.String()), 0644)
}

func cgenPut(t, expr string) string {
	switch t {
	case "int":
		return "bp_put_i32(b, " + expr + ")"
	case "long":
		return "bp_put_i64(b, " + expr + ")"
	case "float":
		return "bp_put_f32(b, " + expr + ")"
	case "double":
		return "bp_put_fixed(b, " + expr + ")"
	case "bool":
		return "bp_put_bool(b, " + expr + ")"
	case "string":
		return "bp_put_str(b, " + expr + ")"
	}
	return cgenIdent(t) + "_encode_body_(&" + expr + ", b)"
}

func cgenGet(t, expr string) string {
	switch t {
	case "int":
		return "bp_get_i32(r, &" + expr + ")"
	case "long":
		return "bp_get_i64(r, &" + expr + ")"
	case "float":
		return "bp_get_f32(r, &" + expr + ")"
	case "double":
		return "bp_get_f64(r, &" + expr + ")"
	case "bool":
		return "bp_get_bool(r, &" + expr + ")"
	case "string":
		return "bp_get_str(r, &" + expr + ")"
	}
	return cgenIdent(t) + "_decode_body_(&" + expr + ", r, depth + 1)"
}

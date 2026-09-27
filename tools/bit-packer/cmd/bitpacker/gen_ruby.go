package main

// Ruby target: one file per schema, <schema>.rb, defining a module (the
// CamelCased schema name, or --package when given) holding one class per
// schema class. Standard library only. See docs/ruby.md.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func init() { registerGenerator(genRuby, "ruby", "rb") }

var rubyKeywords = map[string]bool{
	"BEGIN": true, "END": true, "alias": true, "and": true, "begin": true, "break": true,
	"case": true, "class": true, "def": true, "defined?": true, "do": true, "else": true,
	"elsif": true, "end": true, "ensure": true, "false": true, "for": true, "if": true,
	"in": true, "module": true, "next": true, "nil": true, "not": true, "or": true,
	"redo": true, "rescue": true, "retry": true, "return": true, "self": true, "super": true,
	"then": true, "true": true, "undef": true, "unless": true, "until": true, "when": true,
	"while": true, "yield": true, "__FILE__": true, "__LINE__": true, "__ENCODING__": true,
}

// rubyCamel turns a file or package name into a Ruby constant name.
func rubyCamel(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			up = true
			continue
		}
		if up {
			b.WriteRune(unicode.ToUpper(r))
			up = false
		} else {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" || unicode.IsDigit(rune(out[0])) {
		out = "Schema" + out
	}
	return out
}

// rubyConst makes a schema class name a valid constant (capitalised).
func rubyConst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// rubyAttr makes a field name a valid method/local name (lower-case first).
// Names that would shadow a method the generated class relies on get a
// trailing underscore.
func rubyAttr(s string) string {
	if s != "" && unicode.IsUpper(rune(s[0])) {
		s = strings.ToLower(s[:1]) + s[1:]
	}
	if rubyReservedMethods[s] {
		s += "_"
	}
	return s
}

var rubyReservedMethods = map[string]bool{
	"encode": true, "encode_to": true, "to_h": true, "hash": true, "class": true,
	"object_id": true, "send": true, "public_send": true, "freeze": true, "frozen": true,
	"dup": true, "clone": true, "inspect": true, "to_s": true, "method": true, "methods": true,
	"initialize": true, "instance_variable_set": true, "instance_variable_get": true,
	"is_a": true, "instance_of": true, "equal": true, "eql": true, "display": true,
	"then": true, "tap": true, "extend": true, "itself": true,
}

func rubyModuleName(cfg GeneratorConfig) string {
	if cfg.PackageName != "" && cfg.PackageName != "generated" {
		return rubyCamel(cfg.PackageName)
	}
	return rubyCamel(cfg.InputFileName)
}

func rubyIsScalar(t string) bool {
	switch t {
	case "int", "long", "float", "double", "bool", "string":
		return true
	}
	return false
}

func rubyDefault(t string) string {
	switch t {
	case "int", "long":
		return "0"
	case "float", "double":
		return "0.0"
	case "bool":
		return "false"
	case "string":
		return "+''"
	}
	return rubyConst(t) + ".new"
}

func rubyMinSize(classes []Class, name string, seen map[string]bool) int {
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
			if f.IsArray || rubyIsScalar(f.Type) {
				n++
			} else {
				n += rubyMinSize(classes, f.Type, seen)
			}
		}
	}
	return n
}

func rubyWrite(t, expr string) string {
	switch t {
	case "int":
		return "Wire.put_int(buf, " + expr + ")"
	case "long":
		return "Wire.put_long(buf, " + expr + ")"
	case "float":
		return "Wire.put_f32(buf, " + expr + ")"
	case "double":
		return "Wire.put_fixed(buf, " + expr + ")"
	case "bool":
		return "buf << (" + expr + " ? 1 : 0)"
	case "string":
		return "Wire.put_string(buf, " + expr + ")"
	}
	return "(" + expr + " || " + rubyConst(t) + ".new).encode_to(buf)"
}

func rubyRead(t string) string {
	switch t {
	case "int":
		return "r.int"
	case "long":
		return "r.long"
	case "float":
		return "r.f32"
	case "double":
		return "r.fixed"
	case "bool":
		return "r.bool"
	case "string":
		return "r.string"
	}
	return rubyConst(t) + ".decode_from(r, depth + 1)"
}

const rubyRuntime = `  # Raised by .decode for malformed input: truncated data, a wrong version
  # prefix, an over-long varint, a negative length or invalid UTF-8.
  class DecodeError < StandardError; end

  # Wire helpers (internal).
  module Wire
    MASK32 = 0xFFFF_FFFF
    MASK64 = 0xFFFF_FFFF_FFFF_FFFF
    MAX_DEPTH = 256
    I64_MAX = (1 << 63) - 1
    I64_MIN = -(1 << 63)

    module_function

    def put_uvarint(buf, v)
      while v >= 0x80
        buf << ((v & 0x7F) | 0x80)
        v >>= 7
      end
      buf << v
    end

    # int: wrap to 32 bits (like every fixed-width target), zigzag, varint.
    def put_int(buf, v)
      u = v.to_int & MASK32
      put_uvarint(buf, ((u << 1) & MASK32) ^ (u[31] == 1 ? MASK32 : 0))
    end

    # long: wrap to 64 bits, zigzag, varint.
    def put_long(buf, v)
      u = v.to_int & MASK64
      put_uvarint(buf, ((u << 1) & MASK64) ^ (u[63] == 1 ? MASK64 : 0))
    end

    # Rounds a Float to the nearest single-precision value.
    def f32(x)
      [x].pack('f').unpack1('f')
    end

    # float: single precision, like the fixed-width targets: round v to
    # float32, multiply by 10000 rounding the product to float32 (a float32
    # product is exact in a double, so one rounding equals a float32
    # multiply), then trunc as a long.
    def put_f32(buf, v)
      put_fixed_scaled(buf, f32(f32(v.to_f) * 10_000.0))
    end

    # double: trunc(v * 10000) as a long, in double precision.
    def put_fixed(buf, v)
      put_fixed_scaled(buf, v.to_f * 10_000)
    end

    # NaN encodes as 0; values outside the long range saturate.
    def put_fixed_scaled(buf, x)
      n = if x.nan? then 0
          elsif x >= 9.223372036854775808e18 then I64_MAX
          elsif x < -9.223372036854775808e18 then I64_MIN
          else x.truncate
          end
      put_long(buf, n)
    end

    def put_string(buf, s)
      s = s.to_s
      b = if s.encoding == Encoding::UTF_8 || s.encoding == Encoding::BINARY
            s.b
          else
            s.encode(Encoding::UTF_8).b
          end
      put_long(buf, b.bytesize)
      buf << b
    end

    def new_buffer
      String.new(capacity: 256, encoding: Encoding::BINARY)
    end
  end

  # Bounds-checked reader over a binary String (internal).
  class Reader
    def initialize(data)
      @data = data
      @pos = 0
      @len = data.bytesize
    end

    attr_reader :pos

    def remaining
      @len - @pos
    end

    def uvarint
      v = 0
      shift = 0
      10.times do |i|
        raise DecodeError, 'truncated input' if @pos >= @len
        b = @data.getbyte(@pos)
        @pos += 1
        raise DecodeError, 'varint overflows 64 bits' if i == 9 && b > 1
        v |= (b & 0x7F) << shift
        return v if b < 0x80
        shift += 7
      end
      raise DecodeError, 'varint longer than 10 bytes'
    end

    def int
      u = uvarint & Wire::MASK32
      (u >> 1) ^ -(u & 1)
    end

    def long
      u = uvarint
      (u >> 1) ^ -(u & 1)
    end

    def fixed
      long / 10_000.0
    end

    # float32(n) / 10000, rounded to float32 (one double division of two
    # float32 values rounds the same as a float32 division).
    def f32
      Wire.f32(Wire.f32(long.to_f) / 10_000.0)
    end

    def bool
      raise DecodeError, 'truncated input' if @pos >= @len
      b = @data.getbyte(@pos)
      @pos += 1
      b != 0
    end

    def raw_string
      n = long
      raise DecodeError, "negative string length #{n}" if n.negative?
      raise DecodeError, 'truncated input' if n > remaining
      s = @data.byteslice(@pos, n)
      @pos += n
      s
    end

    def string
      s = raw_string.force_encoding(Encoding::UTF_8)
      raise DecodeError, 'string is not valid UTF-8' unless s.valid_encoding?
      s
    end

    # Array count; each element needs at least min_size bytes, so a count
    # the remaining input cannot hold is rejected before looping.
    def count(min_size)
      n = int
      raise DecodeError, "negative array count #{n}" if n.negative?
      if min_size.positive?
        raise DecodeError, 'truncated input' if n > remaining / min_size
      elsif n > remaining + (1 << 20)
        raise DecodeError, "array count #{n} too large"
      end
      n
    end

    def version!(want)
      got = raw_string
      raise DecodeError, "version mismatch: got #{got.inspect}, want #{want.inspect}" unless got == want.b
    end
  end
`

func genRuby(classes []Class, cfg GeneratorConfig) error {
	mod := rubyModuleName(cfg)
	var s strings.Builder
	fmt.Fprintf(&s, "# frozen_string_literal: true\n\n# Generated by BitPacker from %s.buff. Do not edit.\n\n", cfg.InputFileName)
	fmt.Fprintf(&s, "module %s\n", mod)
	fmt.Fprintf(&s, "  SCHEMA_VERSION = %q\n\n", cfg.Version)
	s.WriteString(rubyRuntime)

	for _, c := range classes {
		cn := rubyConst(c.Name)
		s.WriteString("\n")
		fmt.Fprintf(&s, "  class %s\n", cn)
		if len(c.Fields) > 0 {
			attrs := []string{}
			for _, f := range c.Fields {
				attrs = append(attrs, ":"+rubyAttr(f.Name))
			}
			fmt.Fprintf(&s, "    attr_accessor %s\n\n", strings.Join(attrs, ", "))
			fmt.Fprintf(&s, "    FIELDS = %%i[%s].freeze\n\n", strings.Join(func() []string {
				o := []string{}
				for _, f := range c.Fields {
					o = append(o, rubyAttr(f.Name))
				}
				return o
			}(), " "))
		} else {
			s.WriteString("    FIELDS = [].freeze\n\n")
		}

		// initialize
		params := []string{}
		for _, f := range c.Fields {
			d := rubyDefault(f.Type)
			if f.IsArray {
				d = "[]"
			}
			params = append(params, fmt.Sprintf("%s: %s", rubyAttr(f.Name), d))
		}
		if len(params) > 0 {
			fmt.Fprintf(&s, "    def initialize(%s)\n", strings.Join(params, ", "))
		} else {
			s.WriteString("    def initialize\n")
		}
		for _, f := range c.Fields {
			a := rubyAttr(f.Name)
			val := a
			if rubyKeywords[a] {
				val = fmt.Sprintf("binding.local_variable_get(:%s)", a)
			}
			fmt.Fprintf(&s, "      @%s = %s\n", a, val)
		}
		s.WriteString("    end\n\n")

		// encode
		s.WriteString("    # Encodes with the schema version prefix; returns a binary String.\n")
		s.WriteString("    def encode\n      buf = Wire.new_buffer\n      Wire.put_string(buf, SCHEMA_VERSION)\n      encode_to(buf)\n      buf\n    end\n\n")
		s.WriteString("    # Appends the body (no version prefix) to a binary String.\n")
		s.WriteString("    def encode_to(buf)\n")
		for _, f := range c.Fields {
			a := "@" + rubyAttr(f.Name)
			if f.IsArray {
				fmt.Fprintf(&s, "      list = %s || []\n", a)
				s.WriteString("      Wire.put_int(buf, list.length)\n")
				fmt.Fprintf(&s, "      list.each { |v| %s }\n", rubyWrite(f.Type, "v"))
			} else {
				fmt.Fprintf(&s, "      %s\n", rubyWrite(f.Type, a))
			}
		}
		s.WriteString("      buf\n    end\n\n")

		// decode
		s.WriteString("    # Decodes a version-prefixed message. Raises DecodeError on bad input.\n")
		fmt.Fprintf(&s, "    def self.decode(data)\n      r = Reader.new(data.b)\n      r.version!(SCHEMA_VERSION)\n      decode_from(r, 0)\n    end\n\n")
		s.WriteString("    def self.decode_from(r, depth = 0)\n")
		s.WriteString("      raise DecodeError, 'nesting too deep' if depth > Wire::MAX_DEPTH\n")
		s.WriteString("      # keyword arguments are evaluated left to right: schema order\n")
		s.WriteString("      new(\n")
		for _, f := range c.Fields {
			a := rubyAttr(f.Name)
			if f.IsArray {
				minSz := 1
				if !rubyIsScalar(f.Type) {
					minSz = rubyMinSize(classes, f.Type, map[string]bool{})
				}
				fmt.Fprintf(&s, "        %s: Array.new(r.count(%d)) { %s },\n", a, minSz, rubyRead(f.Type))
			} else {
				fmt.Fprintf(&s, "        %s: %s,\n", a, rubyRead(f.Type))
			}
		}
		s.WriteString("      )\n    end\n\n")

		// helpers
		s.WriteString("    def to_h\n      FIELDS.to_h { |f| [f, public_send(f)] }\n    end\n\n")
		s.WriteString("    def ==(other)\n      other.is_a?(self.class) && to_h == other.to_h\n    end\n    alias eql? ==\n\n")
		s.WriteString("    def hash\n      to_h.hash\n    end\n")
		s.WriteString("  end\n")
	}
	s.WriteString("end\n")
	return os.WriteFile(filepath.Join(cfg.OutDir, cfg.InputFileName+".rb"), []byte(s.String()), 0644)
}

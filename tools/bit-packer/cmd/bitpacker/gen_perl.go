package main

// Perl target: one module per schema, <Schema>.pm (CamelCased schema name,
// or --package), holding package <Schema> (wire helpers, $SCHEMA_VERSION)
// and one hash-based class <Schema>::<Class> per schema class. Core Perl
// only; needs a perl built with 64-bit integers. See docs/perl.md.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func init() { registerGenerator(genPerl, "perl", "pl") }

func perlCamel(s string) string {
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

func perlPackageName(cfg GeneratorConfig) string {
	if cfg.PackageName != "" && cfg.PackageName != "generated" {
		return perlCamel(cfg.PackageName)
	}
	return perlCamel(cfg.InputFileName)
}

func perlIsScalar(t string) bool {
	switch t {
	case "int", "long", "float", "double", "bool", "string":
		return true
	}
	return false
}

// Methods every generated class defines (or inherits); no accessor is
// generated for a field with one of these names (use $obj->{name}).
var perlReservedSubs = map[string]bool{
	"new": true, "encode": true, "encode_to": true, "decode": true, "decode_from": true,
	"to_hash": true, "fields": true, "_enc": true, "_dec": true,
	"DESTROY": true, "AUTOLOAD": true, "BEGIN": true, "END": true, "INIT": true, "CHECK": true,
	"UNITCHECK": true, "import": true, "unimport": true, "can": true, "isa": true, "DOES": true,
	"VERSION": true,
}

func perlMinSize(classes []Class, name string, seen map[string]bool) int {
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
			if f.IsArray || perlIsScalar(f.Type) {
				n++
			} else {
				n += perlMinSize(classes, f.Type, seen)
			}
		}
	}
	return n
}

func perlQuote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

const perlRuntime = `use strict;
use warnings;
no warnings 'recursion'; # nesting is bounded by $MAX_DEPTH instead
use Carp ();
use Config ();

BEGIN {
    die __PACKAGE__ . " needs a perl with 64-bit integers (ivsize 8)\n"
        unless $Config::Config{ivsize} >= 8;
}

our $SCHEMA_VERSION = %s;
our $MAX_DEPTH = 256;

# ---- encoding (internal) ----
# All integer work below runs without "use integer": Perl's bit operators
# then act on unsigned 64-bit values, which is what zigzag needs.

sub _put_uvarint {
    my ($w, $u) = @_;
    while ($u >= 0x80) {
        $$w .= chr(($u & 0x7F) | 0x80);
        $u >>= 7;
    }
    $$w .= chr($u);
}

# int: wrapped to 32 bits (like the fixed-width targets), zigzag, varint.
sub _put_int {
    my ($w, $v) = @_;
    my $u = int($v // 0) & 0xFFFFFFFF;
    _put_uvarint($w, (($u << 1) & 0xFFFFFFFF) ^ (($u & 0x80000000) ? 0xFFFFFFFF : 0));
}

# long: zigzag on 64 bits. A negative IV, or a UV >= 2**63 (the same bit
# pattern), maps to (~v << 1) | 1.
sub _put_long {
    my ($w, $v) = @_;
    $v = int($v // 0);
    _put_uvarint($w, ($v < 0 || $v >= 9223372036854775808) ? ((~$v) << 1) | 1 : $v << 1);
}

# Rounds a number to the nearest single-precision value.
sub _f32 { unpack('f', pack('f', $_[0])) }

# float: single precision, like the fixed-width targets: round v to float32,
# multiply by 10000 rounding the product to float32 (a float32 product is
# exact in a double, so one rounding equals a float32 multiply), then trunc.
sub _put_f32 { _put_scaled($_[0], _f32(_f32($_[1] // 0) * 10000)) }

# double: trunc(v * 10000) as a long, in double precision.
sub _put_fixed { _put_scaled($_[0], ($_[1] // 0) * 10000) }

# NaN encodes as 0; values beyond the long range saturate.
sub _put_scaled {
    my ($w, $x) = @_;
    my $n;
    if ($x != $x)                          { $n = 0 }
    elsif ($x >= 9.2233720368547758e18)    { $n = 9223372036854775807 }
    elsif ($x < -9.2233720368547758e18)    { $n = -9223372036854775808 }
    else                                   { $n = int($x) }
    _put_long($w, $n);
}

sub _put_bool { ${$_[0]} .= $_[1] ? "\x01" : "\x00" }

# Strings are Perl character strings; they go on the wire as UTF-8.
sub _put_string {
    my ($w, $s) = @_;
    $s = '' unless defined $s;
    utf8::encode($s);
    _put_long($w, length $s);
    $$w .= $s;
}

# ---- decoding (internal) ----
# A reader is { d => bytes, p => position, n => length }.

sub _reader {
    my ($data) = @_;
    Carp::croak('decode: data is undefined') unless defined $data;
    utf8::downgrade($data, 1)
        or die __PACKAGE__ . ": decode: input contains characters above 0xFF; pass bytes\n";
    return { d => $data, p => 0, n => length $data };
}

sub _uvarint {
    my ($r) = @_;
    my ($v, $shift) = (0, 0);
    for my $i (0 .. 9) {
        die __PACKAGE__ . ": truncated input\n" if $r->{p} >= $r->{n};
        my $byte = ord(substr($r->{d}, $r->{p}++, 1));
        die __PACKAGE__ . ": varint overflows 64 bits\n" if $i == 9 && $byte > 1;
        $v |= ($byte & 0x7F) << $shift;
        return $v if $byte < 0x80;
        $shift += 7;
    }
    die __PACKAGE__ . ": varint longer than 10 bytes\n";
}

sub _get_int {
    my $u = _uvarint($_[0]) & 0xFFFFFFFF;
    return $u & 1 ? -($u >> 1) - 1 : $u >> 1;
}

sub _get_long {
    my $u = _uvarint($_[0]);
    return $u & 1 ? -($u >> 1) - 1 : $u >> 1;
}

sub _get_fixed { _get_long($_[0]) / 10000 }

# float32(n) / 10000, rounded to float32.
sub _get_f32 { _f32(_f32(_get_long($_[0])) / 10000) }

sub _get_bool {
    my ($r) = @_;
    die __PACKAGE__ . ": truncated input\n" if $r->{p} >= $r->{n};
    return ord(substr($r->{d}, $r->{p}++, 1)) ? !!1 : !!0;
}

sub _get_raw_string {
    my ($r) = @_;
    my $n = _get_long($r);
    die __PACKAGE__ . ": negative string length $n\n" if $n < 0;
    die __PACKAGE__ . ": truncated input\n" if $n > $r->{n} - $r->{p};
    my $s = substr($r->{d}, $r->{p}, $n);
    $r->{p} += $n;
    return $s;
}

sub _get_string {
    my $s = _get_raw_string($_[0]);
    utf8::decode($s) or die __PACKAGE__ . ": string is not valid UTF-8\n";
    return $s;
}

# Array count; each element needs at least $min bytes, so a count the rest
# of the input cannot hold is rejected before looping.
sub _get_count {
    my ($r, $min) = @_;
    my $n = _get_int($r);
    die __PACKAGE__ . ": negative array count $n\n" if $n < 0;
    my $left = $r->{n} - $r->{p};
    if ($min > 0) {
        die __PACKAGE__ . ": truncated input\n" if $n > $left / $min;
    } elsif ($n > $left + (1 << 20)) {
        die __PACKAGE__ . ": array count $n too large\n";
    }
    return $n;
}

sub _check_version {
    my ($r) = @_;
    my $got = _get_raw_string($r);
    die __PACKAGE__ . ": version mismatch: got '$got', want '$SCHEMA_VERSION'\n"
        unless $got eq $SCHEMA_VERSION;
}

# Shared class methods, installed into every generated class below.
sub _install {
    my ($class, @fields) = @_;
    no strict 'refs';
    my %%reserved = map { $_ => 1 } qw(%s);
    for my $f (@fields) {
        next if $reserved{$f};
        *{"${class}::$f"} = sub {
            my $self = shift;
            $self->{$f} = shift if @_;
            return $self->{$f};
        };
    }
    *{"${class}::fields"} = sub { @fields };
    *{"${class}::encode"} = sub {
        my ($self) = @_;
        my $buf = '';
        _put_string(\$buf, $SCHEMA_VERSION);
        &{"${class}::_enc"}($self, \$buf);
        return $buf;
    };
    *{"${class}::encode_to"} = sub {
        my ($self, $buf) = @_;
        &{"${class}::_enc"}($self, $buf);
        return $buf;
    };
    *{"${class}::decode"} = sub {
        my (undef, $data) = @_;
        my $r = _reader($data);
        _check_version($r);
        return &{"${class}::_dec"}($r, 0);
    };
    *{"${class}::decode_from"} = sub {
        my (undef, $r, $depth) = @_;
        return &{"${class}::_dec"}($r, $depth // 0);
    };
}
`

func perlEnc(pkg, t, expr string) string {
	switch t {
	case "int":
		return "_put_int($w, " + expr + ")"
	case "long":
		return "_put_long($w, " + expr + ")"
	case "float":
		return "_put_f32($w, " + expr + ")"
	case "double":
		return "_put_fixed($w, " + expr + ")"
	case "bool":
		return "_put_bool($w, " + expr + ")"
	case "string":
		return "_put_string($w, " + expr + ")"
	}
	cls := pkg + "::" + t
	return cls + "::_enc(" + expr + " // " + cls + "->new, $w)"
}

func perlDec(pkg, t string) string {
	switch t {
	case "int":
		return "_get_int($r)"
	case "long":
		return "_get_long($r)"
	case "float":
		return "_get_f32($r)"
	case "double":
		return "_get_fixed($r)"
	case "bool":
		return "_get_bool($r)"
	case "string":
		return "_get_string($r)"
	}
	return pkg + "::" + t + "::_dec($r, $depth + 1)"
}

func genPerl(classes []Class, cfg GeneratorConfig) error {
	pkg := perlPackageName(cfg)
	reserved := []string{}
	for k := range perlReservedSubs {
		reserved = append(reserved, k)
	}
	// deterministic output
	for i := 0; i < len(reserved); i++ {
		for j := i + 1; j < len(reserved); j++ {
			if reserved[j] < reserved[i] {
				reserved[i], reserved[j] = reserved[j], reserved[i]
			}
		}
	}

	var s strings.Builder
	fmt.Fprintf(&s, "# Generated by BitPacker from %s.buff. Do not edit.\n", cfg.InputFileName)
	fmt.Fprintf(&s, "package %s;\n", pkg)
	fmt.Fprintf(&s, perlRuntime, perlQuote(cfg.Version), strings.Join(reserved, " "))

	for _, c := range classes {
		cls := pkg + "::" + c.Name
		s.WriteString("\n")
		fmt.Fprintf(&s, "package %s {\n", cls)
		// the helpers live in the parent package; import them by name
		s.WriteString("    BEGIN {\n        no strict 'refs';\n")
		fmt.Fprintf(&s, "        *{__PACKAGE__ . \"::$_\"} = \\&{\"%s::$_\"} for qw(\n", pkg)
		s.WriteString("            _put_int _put_long _put_f32 _put_fixed _put_bool _put_string\n")
		s.WriteString("            _get_int _get_long _get_f32 _get_fixed _get_bool _get_string _get_count);\n")
		s.WriteString("    }\n\n")

		names := []string{}
		for _, f := range c.Fields {
			names = append(names, f.Name)
		}
		fmt.Fprintf(&s, "    %s::_install(__PACKAGE__, qw(%s));\n\n", pkg, strings.Join(names, " "))

		// new
		s.WriteString("    sub new {\n        my ($class, %args) = @_;\n        my $self = bless {\n")
		for _, f := range c.Fields {
			var d string
			switch {
			case f.IsArray:
				d = "[]"
			case f.Type == "int" || f.Type == "long":
				d = "0"
			case f.Type == "float" || f.Type == "double":
				d = "0.0"
			case f.Type == "bool":
				d = "!!0"
			case f.Type == "string":
				d = "''"
			default:
				d = pkg + "::" + f.Type + "->new"
			}
			fmt.Fprintf(&s, "            %s => $args{%s} // %s,\n", perlQuote(f.Name), perlQuote(f.Name), d)
		}
		s.WriteString("        }, $class;\n")
		s.WriteString("        for (keys %args) {\n            Carp::croak(\"$class: unknown field '$_'\") unless exists $self->{$_};\n        }\n")
		s.WriteString("        return $self;\n    }\n\n")

		// _enc
		s.WriteString("    sub _enc {\n        my ($o, $w) = @_;\n")
		if len(c.Fields) == 0 {
			s.WriteString("        return;\n")
		}
		for _, f := range c.Fields {
			acc := "$o->{" + perlQuote(f.Name) + "}"
			if f.IsArray {
				fmt.Fprintf(&s, "        {\n            my $l = %s // [];\n", acc)
				s.WriteString("            _put_int($w, scalar @$l);\n")
				fmt.Fprintf(&s, "            %s for @$l;\n        }\n", perlEnc(pkg, f.Type, "$_"))
			} else {
				fmt.Fprintf(&s, "        %s;\n", perlEnc(pkg, f.Type, acc))
			}
		}
		s.WriteString("    }\n\n")

		// _dec
		s.WriteString("    sub _dec {\n        my ($r, $depth) = @_;\n")
		fmt.Fprintf(&s, "        die \"%s: nesting too deep\\n\" if $depth > $%s::MAX_DEPTH;\n", pkg, pkg)
		s.WriteString("        my %o;\n")
		for _, f := range c.Fields {
			key := perlQuote(f.Name)
			if f.IsArray {
				minSz := 1
				if !perlIsScalar(f.Type) {
					minSz = perlMinSize(classes, f.Type, map[string]bool{})
				}
				fmt.Fprintf(&s, "        { my $n = _get_count($r, %d); $o{%s} = [map { %s } 1 .. $n]; }\n", minSz, key, perlDec(pkg, f.Type))
			} else {
				fmt.Fprintf(&s, "        $o{%s} = %s;\n", key, perlDec(pkg, f.Type))
			}
		}
		s.WriteString("        return bless \\%o, __PACKAGE__;\n    }\n")
		s.WriteString("}\n")
	}
	s.WriteString("\n1;\n")
	return os.WriteFile(filepath.Join(cfg.OutDir, pkg+".pm"), []byte(s.String()), 0644)
}

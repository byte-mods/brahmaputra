package main

// Helpers shared by the TypeScript, Kotlin, Scala, F# and Dart generators
// (gen_typescript.go, gen_kotlin.go, gen_scala.go, gen_fsharp.go,
// gen_dart.go). Every identifier is prefixed "polyglot" so it cannot clash
// with another generator's file in this package.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

var polyglotScalars = map[string]bool{
	"int": true, "long": true, "float": true, "double": true, "bool": true, "string": true,
}

func polyglotIsScalar(t string) bool { return polyglotScalars[t] }

// polyglotCamel turns a schema name (usually snake_case) into lowerCamelCase:
// "is_alive" -> "isAlive", "world_id" -> "worldId". Names without an
// underscore keep their spelling apart from a lower-cased first letter.
// Leading underscores are kept.
func polyglotCamel(name string) string {
	lead := len(name) - len(strings.TrimLeft(name, "_"))
	parts := strings.Split(name[lead:], "_")
	var b strings.Builder
	b.WriteString(name[:lead])
	first := true
	for _, p := range parts {
		if p == "" {
			continue
		}
		r := []rune(p)
		if first {
			r[0] = unicode.ToLower(r[0])
			first = false
		} else {
			r[0] = unicode.ToUpper(r[0])
		}
		b.WriteString(string(r))
	}
	if b.Len() == 0 {
		return name
	}
	return b.String()
}

// polyglotPascal turns a schema name into PascalCase: "is_alive" -> "IsAlive".
func polyglotPascal(name string) string {
	c := polyglotCamel(name)
	lead := len(c) - len(strings.TrimLeft(c, "_"))
	if lead == len(c) {
		return c
	}
	r := []rune(c[lead:])
	r[0] = unicode.ToUpper(r[0])
	return c[:lead] + string(r)
}

// polyglotValidate rejects schemas the idiomatic targets cannot represent:
// two fields of one class that map to the same target name, and a cycle of
// non-array class fields (such a value would be infinitely large, and a
// default constructor for it would never return).
func polyglotValidate(lang string, classes []Class, fieldName func(string) string) error {
	byName := map[string]Class{}
	for _, c := range classes {
		if _, dup := byName[c.Name]; dup {
			return fmt.Errorf("%s: class %s defined twice", lang, c.Name)
		}
		byName[c.Name] = c
	}
	for _, c := range classes {
		seen := map[string]string{}
		for _, f := range c.Fields {
			n := fieldName(f.Name)
			if prev, dup := seen[n]; dup {
				return fmt.Errorf("%s: fields %s.%s and %s.%s both map to %q", lang, c.Name, prev, c.Name, f.Name, n)
			}
			seen[n] = f.Name
		}
	}
	// Cycle detection over non-array class-typed fields.
	const white, grey, black = 0, 1, 2
	state := map[string]int{}
	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch state[name] {
		case grey:
			return fmt.Errorf("%s: class %s contains itself through non-array fields (%s); such a value is infinite — make one of the fields an array",
				lang, name, strings.Join(append(path, name), " -> "))
		case black:
			return nil
		}
		state[name] = grey
		c := byName[name]
		for _, f := range c.Fields {
			if !f.IsArray && !polyglotIsScalar(f.Type) {
				if err := visit(f.Type, append(path, name)); err != nil {
					return err
				}
			}
		}
		state[name] = black
		return nil
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := visit(n, nil); err != nil {
			return err
		}
	}
	return nil
}

// polyglotQuote renders s as a double-quoted string literal whose only
// escapes (\\ \" \n \t \r \uXXXX) are shared by TypeScript, Kotlin, Scala,
// F# and Dart. The version string is [\w.]+ in practice, but be safe.
// Dart also treats '$' specially, so it is always escaped as $.
func polyglotQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '\\':
			b.WriteString(`\\`)
		case r == '"':
			b.WriteString(`\"`)
		case r == '$' || r < 0x20 || r > 0x7e:
			if r > 0xffff {
				// Surrogate pair.
				r -= 0x10000
				fmt.Fprintf(&b, `\u%04X\u%04X`, 0xD800+(r>>10), 0xDC00+(r&0x3ff))
			} else {
				fmt.Fprintf(&b, `\u%04X`, r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func polyglotWrite(cfg GeneratorConfig, fileName, content string) error {
	if err := os.MkdirAll(cfg.OutDir, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(cfg.OutDir, fileName), []byte(content), 0644)
}

// polyglotLines is a tiny indenting code writer.
type polyglotLines struct {
	b      strings.Builder
	indent string
	unit   string
}

func (p *polyglotLines) line(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	if s == "" {
		p.b.WriteByte('\n')
		return
	}
	p.b.WriteString(p.indent)
	p.b.WriteString(s)
	p.b.WriteByte('\n')
}

func (p *polyglotLines) raw(s string) { p.b.WriteString(s) }
func (p *polyglotLines) in()          { p.indent += p.unit }
func (p *polyglotLines) out()         { p.indent = p.indent[:len(p.indent)-len(p.unit)] }
func (p *polyglotLines) String() string {
	return p.b.String()
}

// polyglotCheckClassNames rejects class names that would shadow a builtin or
// a runtime helper the generated file relies on.
func polyglotCheckClassNames(lang string, classes []Class, reserved ...string) error {
	bad := map[string]bool{}
	for _, r := range reserved {
		bad[r] = true
	}
	for _, c := range classes {
		if bad[c.Name] {
			return fmt.Errorf("%s: class name %s clashes with a name the generated code uses; rename the class", lang, c.Name)
		}
	}
	return nil
}

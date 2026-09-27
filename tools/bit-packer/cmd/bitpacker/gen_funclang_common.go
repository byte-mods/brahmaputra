package main

// Helpers shared by the Erlang, Elixir, Haskell and OCaml generators
// (gen_erlang.go, gen_elixir.go, gen_haskell.go, gen_ocaml.go). Every
// identifier is prefixed "funclang" so it cannot clash with another
// generator's file in this package.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

var funclangScalars = map[string]bool{
	"int": true, "long": true, "float": true, "double": true, "bool": true, "string": true,
}

func funclangIsScalar(t string) bool { return funclangScalars[t] }

// funclangCheck rejects field types that are neither scalars nor classes of
// this schema (the parser already does, but generators are also callable
// directly).
func funclangCheck(classes []Class) error {
	known := map[string]bool{}
	for _, c := range classes {
		known[c.Name] = true
	}
	for _, c := range classes {
		for _, f := range c.Fields {
			if !funclangIsScalar(f.Type) && !known[f.Type] {
				return fmt.Errorf("unknown type %q for field %s.%s", f.Type, c.Name, f.Name)
			}
		}
	}
	return nil
}

// funclangSnake turns "WorldState" into "world_state", "HTTPServer" into
// "http_server" and leaves "is_alive" alone.
func funclangSnake(name string) string {
	r := []rune(name)
	var b strings.Builder
	for i, c := range r {
		if unicode.IsUpper(c) {
			if i > 0 && r[i-1] != '_' {
				prevLower := unicode.IsLower(r[i-1]) || unicode.IsDigit(r[i-1])
				nextLower := i+1 < len(r) && unicode.IsLower(r[i+1])
				if prevLower || (unicode.IsUpper(r[i-1]) && nextLower) {
					b.WriteRune('_')
				}
			}
			b.WriteRune(unicode.ToLower(c))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// funclangPascal turns "world_state" / "worldState" / "WorldState" into
// "WorldState".
func funclangPascal(name string) string {
	var b strings.Builder
	for _, p := range strings.Split(name, "_") {
		if p == "" {
			continue
		}
		r := []rune(p)
		r[0] = unicode.ToUpper(r[0])
		b.WriteString(string(r))
	}
	if b.Len() == 0 {
		return "X" + name
	}
	return b.String()
}

// funclangCamel is funclangPascal with a lower-case first letter.
func funclangCamel(name string) string {
	r := []rune(funclangPascal(name))
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

// funclangHasFields reports whether every encoded instance of the type takes
// at least one byte, which lets a decoder reject an array count larger than
// the bytes left before allocating anything. Scalars always take >= 1 byte;
// a class does if any of its fields does (arrays/scalars always do).
func funclangMinOneByte(t string, byName map[string]Class, seen map[string]bool) bool {
	if funclangIsScalar(t) {
		return true
	}
	if seen[t] {
		return false
	}
	seen[t] = true
	c := byName[t]
	for _, f := range c.Fields {
		if f.IsArray || funclangMinOneByte(f.Type, byName, seen) {
			return true
		}
	}
	return false
}

func funclangByName(classes []Class) map[string]Class {
	m := map[string]Class{}
	for _, c := range classes {
		m[c.Name] = c
	}
	return m
}

// funclangGroups orders classes so that every class comes after the classes
// it references, grouping mutually recursive classes (Tarjan SCCs) together.
// Within the constraints, schema order is kept.
func funclangGroups(classes []Class) [][]Class {
	idx := map[string]int{}
	for i, c := range classes {
		idx[c.Name] = i
	}
	index := make([]int, len(classes))
	low := make([]int, len(classes))
	onStack := make([]bool, len(classes))
	for i := range index {
		index[i] = -1
	}
	var stack []int
	var groups [][]Class
	counter := 0
	var strong func(v int)
	strong = func(v int) {
		index[v], low[v] = counter, counter
		counter++
		stack = append(stack, v)
		onStack[v] = true
		for _, f := range classes[v].Fields {
			w, ok := idx[f.Type]
			if !ok {
				continue
			}
			if index[w] < 0 {
				strong(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && index[w] < low[v] {
				low[v] = index[w]
			}
		}
		if low[v] == index[v] {
			var g []int
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				g = append(g, w)
				if w == v {
					break
				}
			}
			// keep schema order inside the group
			for i := 0; i < len(g); i++ {
				for j := i + 1; j < len(g); j++ {
					if g[j] < g[i] {
						g[i], g[j] = g[j], g[i]
					}
				}
			}
			var cg []Class
			for _, k := range g {
				cg = append(cg, classes[k])
			}
			groups = append(groups, cg)
		}
	}
	for i := range classes {
		if index[i] < 0 {
			strong(i)
		}
	}
	return groups
}

func funclangWrite(dir, name, content string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), []byte(content), 0644)
}

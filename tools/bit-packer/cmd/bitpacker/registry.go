package main

// Generators for languages beyond the original eight register themselves
// here from their own gen_<lang>.go file, in an init() function:
//
//	func init() { registerGenerator(genRuby, "ruby", "rb") }
//
// A generator receives every class in the schema and the resolved config
// (cfg.OutDir already includes the language subdirectory) and writes its
// files there, typically with writeTemplate. Keeping each language in its
// own file means adding one never touches another's code.
var extraGenerators = map[string]func([]Class, GeneratorConfig) error{}

// extraCanonical maps every registered name to the first one, which names
// the output subdirectory (so `--lang rb` still writes DIR/ruby).
var extraCanonical = map[string]string{}

func registerGenerator(gen func([]Class, GeneratorConfig) error, names ...string) {
	for _, name := range names {
		if _, dup := extraGenerators[name]; dup {
			panic("bitpacker: generator registered twice: " + name)
		}
		extraGenerators[name] = gen
		extraCanonical[name] = names[0]
	}
}

// builtinAliases are the original eight targets' alternative names.
var builtinAliases = map[string]string{
	"cs": "csharp", "py": "python", "c++": "cpp", "javascript": "js",
}

// canonicalLang resolves an alias to the target's canonical name.
func canonicalLang(lang string) string {
	if c, ok := builtinAliases[lang]; ok {
		return c
	}
	if c, ok := extraCanonical[lang]; ok {
		return c
	}
	return lang
}

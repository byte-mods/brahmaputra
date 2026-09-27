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

func registerGenerator(gen func([]Class, GeneratorConfig) error, names ...string) {
	for _, name := range names {
		if _, dup := extraGenerators[name]; dup {
			panic("bitpacker: generator registered twice: " + name)
		}
		extraGenerators[name] = gen
	}
}

// Keeps cross_lang_test (test programs and generated code) out of the
// bit-parser module, so `go build ./...` in tools/bit-packer never sees it.
// The Go target test is its own module in go/.
module crosstest

go 1.21

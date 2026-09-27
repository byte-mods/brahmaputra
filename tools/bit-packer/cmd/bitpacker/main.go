package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

// --- Domain Models ---
type Field struct {
	Type    string
	Name    string
	IsArray bool
}

type Class struct {
	Name   string
	Fields []Field
}

type SchemaCtx struct {
	Version       string
	Classes       []Class
	InputFileName string
}

type GeneratorConfig struct {
	OutDir        string
	Lang          string
	UseCompress   bool
	Version       string
	MainClass     string
	SepStructs    bool
	InputFileName string
	PackageName   string
}

// --- Main Entry Point ---
func main() {
	schemaPath := flag.String("file", "", "Path to the .buff schema file")
	langs := flag.String("lang", "go,rust,python,java,csharp,js,php", "Target languages")
	outDir := flag.String("out", "./generated", "Output directory")
	compress := flag.Bool("compress", false, "Enable Zlib compression")
	sep := flag.Bool("sep", false, "Generate separate files for structs and impls (Rust only)")
	pkg := flag.String("package", "", "Package name for generated code (Go/Java/C#)")
	flag.Parse()

	// 1. Validation
	if *schemaPath == "" {
		fmt.Println("❌ Error: Please provide a file using --file")
		os.Exit(1)
	}
	if !strings.HasSuffix(*schemaPath, ".buff") {
		fmt.Printf("❌ Error: Input file must have .buff extension (got %s)\n", filepath.Ext(*schemaPath))
		os.Exit(1)
	}

	// 2. Parse Schema
	ctx, err := parseSchema(*schemaPath)
	if err != nil {
		fmt.Printf("❌ Schema Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("📦 Schema Version: %s\n", ctx.Version)
	fmt.Printf("🔹 Found Classes: %d (Main: %s)\n", len(ctx.Classes), ctx.Classes[0].Name)

	// 3. Generate Code
	targetLangs := strings.Split(*langs, ",")
	for _, lang := range targetLangs {
		lang = strings.TrimSpace(lang)
		fmt.Printf("🚀 Generating %s (Big Endian)...\n", lang)

		// Determine package name based on language defaults
		pkgName := *pkg
		if pkgName == "" {
			switch lang {
			case "go":
				pkgName = "bitpacker"
			case "java":
				pkgName = "generated"
			case "csharp", "cs":
				pkgName = "Generated"
			default:
				pkgName = "generated"
			}
		}

		cfg := GeneratorConfig{
			OutDir:        filepath.Join(*outDir, lang),
			Lang:          lang,
			UseCompress:   *compress,
			Version:       ctx.Version,
			MainClass:     ctx.Classes[0].Name,
			SepStructs:    *sep,
			InputFileName: ctx.InputFileName,
			PackageName:   pkgName,
		}

		if lang == "java" {
			cfg.MainClass += "Gen"
		}

		if err := generateCode(lang, ctx.Classes, cfg); err != nil {
			fmt.Printf("   ⚠️ Error generating %s: %v\n", lang, err)
		}
	}
	fmt.Println("✅ Generation Complete!")
}

// --- Parser ---
func parseSchema(path string) (*SchemaCtx, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	var classes []Class
	var currentClass *Class
	version := ""

	// Valid types
	validTypes := map[string]bool{
		"int": true, "long": true, "float": true, "double": true, "bool": true, "string": true,
	}
	// Track defined class names for type validation
	definedClasses := make(map[string]bool)

	versionRegex := regexp.MustCompile(`^\s*version\s*=\s*([\w\.]+)`)
	classStart := regexp.MustCompile(`^class\s+(\w+)\s*\{`)
	fieldDef := regexp.MustCompile(`^\s*(\w+)(\[\])?\s+(\w+);`)
	classEnd := regexp.MustCompile(`^\s*\}`)

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		if matches := versionRegex.FindStringSubmatch(line); matches != nil {
			version = matches[1]
		} else if matches := classStart.FindStringSubmatch(line); matches != nil {
			if currentClass != nil {
				return nil, fmt.Errorf("line %d: nested classes not supported", i+1)
			}
			currentClass = &Class{Name: matches[1]}
			definedClasses[matches[1]] = true
		} else if matches := fieldDef.FindStringSubmatch(line); matches != nil {
			if currentClass == nil {
				return nil, fmt.Errorf("line %d: field outside class", i+1)
			}
			currentClass.Fields = append(currentClass.Fields, Field{
				Type: matches[1], IsArray: matches[2] == "[]", Name: matches[3],
			})
		} else if classEnd.MatchString(line) {
			if currentClass != nil {
				classes = append(classes, *currentClass)
				currentClass = nil
			} else {
				return nil, fmt.Errorf("line %d: unexpected '}'", i+1)
			}
		} else {
			return nil, fmt.Errorf("line %d: syntax error: %s", i+1, line)
		}
	}

	if version == "" {
		return nil, fmt.Errorf("missing version definition")
	}
	if len(classes) == 0 {
		return nil, fmt.Errorf("no classes defined")
	}

	// Secondary Type Validation
	for _, cls := range classes {
		for _, f := range cls.Fields {
			if !validTypes[f.Type] && !definedClasses[f.Type] {
				return nil, fmt.Errorf("unknown type '%s' for field '%s.%s'", f.Type, cls.Name, f.Name)
			}
		}
	}

	fileName := filepath.Base(path)
	fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))

	return &SchemaCtx{Version: version, Classes: classes, InputFileName: fileName}, nil
}

// --- Code Generators ---
func generateCode(lang string, classes []Class, cfg GeneratorConfig) error {
	if err := os.MkdirAll(cfg.OutDir, 0755); err != nil {
		return err
	}

	var tmplStr, fileName string
	baseName := cfg.MainClass

	switch lang {
	case "go":
		if cfg.SepStructs {
			// 1. Generate Structs
			if err := writeTemplate(filepath.Join(cfg.OutDir, strings.ToLower(baseName)+"_structs.go"), tmplGoStructs, classes, cfg); err != nil {
				return err
			}
			// 2. Generate Impls
			return writeTemplate(filepath.Join(cfg.OutDir, strings.ToLower(baseName)+"_impl.go"), tmplGoImpl, classes, cfg)
		}
		tmplStr = tmplGo
		fileName = strings.ToLower(baseName) + ".go"
	case "rust":
		if cfg.SepStructs {
			// 1. Generate Structs
			if err := writeTemplate(filepath.Join(cfg.OutDir, strings.ToLower(baseName)+"_structs.rs"), tmplRustStructs, classes, cfg); err != nil {
				return err
			}
			// 2. Generate Impls
			return writeTemplate(filepath.Join(cfg.OutDir, strings.ToLower(baseName)+"_impl.rs"), tmplRustImpls, classes, cfg)
		}
		tmplStr = tmplRust
		fileName = strings.ToLower(baseName) + ".rs"
	case "java":
		tmplStr = tmplJava
		fileName = baseName + ".java"
	case "csharp", "cs":
		if err := writeTemplate(filepath.Join(cfg.OutDir, cfg.InputFileName+".cs"), tmplCSharp, classes, cfg); err != nil {
			return err
		}
		// No separate impl file for C# (contained in class file)
		return nil
	case "python", "py":
		tmplStr = tmplPython
		// Use InputFileName for the main python file
		fileName = cfg.InputFileName + ".py"

		// Write C extension implementation (bitpacker.c)
		if err := os.WriteFile(filepath.Join(cfg.OutDir, "bitpacker.c"), []byte(tmplBitpackerC), 0644); err != nil {
			return err
		}
		// Write setup.py
		if err := os.WriteFile(filepath.Join(cfg.OutDir, "setup.py"), []byte(tmplSetupPy), 0644); err != nil {
			return err
		}

		// Generate Pure C implementation for benchmarking
		if err := writeTemplate(filepath.Join(cfg.OutDir, cfg.InputFileName+"_model.h"), tmplPureH, classes, cfg); err != nil {
			return err
		}
		if err := writeTemplate(filepath.Join(cfg.OutDir, cfg.InputFileName+"_model.c"), tmplPureC, classes, cfg); err != nil {
			return err
		}
	case "cpp", "c++":
		if err := writeTemplate(filepath.Join(cfg.OutDir, cfg.InputFileName+".hpp"), tmplCPPHeader, classes, cfg); err != nil {
			return err
		}
		return writeTemplate(filepath.Join(cfg.OutDir, cfg.InputFileName+".cpp"), tmplCPPImpl, classes, cfg)
	case "javascript", "js":
		tmplStr = tmplJS
		fileName = strings.ToLower(baseName) + ".js"
	case "php":
		tmplStr = tmplPHP
		fileName = baseName + ".php"
	default:
		if gen, ok := extraGenerators[lang]; ok {
			return gen(classes, cfg)
		}
		return fmt.Errorf("unsupported language: %s", lang)
	}

	// For languages that don't use separate files, render the single template
	if !cfg.SepStructs || lang != "rust" {
		return render(cfg, fileName, tmplStr, classes)
	}
	return nil // Handled by separate writeTemplate calls for Rust
}

// ==========================================
// 1. GO TEMPLATE (OPTIMIZED)
// ==========================================

const tmplGoCommon = `// Generated by BitPacker
package {{.Config.PackageName}}

import (
	"errors"
)

const VERSION = "{{.Config.Version}}"

// --- ZeroCopyByteBuff ---

type ZeroCopyByteBuff struct {
	buf    []byte
	offset int
}

func NewZeroCopyByteBuff(capacity int) *ZeroCopyByteBuff {
	return &ZeroCopyByteBuff{
		buf:    make([]byte, 0, capacity),
		offset: 0,
	}
}

func NewReader(data []byte) *ZeroCopyByteBuff {
	return &ZeroCopyByteBuff{
		buf:    data,
		offset: 0,
	}
}

func (b *ZeroCopyByteBuff) Bytes() []byte {
	return b.buf
}

// ZigZag helpers
func zigzagEncode32(n int32) uint32 { return uint32((n << 1) ^ (n >> 31)) }
func zigzagDecode32(n uint32) int32 { return int32(n>>1) ^ -int32(n&1) }
func zigzagEncode64(n int64) uint64 { return uint64((n << 1) ^ (n >> 63)) }
func zigzagDecode64(n uint64) int64 { return int64(n>>1) ^ -int64(n&1) }

// Write Helpers

func (b *ZeroCopyByteBuff) PutInt32(v int32) {
	b.putVarUint64(uint64(zigzagEncode32(v)))
}

func (b *ZeroCopyByteBuff) PutInt64(v int64) {
	b.putVarUint64(zigzagEncode64(v))
}

func (b *ZeroCopyByteBuff) PutVarInt64(v int64) {
	b.putVarUint64(zigzagEncode64(v))
}

func (b *ZeroCopyByteBuff) putVarUint64(v uint64) {
	// FAST PATH: 1 byte (covers 0-127, most common for game data)
	if v < 0x80 {
		b.buf = append(b.buf, byte(v))
		return
	}
	// FAST PATH: 2 bytes (covers 128-16383)
	if v < 0x4000 {
		b.buf = append(b.buf, byte((v&0x7F)|0x80), byte(v>>7))
		return
	}
	// General path
	for v >= 0x80 {
		b.buf = append(b.buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
	b.buf = append(b.buf, byte(v))
}

func (b *ZeroCopyByteBuff) PutFloat32(v float32) {
	// Multiply by 10000.0 and truncate
	b.PutVarInt64(int64(v * 10000.0))
}

func (b *ZeroCopyByteBuff) PutFloat64(v float64) {
	b.PutVarInt64(int64(v * 10000.0))
}

func (b *ZeroCopyByteBuff) PutBool(v bool) {
	if v {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
}

func (b *ZeroCopyByteBuff) PutString(v string) {
    // Length followed by bytes
	b.PutVarInt64(int64(len(v)))
	b.buf = append(b.buf, v...)
}

// Read Helpers

func (b *ZeroCopyByteBuff) getVarUint64() (uint64, error) {
	var result uint64
	var shift uint
	for {
		if b.offset >= len(b.buf) {
			return 0, errors.New("buffer underflow")
		}
		byt := b.buf[b.offset]
		b.offset++
		result |= uint64(byt&0x7F) << shift
		if byt&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, nil
}

func (b *ZeroCopyByteBuff) GetInt32() (int32, error) {
	v, err := b.getVarUint64()
	if err != nil { return 0, err }
	return zigzagDecode32(uint32(v)), nil
}

func (b *ZeroCopyByteBuff) GetInt64() (int64, error) {
	v, err := b.getVarUint64()
	if err != nil { return 0, err }
	return zigzagDecode64(v), nil
}

func (b *ZeroCopyByteBuff) GetVarInt64() (int64, error) {
	v, err := b.getVarUint64()
	if err != nil { return 0, err }
	return zigzagDecode64(v), nil
}

func (b *ZeroCopyByteBuff) GetFloat32() (float32, error) {
	v, err := b.GetVarInt64()
	return float32(v) / 10000.0, err
}

func (b *ZeroCopyByteBuff) GetFloat64() (float64, error) {
	v, err := b.GetVarInt64()
	return float64(v) / 10000.0, err
}

func (b *ZeroCopyByteBuff) GetBool() (bool, error) {
	if b.offset >= len(b.buf) {
		return false, errors.New("buffer underflow")
	}
	v := b.buf[b.offset]
	b.offset++
	return v != 0, nil
}

func (b *ZeroCopyByteBuff) GetString() (string, error) {
	l, err := b.GetVarInt64()
	if err != nil {
		return "", err
	}
	length := int(l)
	if b.offset+length > len(b.buf) {
		return "", errors.New("buffer underflow")
	}
	s := string(b.buf[b.offset : b.offset+length])
	b.offset += length
	return s, nil
}
`

const tmplGoStructs = `// Generated by BitPacker
package {{.Config.PackageName}}

{{range .Classes}}
type {{.Name}} struct {
	{{range .Fields}}{{.Name | Title}} {{if .IsArray}}[]{{end}}{{mapTypeGo .Type}} ` + "`" + `json:"{{.Name}}" msgpack:"{{.Name}}"` + "`" + `
	{{end}}
}
{{end}}
`

// tmplGoStructBody is just the struct definitions, no package header
const tmplGoStructBody = `
{{range .Classes}}
type {{.Name}} struct {
	{{range .Fields}}{{.Name | Title}} {{if .IsArray}}[]{{end}}{{mapTypeGo .Type}} ` + "`" + `json:"{{.Name}}" msgpack:"{{.Name}}"` + "`" + `
	{{end}}
}
{{end}}
`

// tmplGoImplBody is just the encode/decode methods, no common header
const tmplGoImplBody = `
{{range .Classes}}
func (o *{{.Name}}) Encode() []byte {
	buf := NewZeroCopyByteBuff(65536)
	buf.PutString(VERSION)
	o.EncodeTo(buf)
	return buf.Bytes()
}

func (o *{{.Name}}) EncodeTo(buf *ZeroCopyByteBuff) {
	{{range .Fields}}
	{{if .IsArray}}
	buf.PutInt32(int32(len(o.{{.Name | Title}})))
	for _, item := range o.{{.Name | Title}} {
		{{encodeFieldGo "item" .Type}}
	}
	{{else}}
	{{encodeFieldGo (printf "o.%s" (.Name | Title)) .Type}}
	{{end}}
	{{end}}
}

func Decode{{.Name}}(data []byte) (*{{.Name}}, error) {
	buf := NewReader(data)
	version, err := buf.GetString()
	if err != nil { return nil, err }
	if version != VERSION {
		return nil, errors.New("version mismatch")
	}
	return Decode{{.Name}}From(buf)
}

func Decode{{.Name}}From(buf *ZeroCopyByteBuff) (*{{.Name}}, error) {
	o := &{{.Name}}{}
	var err error
	{{range .Fields}}
	{{if .IsArray}}
	{{.Name}}Len, err := buf.GetInt32()
	if err != nil { return nil, err }
	o.{{.Name | Title}} = make([]{{mapTypeGo .Type}}, {{.Name}}Len)
	for i := 0; i < int({{.Name}}Len); i++ {
		{{decodeFieldGo (printf "o.%s[i]" (.Name | Title)) "" "" .Type}}
	}
	{{else}}
	{{decodeFieldGo (printf "o.%s" (.Name | Title)) (.Name | Title) "" .Type}}
	{{end}}
	{{end}}
	return o, nil
}
{{end}}
`

// Separate impl template (for --sep mode): includes common header + impls
const tmplGoImpl = tmplGoCommon + tmplGoImplBody

// Combined template for single-file Go output
const tmplGo = tmplGoCommon + tmplGoStructBody + tmplGoImplBody

// ==========================================
// 2. RUST TEMPLATE
// ==========================================
const tmplRust = `// Generated by BitPacker
use std::io::{Error, ErrorKind, Write};
use std::convert::TryInto;
use std::str;
{{if $.Config.UseCompress}}use flate2::{write::ZlibEncoder, read::ZlibDecoder, Compression};{{end}}

pub const VERSION: &str = "{{.Config.Version}}";

// --- ZeroCopyByteBuff Implementation ---
#[derive(Debug, Clone, Copy)]
pub enum Endian {
    Big,
    Little,
}

pub struct ZeroCopyByteBuff<'a> {
    data: &'a [u8],       
    write_buf: Vec<u8>,   
    cursor: usize,
    decode_error: bool,
    multiplier: f64,
    endian: Endian,
}

impl<'a> ZeroCopyByteBuff<'a> {
    pub fn from_slice(slice: &'a [u8], endian: Endian) -> Self {
        Self {
            data: slice,
            write_buf: Vec::new(),
            cursor: 0,
            decode_error: false,
            multiplier: 10000.0,
            endian,
        }
    }

    pub fn new_writer(capacity: usize, endian: Endian) -> Self {
        Self {
            data: &[],
            write_buf: Vec::with_capacity(capacity),
            cursor: 0,
            decode_error: false,
            multiplier: 10000.0,
            endian,
        }
    }

	// zig-zag encoding: (n << 1) ^ (n >> 31)
	#[inline(always)]
	fn zigzag_encode32(n: i32) -> u32 {
		((n << 1) ^ (n >> 31)) as u32
	}

	#[inline(always)]
	fn zigzag_decode32(n: u32) -> i32 {
		((n >> 1) as i32) ^ (-((n & 1) as i32))
	}

	#[inline(always)]
	fn zigzag_encode64(n: i64) -> u64 {
		((n << 1) ^ (n >> 63)) as u64
	}

	#[inline(always)]
	fn zigzag_decode64(n: u64) -> i64 {
		((n >> 1) as i64) ^ (-((n & 1) as i64))
	}

	#[inline(always)]
	fn get_varint32(&mut self) -> u32 {
		let mut result: u32 = 0;
		for index in 0..5 {
            let Some(&byte) = self.data.get(self.cursor) else {
                self.decode_error = true;
                return 0;
            };
            self.cursor += 1;
            if index == 4 && byte & 0xf0 != 0 {
                self.decode_error = true;
                return 0;
            }
			result |= ((byte & 0x7F) as u32) << (index * 7);
			if byte & 0x80 == 0 {
				return result;
			}
		}
		self.decode_error = true;
        0
	}

	#[inline(always)]
	fn put_varint32(&mut self, mut value: u32) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(5);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

	#[inline(always)]
	fn get_varint64(&mut self) -> u64 {
		let mut result: u64 = 0;
		for index in 0..10 {
            let Some(&byte) = self.data.get(self.cursor) else {
                self.decode_error = true;
                return 0;
            };
            self.cursor += 1;
            if index == 9 && byte & 0xfe != 0 {
                self.decode_error = true;
                return 0;
            }
			result |= ((byte & 0x7F) as u64) << (index * 7);
			if byte & 0x80 == 0 {
				return result;
			}
		}
		self.decode_error = true;
        0
	}

	#[inline(always)]
	fn put_varint64(&mut self, mut value: u64) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(10);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

    #[inline(always)]
    pub fn get_i32(&mut self) -> i32 {
        let val = self.get_varint32();
		Self::zigzag_decode32(val)
    }

	#[inline(always)]
    pub fn get_bool(&mut self) -> bool {
        match self.data.get(self.cursor) {
            Some(&byte) => {
                self.cursor += 1;
                byte != 0
            }
            None => {
                self.decode_error = true;
                false
            }
        }
    }

    #[inline(always)]
    pub fn get_str(&mut self) -> Result<&'a str, &'static str> {
		let raw_len = self.get_i32();
        self.ensure_valid()?;
        let len = usize::try_from(raw_len).map_err(|_| "negative string length")?;
        if len == 0 { return Ok(""); }
        let end = self.cursor.checked_add(len).ok_or("string length overflow")?;
        let s_bytes = self.data.get(self.cursor..end).ok_or("truncated string")?;
        self.cursor = end;
        str::from_utf8(s_bytes).map_err(|_| "invalid UTF-8 string")
    }

    pub fn get_collection_len(&mut self) -> Result<usize, &'static str> {
        let raw_len = self.get_i32();
        self.ensure_valid()?;
        let len = usize::try_from(raw_len).map_err(|_| "negative collection length")?;
        if len > self.data.len().saturating_sub(self.cursor) {
            return Err("collection length exceeds remaining input");
        }
        Ok(len)
    }

    pub fn ensure_valid(&self) -> Result<(), &'static str> {
        if self.decode_error {
            Err("truncated or invalid varint")
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    pub fn get_float(&mut self) -> f64 {
        let val = self.get_i64(); 
        val as f64 / self.multiplier
    }

    #[inline(always)]
    pub fn get_i64(&mut self) -> i64 {
		let val = self.get_varint64();
		Self::zigzag_decode64(val)
    }

    #[inline(always)]
    pub fn put_i64(&mut self, value: i64) {
		self.put_varint64(Self::zigzag_encode64(value));
    }

    #[inline(always)]
    pub fn put_i32(&mut self, value: i32) {
		self.put_varint32(Self::zigzag_encode32(value));
    }
	
	#[inline(always)]
	pub fn put_bool(&mut self, value: bool) {
		self.write_buf.push(if value { 1 } else { 0 });
	}

	#[inline(always)]
    pub fn put_str(&mut self, value: &str) {
        let len = value.len();
		self.put_i32(len as i32);
        // Unsafe copy
        self.write_buf.reserve(len);
        unsafe {
            let ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, len);
            self.write_buf.set_len(self.write_buf.len() + len);
        }
    }

	#[inline(always)]
    pub fn put_float(&mut self, value: f64) {
        let i_val = (value * self.multiplier) as i64;
		self.put_varint64(Self::zigzag_encode64(i_val));
    }

    pub fn finish(self) -> Vec<u8> {
        self.write_buf
    }
}

// --- Generated Classes ---
{{range .Classes}}
#[derive(Debug, Default, Clone)]
pub struct {{.Name}} {
	{{range .Fields}}pub {{.Name}}: {{if .IsArray}}Vec<{{mapTypeRust .Type}}>{{else}}{{mapTypeRust .Type}}{{end}},
	{{end}}
}

impl {{.Name}} {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		{{if $.Config.UseCompress}}
		let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
		e.write_all(&wtr)?;
		e.finish()
		{{else}}
		Ok(wtr)
		{{end}}
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		{{range .Fields}}
		{{if .IsArray}}
		buf.put_i32(self.{{.Name}}.len() as i32);
		for item in &self.{{.Name}} {
			{{encodeFieldRust "item" .Type}}
		}
		{{else}}
		{{encodeFieldRust (printf "&self.%s" .Name) .Type}}
		{{end}}
		{{end}}
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		{{if $.Config.UseCompress}}
		let mut d = ZlibDecoder::new(data);
		let mut raw = vec![];
		d.read_to_end(&mut raw)?;
		let mut buf = ZeroCopyByteBuff::from_slice(&raw, Endian::Big);
		{{else}}
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		{{end}}

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = {{.Name}}::default();
		{{range .Fields}}
		{{if .IsArray}}
		let {{.Name}}_len = buf.get_collection_len().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		for _ in 0..{{.Name}}_len {
			{{decodeFieldRust "let val" .Type}}
			obj.{{.Name}}.push(val);
		}
		{{else}}
		{{decodeFieldRust (printf "obj.%s" .Name) .Type}}
		{{end}}
		{{end}}
		buf.ensure_valid().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		Ok(obj)
	}
}
{{end}}
`

const tmplRustStructs = `// Generated Structs

{{range .Classes}}
#[derive(Debug, Default, Clone)]
pub struct {{.Name}} {
	{{range .Fields}}pub {{.Name}}: {{if .IsArray}}Vec<{{mapTypeRust .Type}}>{{else}}{{mapTypeRust .Type}}{{end}},
	{{end}}
}
{{end}}`

const tmplRustImpls = `// Generated Implementation
use std::io::{Error, ErrorKind, Write};
use std::convert::TryInto;
use std::str;
{{if $.Config.UseCompress}}use flate2::{write::ZlibEncoder, read::ZlibDecoder, Compression};{{end}}

pub const VERSION: &str = "{{.Config.Version}}";

// --- ZeroCopyByteBuff Implementation ---
#[derive(Debug, Clone, Copy)]
pub enum Endian {
    Big,
    Little,
}

pub struct ZeroCopyByteBuff<'a> {
    data: &'a [u8],       
    write_buf: Vec<u8>,   
    cursor: usize,
    decode_error: bool,
    multiplier: f64,
    endian: Endian,
}

impl<'a> ZeroCopyByteBuff<'a> {
    pub fn from_slice(slice: &'a [u8], endian: Endian) -> Self {
        Self {
            data: slice,
            write_buf: Vec::new(),
            cursor: 0,
            decode_error: false,
            multiplier: 10000.0,
            endian,
        }
    }

    pub fn new_writer(capacity: usize, endian: Endian) -> Self {
        Self {
            data: &[],
            write_buf: Vec::with_capacity(capacity),
            cursor: 0,
            decode_error: false,
            multiplier: 10000.0,
            endian,
        }
    }

	// zig-zag encoding: (n << 1) ^ (n >> 31)
	#[inline(always)]
	fn zigzag_encode32(n: i32) -> u32 {
		((n << 1) ^ (n >> 31)) as u32
	}

	#[inline(always)]
	fn zigzag_decode32(n: u32) -> i32 {
		((n >> 1) as i32) ^ (-((n & 1) as i32))
	}

	#[inline(always)]
	fn zigzag_encode64(n: i64) -> u64 {
		((n << 1) ^ (n >> 63)) as u64
	}

	#[inline(always)]
	fn zigzag_decode64(n: u64) -> i64 {
		((n >> 1) as i64) ^ (-((n & 1) as i64))
	}

	#[inline(always)]
	fn get_varint32(&mut self) -> u32 {
		let mut result: u32 = 0;
		for index in 0..5 {
            let Some(&byte) = self.data.get(self.cursor) else {
                self.decode_error = true;
                return 0;
            };
            self.cursor += 1;
            if index == 4 && byte & 0xf0 != 0 {
                self.decode_error = true;
                return 0;
            }
			result |= ((byte & 0x7F) as u32) << (index * 7);
			if byte & 0x80 == 0 {
				return result;
			}
		}
		self.decode_error = true;
        0
	}

	#[inline(always)]
	fn put_varint32(&mut self, mut value: u32) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(5);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

	#[inline(always)]
	fn get_varint64(&mut self) -> u64 {
		let mut result: u64 = 0;
		for index in 0..10 {
            let Some(&byte) = self.data.get(self.cursor) else {
                self.decode_error = true;
                return 0;
            };
            self.cursor += 1;
            if index == 9 && byte & 0xfe != 0 {
                self.decode_error = true;
                return 0;
            }
			result |= ((byte & 0x7F) as u64) << (index * 7);
			if byte & 0x80 == 0 {
				return result;
			}
		}
		self.decode_error = true;
        0
	}

	#[inline(always)]
	fn put_varint64(&mut self, mut value: u64) {
        // FAST PATH: 1 byte
        if (value & !0x7F) == 0 {
            self.write_buf.push(value as u8);
            return;
        }
        // General path: Unsafe writes
        self.write_buf.reserve(10);
        unsafe {
            let mut ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            let mut len = 0;
            loop {
                if (value & !0x7F) == 0 {
                    ptr.write(value as u8);
                    len += 1;
                    break;
                }
                ptr.write((value as u8) | 0x80);
                ptr = ptr.add(1);
                len += 1;
                value >>= 7;
            }
            self.write_buf.set_len(self.write_buf.len() + len);
        }
	}

    #[inline(always)]
    pub fn get_i32(&mut self) -> i32 {
        let val = self.get_varint32();
		Self::zigzag_decode32(val)
    }

	#[inline(always)]
    pub fn get_bool(&mut self) -> bool {
        match self.data.get(self.cursor) {
            Some(&byte) => {
                self.cursor += 1;
                byte != 0
            }
            None => {
                self.decode_error = true;
                false
            }
        }
    }

    #[inline(always)]
    pub fn get_str(&mut self) -> Result<&'a str, &'static str> {
		let raw_len = self.get_i32();
        self.ensure_valid()?;
        let len = usize::try_from(raw_len).map_err(|_| "negative string length")?;
        if len == 0 { return Ok(""); }
        let end = self.cursor.checked_add(len).ok_or("string length overflow")?;
        let s_bytes = self.data.get(self.cursor..end).ok_or("truncated string")?;
        self.cursor = end;
        str::from_utf8(s_bytes).map_err(|_| "invalid UTF-8 string")
    }

    pub fn get_collection_len(&mut self) -> Result<usize, &'static str> {
        let raw_len = self.get_i32();
        self.ensure_valid()?;
        let len = usize::try_from(raw_len).map_err(|_| "negative collection length")?;
        if len > self.data.len().saturating_sub(self.cursor) {
            return Err("collection length exceeds remaining input");
        }
        Ok(len)
    }

    pub fn ensure_valid(&self) -> Result<(), &'static str> {
        if self.decode_error {
            Err("truncated or invalid varint")
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    pub fn get_float(&mut self) -> f64 {
        let val = self.get_i64(); 
        val as f64 / self.multiplier
    }

    #[inline(always)]
    pub fn get_i64(&mut self) -> i64 {
		let val = self.get_varint64();
		Self::zigzag_decode64(val)
    }

    #[inline(always)]
    pub fn put_i64(&mut self, value: i64) {
		self.put_varint64(Self::zigzag_encode64(value));
    }

    #[inline(always)]
    pub fn put_i32(&mut self, value: i32) {
		self.put_varint32(Self::zigzag_encode32(value));
    }
	
	#[inline(always)]
	pub fn put_bool(&mut self, value: bool) {
		self.write_buf.push(if value { 1 } else { 0 });
	}

	#[inline(always)]
    pub fn put_str(&mut self, value: &str) {
        let len = value.len();
		self.put_i32(len as i32);
        // Unsafe copy
        self.write_buf.reserve(len);
        unsafe {
            let ptr = self.write_buf.as_mut_ptr().add(self.write_buf.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, len);
            self.write_buf.set_len(self.write_buf.len() + len);
        }
    }

	#[inline(always)]
    pub fn put_float(&mut self, value: f64) {
        let i_val = (value * self.multiplier) as i64;
		self.put_varint64(Self::zigzag_encode64(i_val));
    }

    pub fn finish(self) -> Vec<u8> {
        self.write_buf
    }
}

// --- Generated Impl ---
{{range .Classes}}
impl {{.Name}} {
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		let mut buf = ZeroCopyByteBuff::new_writer(65536, Endian::Big);
        buf.put_str(VERSION);
        self.encode_to(&mut buf)?;
		let wtr = buf.finish();
		
		{{if $.Config.UseCompress}}
		let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
		e.write_all(&wtr)?;
		e.finish()
		{{else}}
		Ok(wtr)
		{{end}}
	}

    pub fn encode_to(&self, buf: &mut ZeroCopyByteBuff) -> Result<(), Error> {
		{{range .Fields}}
		{{if .IsArray}}
		buf.put_i32(self.{{.Name}}.len() as i32);
		for item in &self.{{.Name}} {
			{{encodeFieldRust "item" .Type}}
		}
		{{else}}
		{{encodeFieldRust (printf "&self.%s" .Name) .Type}}
		{{end}}
		{{end}}
        Ok(())
    }

	pub fn decode(data: &[u8]) -> Result<Self, Error> {
		{{if $.Config.UseCompress}}
		let mut d = ZlibDecoder::new(data);
		let mut raw = vec![];
		d.read_to_end(&mut raw)?;
		let mut buf = ZeroCopyByteBuff::from_slice(&raw, Endian::Big);
		{{else}}
		let mut buf = ZeroCopyByteBuff::from_slice(data, Endian::Big);
		{{end}}

        let v_str = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		if v_str != VERSION {
			return Err(Error::new(ErrorKind::InvalidData, format!("Version Mismatch: Expected {}, got {}", VERSION, v_str)));
		}

        Self::decode_from(&mut buf)
    }

    pub fn decode_from(buf: &mut ZeroCopyByteBuff) -> Result<Self, Error> {
		let mut obj = {{.Name}}::default();
		{{range .Fields}}
		{{if .IsArray}}
		let {{.Name}}_len = buf.get_collection_len().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		for _ in 0..{{.Name}}_len {
			{{decodeFieldRust "let val" .Type}}
			obj.{{.Name}}.push(val);
		}
		{{else}}
		{{decodeFieldRust (printf "obj.%s" .Name) .Type}}
		{{end}}
		{{end}}
		buf.ensure_valid().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
		Ok(obj)
	}
}
{{end}}`

// ==========================================
// 3. JAVA TEMPLATE
// ==========================================
const tmplJava = `package {{.Config.PackageName}};
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class {{.Config.MainClass}} {
    public static final String VERSION = "{{.Config.Version}}";

    // --- ZeroCopyByteBuff ---
    public static class ZeroCopyByteBuff {
        public byte[] buf;
        public int offset;
        public int capacity;

        public ZeroCopyByteBuff(int capacity) {
            this.buf = new byte[capacity];
            this.capacity = capacity;
            this.offset = 0;
        }

        public ZeroCopyByteBuff(byte[] data) {
            this.buf = data;
            this.capacity = data.length;
            this.offset = 0;
        }

        public void ensureCapacity(int needed) {
            if (offset + needed > capacity) {
                int newCap = Math.max(capacity * 2, offset + needed);
                buf = Arrays.copyOf(buf, newCap);
                capacity = newCap;
            }
        }

        public byte[] array() {
            return Arrays.copyOf(buf, offset);
        }

        // Write Helpers
        public void putInt32(int v) {
            putVarInt64(v); // ZigZag encoded
        }

        public void putInt64(long v) {
            putVarInt64(v);
        }

        public void putVarInt64(long v) {
            // ZigZag encode: (n << 1) ^ (n >> 63)
            long zz = (v << 1) ^ (v >> 63);
            // FAST PATH: 1 byte (0-127, most common for game data)
            if ((zz & ~0x7FL) == 0) {
                if (offset >= capacity) ensureCapacity(1);
                buf[offset++] = (byte) zz;
                return;
            }
            // FAST PATH: 2 bytes (128-16383)
            if ((zz & ~0x3FFFL) == 0) {
                if (offset + 2 > capacity) ensureCapacity(2);
                buf[offset++] = (byte) ((zz & 0x7F) | 0x80);
                buf[offset++] = (byte) (zz >>> 7);
                return;
            }
            // General path
            ensureCapacity(10);
            while ((zz & ~0x7FL) != 0) {
                buf[offset++] = (byte) ((zz & 0x7F) | 0x80);
                zz >>>= 7;
            }
            buf[offset++] = (byte) zz;
        }
        
        public void putFloat(float v) {
            putVarInt64((long)(v * 10000.0f));
        }
        
        public void putDouble(double v) {
            putVarInt64((long)(v * 10000.0));
        }

        public void putBool(boolean v) {
            ensureCapacity(1);
            buf[offset++] = (byte) (v ? 1 : 0);
        }

        public void putString(String v) {
            byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
            putVarInt64(bytes.length);
            ensureCapacity(bytes.length);
            System.arraycopy(bytes, 0, buf, offset, bytes.length);
            offset += bytes.length;
        }

        // Read Helpers
        public int getInt32() throws Exception {
            return (int) getVarInt64();
        }

        public long getInt64() throws Exception {
            return getVarInt64();
        }

        public long getVarInt64() throws Exception {
            long result = 0;
            int shift = 0;
            // FAST PATH: 1 byte
            byte b = buf[offset++];
            if ((b & 0x80) == 0) {
                result = b & 0x7F;
            } else {
                result = b & 0x7F;
                shift = 7;
                while (true) {
                    b = buf[offset++];
                    result |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                }
            }
            // ZigZag decode: (n >>> 1) ^ -(n & 1)
            return (result >>> 1) ^ -(result & 1);
        }
        
        public float getFloat() throws Exception {
            return (float) getVarInt64() / 10000.0f;
        }
        
        public double getDouble() throws Exception {
            return (double) getVarInt64() / 10000.0;
        }

        public boolean getBool() throws Exception {
            if (offset >= capacity) throw new Exception("Buffer underflow");
            return buf[offset++] != 0;
        }

        public String getString() throws Exception {
            int len = (int) getVarInt64();
            if (offset + len > capacity) throw new Exception("Buffer underflow");
            String s = new String(buf, offset, len, StandardCharsets.UTF_8);
            offset += len;
            return s;
        }
    }

    {{range .Classes}}
    public static class {{.Name}} {
        {{range .Fields}}public {{if .IsArray}}{{mapTypeJava .Type}}[]{{else}}{{mapTypeJava .Type}}{{end}} {{.Name}};
        {{end}}

        public byte[] encode() {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(65536);
            buf.putString(VERSION);
            encodeTo(buf);
            return buf.array();
        }

        public void encodeTo(ZeroCopyByteBuff buf) {
            {{range .Fields}}
            {{if .IsArray}}
            buf.putInt32(this.{{.Name}}.length);
            for({{mapTypeJava .Type}} item : this.{{.Name}}) {
                {{encodeFieldJava "item" .Type}}
            }
            {{else}}
            {{encodeFieldJava (printf "this.%s" .Name) .Type}}
            {{end}}
            {{end}}
        }

        public static {{.Name}} decode(byte[] data) throws Exception {
            ZeroCopyByteBuff buf = new ZeroCopyByteBuff(data);
            String version = buf.getString();
            if (!version.equals(VERSION)) {
                throw new Exception("Version Mismatch: Expected " + VERSION + ", got " + version);
            }
            return decodeFrom(buf);
        }

        public static {{.Name}} decodeFrom(ZeroCopyByteBuff buf) throws Exception {
            {{.Name}} obj = new {{.Name}}();
            {{range .Fields}}
            {{if .IsArray}}
            int {{.Name}}Len = buf.getInt32();
            obj.{{.Name}} = new {{mapTypeJava .Type}}[{{.Name}}Len];
            for(int i=0; i<{{.Name}}Len; i++) {
                {{decodeFieldJava (printf "obj.%s[i]" .Name) .Type}}
            }
            {{else}}
            {{decodeFieldJava (printf "obj.%s" .Name) .Type}}
            {{end}}
            {{end}}
            return obj;
        }
    }
    {{end}}
}
`

// ==========================================
// 4. PYTHON TEMPLATE
// ==========================================
const tmplPython = `import struct
import zlib
import sys

# Try to import C extension for best performance
_USING_C_EXT = False
try:
    from _bitpacker import ZeroCopyByteBuff
    _USING_C_EXT = True
except ImportError:
    # Pure-Python fallback — works out of the box, but C extension is ~10x faster
    # To build C extension: python3 setup.py build_ext --inplace
    class ZeroCopyByteBuff:
        def __init__(self, data=None):
            if data is not None:
                self._buf = bytearray(data)
                self._offset = 0
            else:
                self._buf = bytearray(65536)
                self._offset = 0
                self._write_pos = 0

        def _ensure(self, n):
            while self._write_pos + n > len(self._buf):
                self._buf.extend(bytearray(len(self._buf)))

        def _put_varint(self, v):
            zz = (v << 1) ^ (v >> 63)
            zz &= 0xFFFFFFFFFFFFFFFF
            if zz < 0x80:
                self._ensure(1)
                self._buf[self._write_pos] = zz
                self._write_pos += 1
                return
            if zz < 0x4000:
                self._ensure(2)
                self._buf[self._write_pos] = (zz & 0x7F) | 0x80
                self._buf[self._write_pos + 1] = zz >> 7
                self._write_pos += 2
                return
            self._ensure(10)
            while zz > 0x7F:
                self._buf[self._write_pos] = (zz & 0x7F) | 0x80
                self._write_pos += 1
                zz >>= 7
            self._buf[self._write_pos] = zz
            self._write_pos += 1

        def _get_varint(self):
            result = 0
            shift = 0
            while True:
                b = self._buf[self._offset]
                self._offset += 1
                result |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            return (result >> 1) ^ -(result & 1)

        def put_int32(self, v): self._put_varint(v)
        def put_int64(self, v): self._put_varint(v)
        def put_varint64(self, v): self._put_varint(v)
        def put_float(self, v): self._put_varint(int(v * 10000.0))
        def put_double(self, v): self._put_varint(int(v * 10000.0))
        def put_bool(self, v):
            self._ensure(1)
            self._buf[self._write_pos] = 1 if v else 0
            self._write_pos += 1
        def put_string(self, v):
            b = v.encode('utf-8')
            self._put_varint(len(b))
            self._ensure(len(b))
            self._buf[self._write_pos:self._write_pos + len(b)] = b
            self._write_pos += len(b)
        def ensure_capacity(self, n): self._ensure(n)

        def get_int32(self): return self._get_varint()
        def get_int64(self): return self._get_varint()
        def get_varint64(self): return self._get_varint()
        def get_float(self): return self._get_varint() / 10000.0
        def get_double(self): return self._get_varint() / 10000.0
        def get_bool(self):
            v = self._buf[self._offset] != 0
            self._offset += 1
            return v
        def get_string(self):
            length = self._get_varint()
            s = self._buf[self._offset:self._offset + length].decode('utf-8')
            self._offset += length
            return s
        def get_bytes(self):
            return bytes(self._buf[:self._write_pos])

VERSION = "{{.Config.Version}}"

{{range .Classes}}
class {{.Name}}:
    __slots__ = ({{range .Fields}}'{{.Name}}', {{end}})

    def __init__(self):
        {{range .Fields}}self.{{.Name}} = {{defaultValPy .Type .IsArray}}
        {{end}}

    def encode(self):
        buf = ZeroCopyByteBuff()
        buf.put_string(VERSION)
        self.encode_to(buf)
        return buf.get_bytes()

    def encode_to(self, buf):
        {{range .Fields}}
        {{if .IsArray}}
        buf.put_int32(len(self.{{.Name}}))
        for item in self.{{.Name}}:
            {{encodeFieldPy "item" .Type}}
        {{else}}
        {{encodeFieldPy (printf "self.%s" .Name) .Type}}
        {{end}}
        {{end}}
        
    @staticmethod
    def decode(data):
        buf = ZeroCopyByteBuff(data)
        version = buf.get_string()
        if version != VERSION:
            raise Exception(f"Version Mismatch: Expected {VERSION}, got {version}")
        return {{.Name}}.decode_from(buf)
    
    @staticmethod
    def decode_from(buf):
        obj = {{.Name}}.__new__({{.Name}}) # Optimization: Skip __init__
        {{range .Fields}}
        {{if .IsArray}}
        length_{{.Name}} = buf.get_int32()
        obj.{{.Name}} = [None] * length_{{.Name}}
        for i in range(length_{{.Name}}):
            {{decodeFieldPy .Type (printf "obj.%s[i]" .Name)}}
        {{else}}
        {{decodeFieldPy .Type (printf "obj.%s" .Name)}}
        {{end}}
        {{end}}
        return obj
{{end}}
`

const tmplBitpackerC = `#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

typedef struct {
    PyObject_HEAD
    char *buf;
    size_t capacity;
    size_t offset;
    size_t limit; // Not strictly needed if offset tracks write, but useful for read bounds if we passed size
} ZeroCopyByteBuff;

static void
ZeroCopyByteBuff_dealloc(ZeroCopyByteBuff *self)
{
    if (self->buf) {
        free(self->buf);
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *
ZeroCopyByteBuff_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    ZeroCopyByteBuff *self;
    self = (ZeroCopyByteBuff *) type->tp_alloc(type, 0);
    if (self != NULL) {
        self->buf = NULL;
        self->capacity = 0;
        self->offset = 0;
    }
    return (PyObject *) self;
}

static int
ZeroCopyByteBuff_init(ZeroCopyByteBuff *self, PyObject *args, PyObject *kwds)
{
    PyObject *data = NULL;
    static char *kwlist[] = {"data", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &data))
        return -1;

    if (data && data != Py_None) {
        if (PyBytes_Check(data)) {
            Py_ssize_t size = PyBytes_Size(data);
            self->buf = malloc(size);
            if (!self->buf) {
                PyErr_NoMemory();
                return -1;
            }
            memcpy(self->buf, PyBytes_AsString(data), size);
            self->capacity = size;
            self->offset = 0; // Decoding starts at 0
        } else {
             PyErr_SetString(PyExc_TypeError, "data must be bytes");
             return -1;
        }
    } else {
        // Default 64MB based on our optimization findings
        self->capacity = 64 * 1024 * 1024; 
        self->buf = malloc(self->capacity);
        if (!self->buf) {
            PyErr_NoMemory();
            return -1;
        }
        self->offset = 0;
    }

    return 0;
}

static void ensure_capacity(ZeroCopyByteBuff *self, size_t needed) {
    if (self->offset + needed > self->capacity) {
        size_t new_capacity = self->capacity * 2;
        if (new_capacity < self->offset + needed) {
            new_capacity = self->offset + needed;
        }
        char *new_buf = realloc(self->buf, new_capacity);
        if (!new_buf) {
            return; 
        }
        self->buf = new_buf;
        self->capacity = new_capacity;
    }
}

// --- Write Helpers ---

static PyObject *
ZeroCopyByteBuff_put_varint64(ZeroCopyByteBuff *self, PyObject *args)
{
    long long v;
    if (!PyArg_ParseTuple(args, "L", &v)) // L for long long (64 bit)
        return NULL;
    
    // ZigZag
    unsigned long long zz = (v << 1) ^ (v >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    // Unrolled fast path logic similar to Python but in C
    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

// Aliases
static PyObject *ZeroCopyByteBuff_put_int32(ZeroCopyByteBuff *self, PyObject *args) { return ZeroCopyByteBuff_put_varint64(self, args); }
static PyObject *ZeroCopyByteBuff_put_int64(ZeroCopyByteBuff *self, PyObject *args) { return ZeroCopyByteBuff_put_varint64(self, args); }

static PyObject *
ZeroCopyByteBuff_put_float(ZeroCopyByteBuff *self, PyObject *args)
{
    float v;
    if (!PyArg_ParseTuple(args, "f", &v)) return NULL;
    
    // Reuse varint logic: int(v * 10000.0)
    long long iv = (long long)(v * 10000.0f);
    
    unsigned long long zz = (iv << 1) ^ (iv >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_double(ZeroCopyByteBuff *self, PyObject *args)
{
    double v;
    if (!PyArg_ParseTuple(args, "d", &v)) return NULL;
    long long iv = (long long)(v * 10000.0);
    unsigned long long zz = (iv << 1) ^ (iv >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_bool(ZeroCopyByteBuff *self, PyObject *args)
{
    int v; // bool is int in C API
    if (!PyArg_ParseTuple(args, "p", &v)) return NULL;
    ensure_capacity(self, 1);
    self->buf[self->offset++] = v ? 1 : 0;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_string(ZeroCopyByteBuff *self, PyObject *args)
{
    char *s;
    Py_ssize_t len;
    if (!PyArg_ParseTuple(args, "s#", &s, &len)) return NULL;

    // len varint
    unsigned long long zz = ((unsigned long long)len) << 1; // Always positive

    ensure_capacity(self, len + 10);
    char *p = self->buf + self->offset;

    // Optimized varint using unrolled loops/switch for small values
    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }

    memcpy(p, s, len);
    self->offset = (p - self->buf) + len;
    Py_RETURN_NONE;
}


// --- Read Helpers ---



static PyObject *
ZeroCopyByteBuff_get_varint64(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    if (self->offset >= self->capacity) { // Simple bounds check, tough to do exact without decoding
         PyErr_SetString(PyExc_IndexError, "Buffer underflow");
         return NULL;
    }
    
    char *p = self->buf + self->offset;
    unsigned long long result = 0;
    int shift = 0;
    
    while (1) {
        unsigned char b = *p++;
        result |= ((unsigned long long)(b & 0x7F)) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    self->offset = p - self->buf;
    
    long long decoded = (result >> 1) ^ -(long long)(result & 1);
    return PyLong_FromLongLong(decoded);
}

// Aliases
static PyObject *ZeroCopyByteBuff_get_int32(ZeroCopyByteBuff *self, PyObject *a) { return ZeroCopyByteBuff_get_varint64(self, a); }
static PyObject *ZeroCopyByteBuff_get_int64(ZeroCopyByteBuff *self, PyObject *a) { return ZeroCopyByteBuff_get_varint64(self, a); }

static PyObject *
ZeroCopyByteBuff_get_float(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    // Reuse varint logic
    PyObject *val = ZeroCopyByteBuff_get_varint64(self, NULL);
    if (!val) return NULL;
    long long iv = PyLong_AsLongLong(val);
    Py_DECREF(val);
    return PyFloat_FromDouble((double)iv / 10000.0);
}

static PyObject *
ZeroCopyByteBuff_get_double(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    PyObject *val = ZeroCopyByteBuff_get_varint64(self, NULL);
    if (!val) return NULL;
    long long iv = PyLong_AsLongLong(val);
    Py_DECREF(val);
    return PyFloat_FromDouble((double)iv / 10000.0);
}

static PyObject *
ZeroCopyByteBuff_get_bool(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    if (self->offset >= self->capacity) {
         PyErr_SetString(PyExc_IndexError, "Buffer underflow");
         return NULL;
    }
    int val = self->buf[self->offset++] != 0;
    if (val) Py_RETURN_TRUE;
    else Py_RETURN_FALSE;
}

static PyObject *
ZeroCopyByteBuff_get_string(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    // Decode length (varint)
    char *p = self->buf + self->offset;
    unsigned long long result = 0;
    int shift = 0;
    
    while (1) {
        unsigned char b = *p++;
        result |= ((unsigned long long)(b & 0x7F)) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    // Update offset temporarily
    self->offset = p - self->buf;
    
    long long len = (result >> 1); // ZigZag decode positive
    
    if (self->offset + len > self->capacity) {
        PyErr_SetString(PyExc_IndexError, "Buffer underflow (string)");
        return NULL;
    }
    
    PyObject *s = PyUnicode_FromStringAndSize(self->buf + self->offset, len);
    self->offset += len;
    return s;
}

static PyObject *
ZeroCopyByteBuff_get_bytes(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    return PyBytes_FromStringAndSize(self->buf, self->offset);
}

static PyObject *
ZeroCopyByteBuff_ensure_capacity(ZeroCopyByteBuff *self, PyObject *args)
{
    long needed;
    if (!PyArg_ParseTuple(args, "l", &needed)) return NULL;
    ensure_capacity(self, (size_t)needed);
    Py_RETURN_NONE;
}

static PyMethodDef ZeroCopyByteBuff_methods[] = {
    {"put_int32", (PyCFunction)ZeroCopyByteBuff_put_int32, METH_VARARGS, "Put int32"},
    {"put_int64", (PyCFunction)ZeroCopyByteBuff_put_int64, METH_VARARGS, "Put int64"},
    {"put_varint64", (PyCFunction)ZeroCopyByteBuff_put_varint64, METH_VARARGS, "Put varint64"},
    {"put_float", (PyCFunction)ZeroCopyByteBuff_put_float, METH_VARARGS, "Put float"},
    {"put_double", (PyCFunction)ZeroCopyByteBuff_put_double, METH_VARARGS, "Put double"},
    {"put_bool", (PyCFunction)ZeroCopyByteBuff_put_bool, METH_VARARGS, "Put bool"},
    {"put_string", (PyCFunction)ZeroCopyByteBuff_put_string, METH_VARARGS, "Put string"},
    {"ensure_capacity", (PyCFunction)ZeroCopyByteBuff_ensure_capacity, METH_VARARGS, "Ensure capacity"},
    
    {"get_int32", (PyCFunction)ZeroCopyByteBuff_get_int32, METH_NOARGS, "Get int32"},
    {"get_int64", (PyCFunction)ZeroCopyByteBuff_get_int64, METH_NOARGS, "Get int64"},
    {"get_varint64", (PyCFunction)ZeroCopyByteBuff_get_varint64, METH_NOARGS, "Get varint64"},
    {"get_float", (PyCFunction)ZeroCopyByteBuff_get_float, METH_NOARGS, "Get float"},
    {"get_double", (PyCFunction)ZeroCopyByteBuff_get_double, METH_NOARGS, "Get double"},
    {"get_bool", (PyCFunction)ZeroCopyByteBuff_get_bool, METH_NOARGS, "Get bool"},
    {"get_string", (PyCFunction)ZeroCopyByteBuff_get_string, METH_NOARGS, "Get string"},
    
    {"get_bytes", (PyCFunction)ZeroCopyByteBuff_get_bytes, METH_NOARGS, "Get internal bytes"},
    {NULL}  /* Sentinel */
};

static PyTypeObject ZeroCopyByteBuffType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_bitpacker.ZeroCopyByteBuff",
    .tp_doc = "ZeroCopyByteBuff implemented in C",
    .tp_basicsize = sizeof(ZeroCopyByteBuff),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = ZeroCopyByteBuff_new,
    .tp_init = (initproc) ZeroCopyByteBuff_init,
    .tp_dealloc = (destructor) ZeroCopyByteBuff_dealloc,
    .tp_methods = ZeroCopyByteBuff_methods,
};

static PyModuleDef bitpackermodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_bitpacker",
    .m_doc = "Example module that creates an extension type.",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit__bitpacker(void)
{
    PyObject *m;
    if (PyType_Ready(&ZeroCopyByteBuffType) < 0)
        return NULL;

    m = PyModule_Create(&bitpackermodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&ZeroCopyByteBuffType);
    if (PyModule_AddObject(m, "ZeroCopyByteBuff", (PyObject *) &ZeroCopyByteBuffType) < 0) {
        Py_DECREF(&ZeroCopyByteBuffType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
`

const tmplSetupPy = `from setuptools import setup, Extension

module1 = Extension('_bitpacker',
                    sources = ['bitpacker.c'])

setup (name = 'BitPacker',
       version = '1.0',
       description = 'BitPacker C Extension',
       ext_modules = [module1])
`

// ==========================================
// 5. C# TEMPLATE
// ==========================================

// ==========================================
// 6. PHP TEMPLATE (FIXED)
// ==========================================
const tmplPHP = `<?php
class {{.Config.MainClass}} {
    const VERSION = "{{.Config.Version}}";
}

{{range .Classes}}
class {{.Name}} {
    {{range .Fields}}public ${{.Name}};
    {{end}}

    public function encode() {
        $d = "";
        $v = {{$.Config.MainClass}}::VERSION;
        $d .= pack("N", strlen($v)) . $v;

        {{range .Fields}}
        {{if .IsArray}}
        $d .= pack("N", count($this->{{.Name}}));
        foreach($this->{{.Name}} as $item) {
            {{encodeFieldPHP "item" .Type}}
        }
        {{else}}
        {{encodeFieldPHP (printf "$this->%s" .Name) .Type}}
        {{end}}
        {{end}}
        
        {{if $.Config.UseCompress}}return gzcompress($d);{{else}}return $d;{{end}}
    }

    public static function decode($data) {
        {{if $.Config.UseCompress}}$data = gzuncompress($data);{{end}}
        $offset = 0;
        
        $vLen = unpack("N", substr($data, $offset, 4))[1]; $offset+=4;
        $vStr = substr($data, $offset, $vLen); $offset+=$vLen;
        
        if ($vStr !== {{$.Config.MainClass}}::VERSION) {
            throw new Exception("Version Mismatch: Expected " . {{$.Config.MainClass}}::VERSION . ", got " . $vStr);
        }

        $obj = new {{.Name}}();
        {{range .Fields}}
        {{if .IsArray}}
        $count = unpack("N", substr($data, $offset, 4))[1]; $offset+=4;
        $obj->{{.Name}} = [];
        for($i=0; $i<$count; $i++) {
            {{decodeFieldPHP "val" .Type}}
            $obj->{{.Name}}[] = $val;
        }
        {{else}}
        {{decodeFieldPHP (printf "$obj->%s" .Name) .Type}}
        {{end}}
        {{end}}
        return $obj;
    }
}
{{end}}
`

// ==========================================
// 4. JS TEMPLATE
// ==========================================
// This template follows the strict optimization guidelines:
// 1. Uint8Array backend for zero-copy
// 2. TextEncoder/Decoder for strings
// 3. ZigZag + VarInts for integers
// 4. Pre-allocation

const tmplJS = `// Generated by BitPacker
const VERSION = "{{.Config.Version}}";

const Endian = {
    Big: 0,
    Little: 1,
};

class ZeroCopyByteBuff {
    constructor(dataOrSize = 65536) {
        if (typeof dataOrSize === 'number') {
            this.writeBuf = new Uint8Array(dataOrSize);
            this.cursor = 0;
            this.multiplier = 10000.0;
        } else { // Uint8Array or Buffer
            this.writeBuf = dataOrSize;
            this.cursor = 0;
            this.multiplier = 10000.0;
        }
        this.textEncoder = new TextEncoder();
        this.textDecoder = new TextDecoder();
    }

    ensureCapacity(needed) {
        if (this.cursor + needed > this.writeBuf.length) {
            const newSize = Math.max(this.writeBuf.length * 2, this.cursor + needed);
            const newBuf = new Uint8Array(newSize);
            newBuf.set(this.writeBuf);
            this.writeBuf = newBuf;
        }
    }

    // ZigZag 32
    static zigzagEncode32(n) {
        return (n << 1) ^ (n >> 31);
    }
    static zigzagDecode32(n) {
        return (n >>> 1) ^ -(n & 1);
    }
    
    // VarInt32
    putVarInt32(value) {
        this.ensureCapacity(5);
        // value is treated as 32-bit signed in bitwise ops
        while ((value & ~0x7F) !== 0) {
            this.writeBuf[this.cursor++] = (value & 0x7F) | 0x80;
            value >>>= 7;
        }
        this.writeBuf[this.cursor++] = value;
    }

    getVarInt32() {
        let result = 0;
        let shift = 0;
        while (true) {
            const byte = this.writeBuf[this.cursor++];
            result |= (byte & 0x7F) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7;
        }
        return result >>> 0; // unsigned
    }

    putInt32(val) {
        this.putVarInt32(ZeroCopyByteBuff.zigzagEncode32(val));
    }

    getInt32() {
        const val = this.getVarInt32();
        return ZeroCopyByteBuff.zigzagDecode32(val);
    }

    // 64-bit VarInt (using BigInt for full precision)
    putVarInt64(val) {
        // Only safe if val is BigInt or safe integer
        // Force BigInt
        let v = BigInt(val); 
        this.ensureCapacity(10);
        // ZigZag 64: (n << 1) ^ (n >> 63)
        let zz = (v << 1n) ^ (v >> 63n);
        
        while ((zz & ~0x7Fn) !== 0n) {
            this.writeBuf[this.cursor++] = Number((zz & 0x7Fn) | 0x80n);
            zz >>= 7n;
        }
        this.writeBuf[this.cursor++] = Number(zz);
    }

    getVarInt64() {
        let result = 0n;
        let shift = 0n;
        while (true) {
            const byte = this.writeBuf[this.cursor++];
            result |= BigInt(byte & 0x7F) << shift;
            if ((byte & 0x80) === 0) break;
            shift += 7n;
        }
        // ZigZag Decode 64: (n >>> 1) ^ -(n & 1)
        return (result >> 1n) ^ -(result & 1n);
    }

	putInt64(val) {
        this.putVarInt64(val);
    }

    getInt64() {
        return this.getVarInt64();
    }

    putFloat(val) {
       // Multiply by scalar and store as int64
       const scaled = BigInt(Math.round(val * this.multiplier));
       this.putVarInt64(scaled);
    }

    getFloat() {
       const val = this.getVarInt64();
       return Number(val) / this.multiplier;
    }

    putBoolean(val) {
        this.ensureCapacity(1);
        this.writeBuf[this.cursor++] = val ? 1 : 0;
    }
    
    getBoolean() {
        return this.writeBuf[this.cursor++] !== 0;
    }

    putString(val) {
        const bytes = this.textEncoder.encode(val);
        this.putInt32(bytes.length);
        this.ensureCapacity(bytes.length);
        this.writeBuf.set(bytes, this.cursor);
        this.cursor += bytes.length;
    }

    getString() {
        const len = this.getInt32();
        if (len === 0) return "";
        const bytes = this.writeBuf.subarray(this.cursor, this.cursor + len);
        this.cursor += len;
        // TextDecoder handles Uint8Array view correctly
        return this.textDecoder.decode(bytes);
    }

    finish() {
        return this.writeBuf.subarray(0, this.cursor);
    }
}

{{range .Classes}}
class {{.Name}} {
constructor() {
	{{range .Fields}}
	this.{{.Name}} = {{if .IsArray}}[]{{else}}{{defaultValueJS .Type}}{{end}};
	{{end}}
}

encode() {
	const buf = new ZeroCopyByteBuff(65536);
	buf.putString(VERSION);
	this.encodeTo(buf);
	return buf.finish();
}

encodeTo(buf) {
	{{range .Fields}}
	{{if .IsArray}}
	buf.putInt32(this.{{.Name}}.length);
	for (let i = 0; i < this.{{.Name}}.length; i++) {
        const item = this.{{.Name}}[i];
		{{encodeFieldJS "item" "" "" .Type}}
	}
	{{else}}
	{{encodeFieldJS "this" "" .Name .Type}}
	{{end}}
	{{end}}
}

static decode(data) {
	// 0 check for Endianness
	const buf = new ZeroCopyByteBuff(data);
	const version = buf.getString();
	if (version !== VERSION) {
		throw new Error("Version Mismatch: Expected " + VERSION + ", got " + version);
	}
	return this.decodeFrom(buf);
}

static decodeFrom(buf) {
	const obj = new {{.Name}}();
	// explicit field order
	{{range .Fields}}
	{{if .IsArray}}
	const {{.Name}}_len = buf.getInt32();
	obj.{{.Name}} = new Array({{.Name}}_len);
	for (let i = 0; i < {{.Name}}_len; i++) {
		{{decodeFieldJS "obj" .Name "i" .Type}}
	}
	{{else}}
	{{decodeFieldJS "obj" .Name "" .Type}}
	{{end}}
	{{end}}
	return obj;
}
}
{{end}}

module.exports = {
	Endian,
	ZeroCopyByteBuff,
{{range .Classes}}    {{.Name}},
{{end}}
};
`

// --- Helper Functions ---

func render(cfg GeneratorConfig, fileName, tmplStr string, classes []Class) error {
	// Helper for template execution
	return writeTemplate(filepath.Join(cfg.OutDir, fileName), tmplStr, classes, cfg)
}

func writeTemplate(path string, tmplStr string, classes []Class, cfg GeneratorConfig) error {
	tmpl, err := template.New(filepath.Base(path)).Funcs(funcMap(cfg)).Parse(tmplStr)
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := tmpl.Execute(f, map[string]interface{}{
		"Classes": classes,
		"Config":  cfg,
	}); err != nil {
		return err
	}
	return nil
}

func funcMap(cfg GeneratorConfig) template.FuncMap {
	return template.FuncMap{
		"Title":  strings.Title,
		"Lower":  strings.ToLower,
		"printf": fmt.Sprintf,
		// Type Maps
		"mapTypeGo": func(t string) string {
			if t == "int" {
				return "int32"
			}
			return t
		},
		"mapTypeRust": func(t string) string {
			if t == "int" {
				return "i32"
			}
			if t == "long" {
				return "i64"
			}
			if t == "string" {
				return "String"
			}
			return t
		},
		"mapTypeJava": func(t string) string {
			if t == "string" {
				return "String"
			}
			if t == "bool" {
				return "boolean"
			}
			return t
		},
		"mapTypeJS": mapTypeJS,
		// GO Helpers
		"encodeFieldGo": func(name, t string) string {
			switch t {
			case "int":
				return fmt.Sprintf("buf.PutInt32(%s)", name)
			case "long":
				return fmt.Sprintf("buf.PutInt64(%s)", name)
			case "float":
				return fmt.Sprintf("buf.PutFloat32(%s)", name) // Assuming float is float32 for consistency
			case "double":
				return fmt.Sprintf("buf.PutFloat64(%s)", name)
			case "bool":
				return fmt.Sprintf("buf.PutBool(%s)", name)
			case "string":
				return fmt.Sprintf("buf.PutString(%s)", name)
			default:
				// Struct
				return fmt.Sprintf("%s.EncodeTo(buf)", name)
			}
		},
		"decodeFieldGo": func(target, name, idx, t string) string {
			switch t {
			case "int":
				return fmt.Sprintf("%s, err = buf.GetInt32(); if err != nil { return nil, err }", target)
			case "long":
				return fmt.Sprintf("%s, err = buf.GetInt64(); if err != nil { return nil, err }", target)
			case "float":
				return fmt.Sprintf("%s, err = buf.GetFloat32(); if err != nil { return nil, err }", target)
			case "double":
				return fmt.Sprintf("%s, err = buf.GetFloat64(); if err != nil { return nil, err }", target)
			case "bool":
				return fmt.Sprintf("%s, err = buf.GetBool(); if err != nil { return nil, err }", target)
			case "string":
				return fmt.Sprintf("%s, err = buf.GetString(); if err != nil { return nil, err }", target)
			default:
				// Struct
				// Assume type name is capitalized t (e.g. "Vec3")
				// We need the type name to call DecodeTypeFrom
				// The mapper 'mapTypeGo' returns the type name for structs.
				// But we are inside the helper, we just have 't'.
				// If t is not primitive, it is the class name.
				return fmt.Sprintf("%sVal, err := Decode%sFrom(buf); if err != nil { return nil, err }; %s = *%sVal", name+idx, t, target, name+idx)
			}
		},
		// Rust Helpers (ZeroCopyByteBuff)
		"encodeFieldRust": func(name, t string) string {
			if t == "int" {
				return fmt.Sprintf("buf.put_i32(*%s);", name)
			}
			if t == "long" {
				return fmt.Sprintf("buf.put_i64(*%s);", name)
			}
			if t == "string" {
				// name is like "&self.field", so we pass it directly (it's &String, coerces to &str)
				return fmt.Sprintf("buf.put_str(%s);", name)
			}
			if t == "bool" {
				return fmt.Sprintf("buf.put_bool(*%s);", name)
			}
			return fmt.Sprintf("%s.encode_to(buf)?;", name)
		},
		"decodeFieldRust": func(target, t string) string {
			if t == "int" {
				return fmt.Sprintf("%s = buf.get_i32();", target)
			}
			if t == "long" {
				return fmt.Sprintf("%s = buf.get_i64();", target)
			}
			if t == "string" {
				return fmt.Sprintf("%s = buf.get_str().map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();", target)
			}
			if t == "bool" {
				return fmt.Sprintf("%s = buf.get_bool();", target)
			}
			return fmt.Sprintf("%s = %s::decode_from(buf)?;", target, t)
		},

		// Java Helpers
		"encodeFieldJava": func(name, t string) string {
			switch t {
			case "int":
				return fmt.Sprintf("buf.putInt32(%s);", name)
			case "long":
				return fmt.Sprintf("buf.putInt64(%s);", name)
			case "float":
				return fmt.Sprintf("buf.putFloat(%s);", name)
			case "double":
				return fmt.Sprintf("buf.putDouble(%s);", name)
			case "bool":
				return fmt.Sprintf("buf.putBool(%s);", name)
			case "string":
				return fmt.Sprintf("buf.putString(%s);", name)
			default:
				// Struct
				return fmt.Sprintf("%s.encodeTo(buf);", name)
			}
		},
		"decodeFieldJava": func(target, t string) string {
			switch t {
			case "int":
				return fmt.Sprintf("%s = buf.getInt32();", target)
			case "long":
				return fmt.Sprintf("%s = buf.getInt64();", target)
			case "float":
				return fmt.Sprintf("%s = buf.getFloat();", target)
			case "double":
				return fmt.Sprintf("%s = buf.getDouble();", target)
			case "bool":
				return fmt.Sprintf("%s = buf.getBool();", target)
			case "string":
				return fmt.Sprintf("%s = buf.getString();", target)
			default:
				// Struct
				// "Vec3.decodeFrom(buf)"
				return fmt.Sprintf("%s = %s.decodeFrom(buf);", target, t)
			}
		},

		// Python Helpers
		"defaultValPy": func(t string, arr bool) string {
			if arr {
				return "[]"
			}
			if t == "string" {
				return "''"
			}
			if t == "bool" {
				return "False"
			}
			return "0"
		},
		"encodeFieldPy": func(name, t string) string {
			switch t {
			case "int":
				return fmt.Sprintf("buf.put_int32(%s)", name)
			case "long":
				return fmt.Sprintf("buf.put_int64(%s)", name)
			case "float":
				return fmt.Sprintf("buf.put_float(%s)", name)
			case "double":
				return fmt.Sprintf("buf.put_double(%s)", name)
			case "bool":
				return fmt.Sprintf("buf.put_bool(%s)", name)
			case "string":
				return fmt.Sprintf("buf.put_string(%s)", name)
			default:
				// Struct
				return fmt.Sprintf("%s.encode_to(buf)", name)
			}
		},
		"decodeFieldPy": func(t, target string) string {
			switch t {
			case "int":
				return fmt.Sprintf("%s = buf.get_int32()", target)
			case "long":
				return fmt.Sprintf("%s = buf.get_int64()", target)
			case "float":
				return fmt.Sprintf("%s = buf.get_float()", target)
			case "double":
				return fmt.Sprintf("%s = buf.get_double()", target)
			case "bool":
				return fmt.Sprintf("%s = buf.get_bool()", target)
			case "string":
				return fmt.Sprintf("%s = buf.get_string()", target)
			default:
				// Struct
				// "Vec3.decode_from(buf)"
				return fmt.Sprintf("%s = %s.decode_from(buf)", target, t)
			}
		},

		// JS Helpers
		"defaultValueJS": defaultValueJS,
		"encodeFieldJS":  encodeFieldJS,
		"decodeFieldJS":  decodeFieldJS,
		// PHP Helpers (FIXED)
		"encodeFieldPHP": func(name, t string) string {
			if t == "string" {
				return fmt.Sprintf("$d .= pack('N', strlen(%s)) . %s;", name, name)
			}
			return fmt.Sprintf("$d .= pack('N', %s);", name)
		},
		"decodeFieldPHP": func(target, t string) string {
			if t == "string" {
				return fmt.Sprintf("$len = unpack('N', substr($data, $offset, 4))[1]; $offset+=4; %s = substr($data, $offset, $len); $offset+=$len;", target)
			}
			return fmt.Sprintf("%s = unpack('N', substr($data, $offset, 4))[1]; $offset+=4;", target)
		},
		// --- C Helpers ---
		"mapTypeC": func(t string) string {
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
				return "char*"
			default:
				return t // Struct
			}
		},
		"isClass": func(t string) bool {
			switch t {
			case "int", "long", "float", "double", "bool", "string":
				return false
			default:
				return true
			}
		},
		"encodeFieldC": func(objVar, fieldName, idx, t string) string {
			target := objVar + "->" + fieldName
			if idx != "" {
				target += "[" + idx + "]"
			}

			switch t {
			case "int":
				return fmt.Sprintf("ZeroCopyByteBuff_put_int32(buf, %s);", target)
			case "long":
				return fmt.Sprintf("ZeroCopyByteBuff_put_int64(buf, %s);", target)
			case "float":
				return fmt.Sprintf("ZeroCopyByteBuff_put_float(buf, %s);", target)
			case "double":
				return fmt.Sprintf("ZeroCopyByteBuff_put_double(buf, %s);", target)
			case "bool":
				return fmt.Sprintf("ZeroCopyByteBuff_put_bool(buf, %s);", target)
			case "string":
				return fmt.Sprintf("ZeroCopyByteBuff_put_string(buf, %s);", target)
			default:
				return fmt.Sprintf("%s_encode(&%s, buf);", t, target)
			}
		},
		"decodeFieldC": func(objVar, fieldName, idx, t string) string {
			target := objVar + "->" + fieldName
			if idx != "" {
				target += "[" + idx + "]"
			}

			switch t {
			case "int":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_int32(buf);", target)
			case "long":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_int64(buf);", target)
			case "float":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_float(buf);", target)
			case "double":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_double(buf);", target)
			case "bool":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_bool(buf);", target)
			case "string":
				return fmt.Sprintf("%s = ZeroCopyByteBuff_get_string(buf);", target)
			default:
				return fmt.Sprintf("%s* val = %s_decode(buf); %s = *val; free(val);", t, t, target)
			}
		},
		"mapTypeCPP":     mapTypeCPP,
		"encodeFieldCPP": encodeFieldCPP,
		"decodeFieldCPP": decodeFieldCPP,
		"mapTypeCS":      mapTypeCS,
		"encodeFieldCS":  encodeFieldCS,
		"decodeFieldCS":  decodeFieldCS,
	}
}

func defaultValueJS(t string) string {
	switch t {
	case "int", "float", "long", "short", "byte":
		return "0"
	case "bool":
		return "false"
	case "string":
		return "\"\""
	default:
		return "new " + t + "()"
	}
}

// --- JS Helpers ---
func mapTypeJS(t string) string {
	switch t {
	case "int", "short", "long", "byte":
		return "number" // JS treats numbers as double (safe integer range is 53-bits)
	case "float", "double":
		return "number"
	case "bool":
		return "boolean"
	case "string":
		return "string"
	default:
		return t // Class name
	}
}

func encodeFieldJS(access, arrayIdx, fieldName, fieldType string) string {
	prefix := access
	if fieldName != "" {
		prefix = access + "." + fieldName
	}
	if arrayIdx != "" {
		// If specific array index
		if access == "item" {
			// Loop var, ignore fieldName
			prefix = "item"
		} else {
			prefix = access + "." + fieldName + "[" + arrayIdx + "]"
		}
	} else if access == "item" {
		prefix = "item"
	}

	switch fieldType {
	case "int":
		return fmt.Sprintf("buf.putInt32(%s);", prefix)
	case "long":
		return fmt.Sprintf("buf.putInt64(%s);", prefix)
	case "float":
		return fmt.Sprintf("buf.putFloat(%s);", prefix)
	case "bool":
		return fmt.Sprintf("buf.putBoolean(%s);", prefix)
	case "string":
		return fmt.Sprintf("buf.putString(%s);", prefix)
	default: // Class
		return fmt.Sprintf("%s.encodeTo(buf);", prefix) // Recursive
	}
}

func decodeFieldJS(objVar, fieldName, arrayIdx, fieldType string) string {
	target := objVar + "." + fieldName
	if arrayIdx != "" {
		target = objVar + "." + fieldName + "[" + arrayIdx + "]"
	}

	switch fieldType {
	case "int":
		return fmt.Sprintf("%s = buf.getInt32();", target)
	case "long":
		return fmt.Sprintf("%s = buf.getInt64();", target)
	case "float":
		return fmt.Sprintf("%s = buf.getFloat();", target)
	case "bool":
		return fmt.Sprintf("%s = buf.getBoolean();", target)
	case "string":
		return fmt.Sprintf("%s = buf.getString();", target)
	default: // Class
		return fmt.Sprintf("%s = %s.decodeFrom(buf);", target, fieldType)
	}
}

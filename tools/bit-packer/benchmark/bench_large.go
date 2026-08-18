package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	// Import generated code.
	// We assume it's in a package compatible with this import path.
	// Since generated/go_bench is not a Go module, we might need to adjust imports or use relative import for this script.
	// However, simplest is to put this file in root or adjust go.mod.
	// Let's create a temporary module in benchmark/go_large or similar.
	// Or just put this file in root for simplicity and then delete.
	// Actually, let's put it in benchmark/go_large/main.go and use replace directive in go.mod there.
	// But for simplicity, let's try to run it from root.

	bp "bit-parser/benchmark/generated/go_bench/go"
)

func main() {
	fileName := "large_payload.bin"
	data, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read %d bytes from %s\n", len(data), fileName)

	// Decode
	runtime.GC()
	start := time.Now()
	world, err := bp.DecodeWorldState(data)
	if err != nil {
		panic(err)
	}
	elapsedDecode := time.Since(start)
	fmt.Printf("Decode time: %.4fs\n", elapsedDecode.Seconds())

	// Verify somewhat
	fmt.Printf("Decoded %d guilds\n", len(world.Guilds))

	// Encode
	runtime.GC()
	start = time.Now()
	encodedData := world.Encode()
	elapsedEncode := time.Since(start)
	fmt.Printf("Encode time: %.4fs\n", elapsedEncode.Seconds())
	fmt.Printf("Encoded size: %d bytes\n", len(encodedData))

	if len(encodedData) != len(data) {
		fmt.Printf("WARNING: Encoded size mismatch! Original: %d, New: %d\n", len(data), len(encodedData))
	}
}

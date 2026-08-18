using System;
using System.IO;
using System.Diagnostics;
using Generated;

namespace BenchLarge {
    class Program {
        static void Main(string[] args) {
            string fileName = "../../large_payload.bin";
            if (!File.Exists(fileName)) {
                // Try alternate path if running from differently
                if (File.Exists("large_payload.bin")) fileName = "large_payload.bin";
                else {
                    Console.WriteLine($"File not found: {fileName}");
                    return;
                }
            }

            byte[] data = File.ReadAllBytes(fileName);
            Console.WriteLine($"Read {data.Length} bytes from {fileName}");

            // Decode
            var sw = Stopwatch.StartNew();
            var world = WorldState.Decode(data);
            sw.Stop();
            Console.WriteLine($"Decode time: {sw.Elapsed.TotalSeconds:F4}s");
            Console.WriteLine($"Decoded {world.guilds.Length} guilds");

            // Encode
            sw.Restart();
            byte[] encodedData = world.Encode();
            sw.Stop();
            Console.WriteLine($"Encode time: {sw.Elapsed.TotalSeconds:F4}s");
            Console.WriteLine($"Encoded size: {encodedData.Length} bytes");

            if (encodedData.Length != data.Length) {
                Console.WriteLine($"WARNING: Encoded size mismatch! Original: {data.Length}, New: {encodedData.Length}");
            }
        }
    }
}

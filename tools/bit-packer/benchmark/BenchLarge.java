import generated_bench.Vec3Gen.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.File;

public class BenchLarge {
    public static void main(String[] args) throws Exception {
        String fileName = "large_payload.bin";
        File file = new File(fileName);
        if (!file.exists()) {
            // Try to find it in root if running from elsewhere
            if (new File("../" + fileName).exists()) {
                fileName = "../" + fileName;
            } else {
                 System.err.println("File not found: " + fileName);
                 System.exit(1);
            }
        }
        
        byte[] data = Files.readAllBytes(Paths.get(fileName));
        System.out.println("Read " + data.length + " bytes from " + fileName);

        // Warmup (optional, but good for Java)
        // For large data, maybe 1 run is enough for "cold" performance, but JIT matters.
        // Let's just run once for now.

        // Decode
        long start = System.nanoTime();
        WorldState world = WorldState.decode(data);
        long end = System.nanoTime();
        double elapsedDecode = (end - start) / 1e9;
        System.out.println(String.format("Decode time: %.4fs", elapsedDecode));

        System.out.println("Decoded " + world.guilds.length + " guilds");

        // Encode
        start = System.nanoTime();
        byte[] encodedData = world.encode();
        end = System.nanoTime();
        double elapsedEncode = (end - start) / 1e9;
        System.out.println(String.format("Encode time: %.4fs", elapsedEncode));
        System.out.println("Encoded size: " + encodedData.length + " bytes");

        if (encodedData.length != data.length) {
            System.out.println("WARNING: Encoded size mismatch! Original: " + data.length + ", New: " + encodedData.length);
        }
    }
}

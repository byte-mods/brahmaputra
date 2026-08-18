import sys
import os
import time

# Ensure we can import the generated code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "generated/python_bench/python")))

try:
    from bench_complex import WorldState
    print("Successfully imported generated python module")
except ImportError as e:
    print(f"Failed to import generated module: {e}")
    sys.exit(1)

def main():
    file_name = "large_payload.bin"
    if not os.path.exists(file_name):
        print(f"Error: {file_name} not found")
        return

    with open(file_name, "rb") as f:
        data = f.read()
    
    print(f"Read {len(data)} bytes from {file_name}")

    # Decode
    start = time.time()
    world = WorldState.decode(data)
    end = time.time()
    print(f"Decode time: {end - start:.4f}s")

    print(f"Decoded {len(world.guilds)} guilds")

    # Encode
    start = time.time()
    encoded_data = world.encode()
    end = time.time()
    print(f"Encode time: {end - start:.4f}s")
    print(f"Encoded size: {len(encoded_data)} bytes")

    if len(encoded_data) != len(data):
        print(f"WARNING: Encoded size mismatch! Original: {len(data)}, New: {len(encoded_data)}")

if __name__ == "__main__":
    main()

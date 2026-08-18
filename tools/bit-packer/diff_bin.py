
import sys

def hex_dump(data):
    return " ".join(f"{b:02x}" for b in data)

def main():
    with open("test_data.bin", "rb") as f:
        d1 = f.read()
    with open("test_data_csharp.bin", "rb") as f:
        d2 = f.read()
    
    print(f"Python: {len(d1)} bytes")
    print(f"C#:     {len(d2)} bytes")
    
    if d1 == d2:
        print("IDENTICAL")
        sys.exit(0)
    
    print("DIFF:")
    for i in range(min(len(d1), len(d2))):
        if d1[i] != d2[i]:
            print(f"Offset {i}: Py={d1[i]:02x} C#={d2[i]:02x}")
            # Show surrounding bytes
            start = max(0, i - 5)
            end = min(len(d1), i + 5)
            print(f"Py Context: {hex_dump(d1[start:end])}")
            print(f"C# Context: {hex_dump(d2[start:end])}")
            break

if __name__ == "__main__":
    main()

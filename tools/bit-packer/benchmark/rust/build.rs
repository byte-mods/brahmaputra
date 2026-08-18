fn main() {
    prost_build::compile_protos(&["../schemas/bench_complex.proto"], &["../schemas/"]).unwrap();

    // Generate FlatBuffers code
    let status = std::process::Command::new("flatc")
        .args(&["--rust", "-o", "src/", "../schemas/bench_complex.fbs"])
        .status()
        .expect("failed to execute flatc");

    if !status.success() {
        panic!("flatc failed");
    }
}

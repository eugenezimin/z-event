//! Generates the gRPC code from `proto/zevent/v1/feed.proto` with protox
//! (a pure-Rust protobuf compiler), so no system `protoc` is needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::compile(["zevent/v1/feed.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(descriptors)?;
    Ok(())
}

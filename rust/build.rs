fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path =
        protoc_bin_vendored::protoc_bin_path().expect("could not find bundled protoc binary");
    unsafe { std::env::set_var("PROTOC", protoc_path) };

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        // Vendored copy, kept in sync by ../scripts/sync-proto.sh. It has to
        // live inside the crate: a published crates.io package contains only
        // its own directory, so `../proto` would not exist for consumers.
        .compile_protos(&["proto/statelet.proto"], &["proto"])?;

    Ok(())
}

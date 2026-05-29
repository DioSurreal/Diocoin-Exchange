// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/matching_engine.proto");

    tonic_build::compile_protos("proto/matching_engine.proto")?;
    Ok(())
}
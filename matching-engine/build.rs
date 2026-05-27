// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the proto file into a Rust module, hidden in the target/ folder
    tonic_build::compile_protos("proto/matching_engine.proto")?;
    Ok(())
}
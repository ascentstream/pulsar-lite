fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&["../../proto/MLDataFormats.proto"], &["../../proto"])?;
    Ok(())
}

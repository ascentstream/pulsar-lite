fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 编译 Pulsar 原生协议（用于二进制协议支持）
    let out_dir = std::env::var("OUT_DIR").unwrap();
    prost_build::Config::new()
        .out_dir(&out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/PulsarApi.proto"], &["proto"])?;

    Ok(())
}

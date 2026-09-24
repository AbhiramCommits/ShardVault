use std::env;
use std::path::PathBuf;

fn main() {
    let csrc = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("../../csrc")
        .canonicalize()
        .unwrap();

    println!("cargo:rerun-if-changed={}", csrc.join("block.c").display());
    println!("cargo:rerun-if-changed={}", csrc.join("block.h").display());

    cc::Build::new()
        .file(csrc.join("block.c"))
        .include(&csrc)
        .flag("-std=c11")
        .flag("-O2")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .compile("sv_block");
}

use std::env;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    if target_os == "windows" {
        // Export symbols for Stata plugin loading
        println!("cargo:rustc-link-arg=-Wl,--export-all-symbols");
        println!("cargo:rustc-link-arg=-Wl,--enable-auto-import");
        println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
    }
}

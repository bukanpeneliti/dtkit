use std::env;
use std::path::PathBuf;

fn main() {
    // Detect the target OS
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    // Set up C/C++ compilation
    let mut build = cc::Build::new();

    // Configure build based on target OS
    match target_os.as_str() {
        "windows" => {
            build
                .define("SYSTEM", "STWIN32")
                .flag("-shared")
                .flag("-fPIC");
        }
        "macos" => {
            build.define("SYSTEM", "APPLEMAC").flag("-bundle");
        }
        _ => {
            // Assume Linux/Unix
            build
                .define("SYSTEM", "OPUNIX")
                .flag("-shared")
                .flag("-fPIC");
        }
    }

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=vendor/stplugin.h");

    // Generate bindings using bindgen
    let bindings = bindgen::Builder::default()
        .header("vendor/stplugin.h")
        .allowlist_function("pginit")
        .allowlist_type("ST_.*")
        .allowlist_var("_stata_")
        .allowlist_var("SD_.*")
        .allowlist_var("SF_.*")
        .allowlist_var("SW_.*")
        .allowlist_var("SV_.*")
        .clang_arg(match target_os.as_str() {
            "windows" => "-DSYSTEM=STWIN32",
            "macos" => "-DSYSTEM=APPLEMAC",
            _ => "-DSYSTEM=OPUNIX",
        })
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let binding_path = out_path.join("bindings.rs");
    bindings
        .write_to_file(binding_path)
        .expect("Couldn't write bindings!");
}

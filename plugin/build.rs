use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn read_api_version(manifest_dir: &Path) -> String {
    let cargo_toml_path = manifest_dir.join("Cargo.toml");
    let cargo_toml = fs::read_to_string(&cargo_toml_path)
        .expect("failed to read plugin/Cargo.toml for dtparquet API metadata");
    let parsed: toml::Value =
        toml::from_str(&cargo_toml).expect("failed to parse plugin/Cargo.toml as TOML");

    parsed
        .get("package")
        .and_then(|v| v.get("metadata"))
        .and_then(|v| v.get("dtparquet"))
        .and_then(|v| v.get("api"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .expect("missing [package.metadata.dtparquet] api in plugin/Cargo.toml")
}

fn patch_ado_api_marker(manifest_dir: &Path, api_version: &str) {
    let ado_path = manifest_dir
        .parent()
        .expect("missing parent directory for plugin/")
        .join("ado")
        .join("dtparquet.ado");

    let current = fs::read_to_string(&ado_path)
        .expect("failed to read ado/dtparquet.ado for API marker patching");

    let marker = "__DTPARQUET_API__";
    let updated = if current.contains(marker) {
        current.replace(marker, api_version)
    } else {
        let mut replaced = false;
        let mut out = Vec::new();
        for line in current.lines() {
            if line.contains("local expected_api \"") {
                out.push(format!("    local expected_api \"{}\"", api_version));
                replaced = true;
            } else {
                out.push(line.to_string());
            }
        }
        if !replaced {
            panic!("missing expected_api assignment in ado/dtparquet.ado for API patching");
        }
        let mut rebuilt = out.join("\n");
        if current.ends_with('\n') {
            rebuilt.push('\n');
        }
        rebuilt
    };

    if updated != current {
        fs::write(&ado_path, updated).expect("failed to patch API marker in ado/dtparquet.ado");
    }
}

fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../ado/dtparquet.ado");

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set"));
    let api_version = read_api_version(&manifest_dir);
    patch_ado_api_marker(&manifest_dir, &api_version);

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    if target_os == "windows" {
        // Export symbols for Stata plugin loading
        println!("cargo:rustc-link-arg=-Wl,--export-all-symbols");
        println!("cargo:rustc-link-arg=-Wl,--enable-auto-import");
        println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
    }
}

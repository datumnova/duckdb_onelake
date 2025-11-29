use std::env;
use std::path::{Path, PathBuf};

fn main() {
    if env::var("SKIP_ONELAKE_HEADER").is_ok() || env::var("CARGO_CFG_TEST").is_ok() {
        // Unit tests do not need the generated header. Skipping cbindgen here avoids parsing issues
        // and speeds up "cargo test" significantly.
        println!("cargo:warning=Skipping cbindgen header generation during tests");
        return;
    }

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let header_dir = env::var("ONELAKE_HEADER_DIR").ok();
    let out_dir = env::var("OUT_DIR").unwrap();

    let header_path = match header_dir {
        Some(dir) => PathBuf::from(dir).join("onelake_delta_writer.h"),
        None => Path::new(&out_dir).join("onelake_delta_writer.h"),
    };

    std::fs::create_dir_all(
        header_path
            .parent()
            .expect("header path should have parent"),
    )
    .expect("failed to create header directory");

    cbindgen::generate(&crate_dir)
        .expect("Unable to generate bindings")
        .write_to_file(&header_path);

    println!("cargo:rerun-if-env-changed=ONELAKE_HEADER_DIR");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!(
        "cargo:warning=Generated header at {}",
        header_path.display()
    );
}

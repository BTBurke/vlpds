//! The web UI (`ui/`, built by `just ui` into `ui/dist`) is embedded with
//! rust-embed. If it hasn't been built, leave a placeholder page so the
//! binary still builds and serves something useful at `/`.
fn main() {
    let dist = std::path::Path::new("ui/dist");
    let index = dist.join("index.html");
    if !index.exists() {
        std::fs::create_dir_all(dist).expect("create ui/dist");
        std::fs::write(
            &index,
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>vlpds</title></head>\
<body><p>This vlpds binary was built without its web UI. Run <code>just ui</code>, then rebuild.</p></body></html>",
        )
        .expect("write placeholder ui/dist/index.html");
    }
    println!("cargo:rerun-if-changed=ui/dist");
    println!("cargo:rerun-if-changed=build.rs");
}

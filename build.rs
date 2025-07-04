fn main() {
    cc::Build::new()
        .file("native/io.c")
        .compile("io");

    println!("cargo:rustc-link-lib=static=io");
    println!("cargo:rustc-link-lib=uring");
}
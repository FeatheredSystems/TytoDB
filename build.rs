fn main() {
    cc::Build::new()
        .file("native/io.c")
        .compile("io"); 
}

fn main() {
    let opts = hyperschema::Options::new();
    hyperschema::build("spec/hyperschema/schema.json", "hyperschema.rs", &opts).unwrap();
}

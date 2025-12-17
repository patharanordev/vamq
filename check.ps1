cargo fmt --all -- --check;
cargo clippy -- -D warnings;
cargo build --locked --verbose;
cargo test --locked --verbose;
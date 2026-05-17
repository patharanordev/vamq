cargo fmt --all -- --check;
cargo clippy --all-targets -- -D warnings;
cargo build --all-targets --locked --verbose;
cargo test --all-targets --locked --verbose;
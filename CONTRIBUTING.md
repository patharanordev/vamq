# Contributing

## Publish

Don't forget validate package:

```sh
cargo fmt
cargo clippy -- -D warnings
cargo test
cargo package
```

Login to crates.io :

```sh
cargo login <YOUR_CRATES_IO_TOKEN>
```

Validate publish without upload package:

```sh
cargo publish --dry-run
```

Publish with upload package to crates.io :

```sh
cargo publish
```

export RUSTFLAGS := "-D warnings"

fix:
    cargo +nightly fmt --all

presubmit:
    cargo +nightly fmt --all -- --check
    cargo clippy --all-targets --all-features
    cargo build --all-targets --all-features
    cargo nextest run --all-features

install-hooks:
    git config core.hooksPath .githooks

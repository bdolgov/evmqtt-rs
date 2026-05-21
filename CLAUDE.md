# Claude working notes

This repo's build/test gates live in the `Justfile`. Two recipes you
must run before committing any change:

* `just fix` — reformat with nightly rustfmt (`cargo +nightly fmt --all`).
  Run this after every code edit.
* `just presubmit` — full validation. Runs:
  - `cargo +nightly fmt --all -- --check`
  - `cargo clippy --all-targets --all-features`
  - `cargo build --all-targets --all-features`
  - `cargo nextest run --all-features`

`RUSTFLAGS=-D warnings` is exported at Justfile scope, so any new
warning is a hard error in `presubmit`.

First-time setup on a fresh checkout: `just install-hooks` points
`core.hooksPath` at `.githooks/` so the same checks run as a
pre-commit hook.

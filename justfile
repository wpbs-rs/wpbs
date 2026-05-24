default: run-dev

clean:
  cargo clean

check:
  cargo check

clippy:
  cargo clippy -- -W clippy::pedantic

clippy-fix:
  cargo clippy --fix -- -W clippy::pedantic

fmt:
  cargo fmt

build-dev:
  cargo build

build-release:
  cargo build --release

run-dev:
  cargo run

run-release:
  cargo run --release

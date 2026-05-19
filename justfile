default: run-debug

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

build-debug:
  cargo build

build-release:
  cargo build --release

run-debug:
  cargo run

run-release:
  cargo run --release

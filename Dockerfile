FROM rust:alpine AS chef
USER root
RUN apk add --no-cache \
  build-base \
  musl-dev \
  curl \
  ca-certificates
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | sh
RUN cargo binstall cargo-chef

WORKDIR /wpbs

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /wpbs/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json
COPY . .
RUN cargo build --locked --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /wpbs/target/release/wpbs /wpbs
COPY LICENSE README.md ./
CMD ["/wpbs"]

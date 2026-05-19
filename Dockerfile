FROM --platform=$BUILDPLATFORM rust:alpine AS chef
ARG LOCK_FLAG="--locked"
USER root
RUN apk add --no-cache \
  musl-dev \
  build-base \
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
RUN cargo chef cook ${LOCK_FLAG} --release --recipe-path recipe.json
COPY . .
RUN cargo build ${LOCK_FLAG} --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /wpbs/target/release/wpbs /wpbs
COPY LICENSE README.md ./
CMD ["/wpbs"]

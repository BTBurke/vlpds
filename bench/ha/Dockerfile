# vlpds node image for HA container tests (bench/ha). Build from the package root:
#   docker build -f bench/ha/Dockerfile -t vlpds-ha:local .
# glibc (debian) runtime so libfaketime (LD_PRELOAD) can skew a node's clock.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY src ./src
# compile-time inputs: bundled lexicons (include_str!) and the embedded web UI
# (rust-embed of ui/dist; build it first with the UI's own tooling)
COPY lexicons ./lexicons
COPY ui/dist ./ui/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/rustup \
    --mount=type=cache,target=/src/target \
    cargo build --profile dev-release --bin vlpds && cp target/dev-release/vlpds /vlpds

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl faketime \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /vlpds /usr/local/bin/vlpds
ENV RUST_LOG=info,slatedb=warn
EXPOSE 2583
ENTRYPOINT ["vlpds"]

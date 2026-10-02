# syntax=docker/dockerfile:1.7
# Production vlpds image: the web UI (ui/) built with node, embedded into a
# release build of the vlpds (and loadgen, vlpds-bucket-probe) binaries, on a slim non-root runtime.
#
#   docker build -t vlpds:local .            (or: just docker-build)
#   docker run -p 2583:2583 -e VLPDS_S3_ENDPOINT=... -e VLPDS_JWT_SECRET=... \
#     -e VLPDS_ADMIN_TOKEN=... -e VLPDS_INTERNAL_TOKEN=... vlpds:local
#
# Configuration is all VLPDS_* env vars (see `vlpds --help`). Prometheus
# metrics are served at /metrics on the app port, or only on
# VLPDS_METRICS_LISTEN (e.g. 0.0.0.0:9583) when set. Thread pools default to
# the container's CPUs (cgroup quota aware): --io-threads = cores, --workers =
# cores/2. vlpds raises its soft open-files limit to the hard limit at startup
# (docker run --ulimit nofile=1048576:1048576 sets the hard limit).

# --- web UI -----------------------------------------------------------------
FROM node:22-bookworm-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

# --- rust release build -----------------------------------------------------
FROM rust:1.98.1-bookworm AS build
# Extra cargo features, e.g. --build-arg VLPDS_FEATURES=profiling for
# --pyroscope-url (continuous CPU profiles).
ARG VLPDS_FEATURES=""

# cmake/clang: aws-lc-sys (rustls) and the vendored libsecp256k1 / jemalloc C builds
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY rust-toolchain.toml ./
# installs the pinned toolchain if the base image's differs
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY lexicons ./lexicons
# the manifest declares the test binary; it is never built here
RUN mkdir -p tests/all && touch tests/all/main.rs
COPY --from=ui /src/ui/dist ./ui/dist
# no debug info in the image (Cargo.toml keeps debug = 1 for local profiling;
# with debug = 0 cargo also strips std's): ~half the image. Symbols stay, so
# panics and backtraces still name functions.
ENV CARGO_PROFILE_RELEASE_DEBUG=0
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bins ${VLPDS_FEATURES:+--features "$VLPDS_FEATURES"} \
    && mkdir -p /out \
    && cp target/release/vlpds target/release/loadgen target/release/vlpds-bucket-probe /out/

# --- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 vlpds \
    && useradd --system --uid 10001 --gid vlpds --home-dir /var/lib/vlpds --create-home vlpds
# vlpds-bucket-probe: the bucket pre-flight (DESIGN.md "Choosing a bucket"),
# run with --entrypoint from the node's own env
COPY --from=build /out/vlpds /out/loadgen /out/vlpds-bucket-probe /usr/local/bin/
USER vlpds:vlpds
WORKDIR /var/lib/vlpds
ENV VLPDS_LISTEN=0.0.0.0:2583 \
    RUST_LOG=info
# 2583: XRPC, web UI, /internal (cluster) and /metrics (Prometheus)
EXPOSE 2583
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -sf http://127.0.0.1:2583/xrpc/_health || exit 1
# tini forwards SIGTERM so vlpds drains and releases its shards gracefully
ENTRYPOINT ["/usr/bin/tini", "--", "vlpds"]

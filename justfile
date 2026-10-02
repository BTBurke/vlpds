# vlpds: atproto PDS on object storage. The web UI (ui/, React + Vite) is
# built into ui/dist and embedded in the binary (src/xrpc/webui.rs).

target_dir := env_var_or_default("CARGO_TARGET_DIR", "target")

# Build the web UI into ui/dist (embedded by the next cargo build)
ui:
    cd ui && npm install --no-audit --no-fund && npm run build

# Vite dev server on :5620, proxying /xrpc, /oauth, /metrics, /internal to a local vlpds (VLPDS_URL, default http://127.0.0.1:2620)
dev-ui url="http://127.0.0.1:2620":
    cd ui && npm install --no-audit --no-fund && VLPDS_URL={{url}} npm run dev

# Build the UI, then the vlpds and loadgen binaries (dev-release profile)
build: ui
    cargo build --profile dev-release --bins

# Release build with the UI embedded
build-release: ui
    cargo build --release --bins

# An in-memory dev server with the UI on :2620 (admin token: dev-admin-token)
dev: build
    {{target_dir}}/dev-release/vlpds --memory --listen 127.0.0.1:2620 --public-url http://127.0.0.1:2620 --dev-mode --no-rate-limits

# Seed a running dev server with accounts and records (password: hunter2)
seed accounts="3" records="200":
    {{target_dir}}/dev-release/loadgen --host http://127.0.0.1:2620 setup --accounts {{accounts}} --records {{records}}

test *args:
    cargo test {{args}}

# Two-build HA scenarios (bench/ha/upgrade.sh: builds the previous release + this tree, plain and
# with the test feature level, then runs hactl.py upgrade-*). `--minio` first = throwaway MinIO container
upgrade-ha *args:
    bench/ha/upgrade.sh {{args}}

# Rolling-upgrade CI gate (DESIGN.md "Tests and CI"): format fixtures + MANIFEST freeze, the
# level-gating test with the test feature level, and one two-build scenario on a throwaway MinIO
upgrade-ci scenario="upgrade-rolling":
    cargo test --test all formats::
    cargo test --features test-level --test level_gating
    VLPDS_HA_S3=127.0.0.1:9260 bench/ha/upgrade.sh --minio {{scenario}}

# Local MinIO (build/docker-compose.yml) on :9000 (console :9001), with the `vlpds` bucket created
minio:
    docker compose -f build/docker-compose.yml up -d --build --wait minio
    docker compose -f build/docker-compose.yml run --rm minio-init

# Stop the local MinIO (keeps its volume; `docker compose -f build/docker-compose.yml down -v` wipes it)
minio-down:
    docker compose -f build/docker-compose.yml down

# bench/step.sh writes ./target/release whatever CARGO_TARGET_DIR says; env ACCOUNTS, RECORDS, DURATION, OUT.
# One benchmark step on a fresh RAM-backed MinIO: just bench <name> <rate> [hot_rate] [inject_put_ms] [vlpds args...]
bench name rate *args:
    docker compose -f build/docker-compose.yml build minio
    CARGO_TARGET_DIR=target cargo build --release --bins
    bench/step.sh {{name}} {{rate}} {{args}}

# Build the Go sync 1.1 firehose checker and run it against a vlpds (extra flags e.g. -cursor 0 -strict)
checker host="http://127.0.0.1:2620" *args:
    cd checker && go build -o checker . && ./checker -host {{host}} {{args}}

# Build the Rust sync 1.1 checker (checker-rs, on shrike) and run it against a vlpds (extra flags e.g. -cursor 0 -strict)
checker-rs host="http://127.0.0.1:2620" *args:
    cd checker-rs && cargo run --release --quiet -- -host {{host}} {{args}}

# Production image (Dockerfile: UI build, release build, slim non-root runtime)
docker-build tag="vlpds:local":
    docker build -t {{tag}} .

# Build + push the production image for deploy/ansible's vlpds_image (docker login ghcr.io first; features e.g. profiling)
docker-push tag=`git rev-parse --short=12 HEAD` image="ghcr.io/jazware/vlpds" platform="linux/amd64" features="":
    docker buildx build --platform {{platform}} --build-arg VLPDS_FEATURES={{features}} -t {{image}}:{{tag}} --push .

# Observability stack for load tests (bench/obs/README.md): Prometheus (1 s scrapes) :9090,
# Grafana (vlpds dashboard, anonymous admin) :3300, Pyroscope :4040, all on 127.0.0.1
obs-up:
    python3 bench/obs/minio-token.py
    docker compose -f bench/obs/docker-compose.yml up -d --wait
    @echo "grafana http://127.0.0.1:3300/d/vlpds  prometheus http://127.0.0.1:9090  pyroscope http://127.0.0.1:4040"

# Stop the observability stack (keeps its data; `docker compose -f bench/obs/docker-compose.yml down -v` wipes it)
obs-down:
    docker compose -f bench/obs/docker-compose.yml down

# Regenerate the vlpds Grafana dashboards (operator `vlpds` + `vlpds-internals`): bench copies and deploy/ansible's (--check: exit 1 if any is stale)
dashboards *args:
    python3 bench/obs/grafana/gen_dashboard.py {{args}}

# CPU profile of a running vlpds (built with --features profiling): top functions by self and cumulative time
profile host="127.0.0.1:2583" seconds="10" *args:
    bench/obs/profile.sh {{args}} {{host}} {{seconds}}

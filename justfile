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

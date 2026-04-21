set dotenv-load := true

# Start local Postgres.
up:
    docker compose up -d postgres

# Stop and remove Postgres (keeps the named volume).
down:
    docker compose down

# Wipe Postgres data volume as well.
clean:
    docker compose down -v

# Run the example indexer against the testnet config.
run-example:
    cargo run -p counter-indexer -- -c examples/counter-indexer/config.testnet.yaml

# Static checks.
check:
    cargo check --workspace --all-targets

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

fmt:
    cargo fmt --all

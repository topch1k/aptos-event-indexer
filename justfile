set dotenv-load := true

# ────────────────────────────────────────────────────────────────────────────
# Move contract (examples/counter-contract)
# ────────────────────────────────────────────────────────────────────────────

contract_dir := "examples/counter-contract"

# Aptos CLI profile used by `contract-publish` / `contract-upgrade`.
# Override at the CLI: `just aptos_profile=my-profile contract-publish`.
aptos_profile := "default"

# Address the contract is published under. Usually equal to the profile's
# account; override via `just counter_address=0xabc... contract-publish`.
counter_address := "default"

# Compile the Move package. Requires the `aptos` CLI on PATH.
contract-build:
    aptos move compile \
        --package-dir {{contract_dir}} \
        --named-addresses counter={{counter_address}}

# Run Move unit tests, if any.
contract-test:
    aptos move test \
        --package-dir {{contract_dir}} \
        --named-addresses counter={{counter_address}}

# First-time publish to the configured Aptos profile's network.
# Creates the module; subsequent deployments go through `contract-upgrade`.
contract-publish:
    aptos move publish \
        --package-dir {{contract_dir}} \
        --named-addresses counter={{counter_address}} \
        --profile {{aptos_profile}} \
        --assume-yes

# Upgrade an already-published package (same as publish, kept as a distinct
# recipe for clarity in CI / runbooks).
contract-upgrade:
    aptos move upgrade \
        --package-dir {{contract_dir}} \
        --named-addresses counter={{counter_address}} \
        --profile {{aptos_profile}} \
        --assume-yes

# ────────────────────────────────────────────────────────────────────────────
# Localnet helpers
# ────────────────────────────────────────────────────────────────────────────

# Start an Aptos localnet with faucet + indexer gRPC on :50051.
# Leave this running in its own terminal.
localnet:
    aptos node run-local-testnet \
        --with-indexer-api \
        --force-restart \
        --assume-yes


# Create/overwrite a CLI profile named `local` pointing at the localnet.
# The faucet auto-funds the new account.
localnet-init:
    aptos init --profile local \
        --network custom \
        --rest-url http://localhost:8080 \
        --faucet-url http://localhost:8081 \
        --assume-yes

# Create/overwrite a second CLI profile `local_greeter` pointing at the same
# localnet. Having a separate profile means the greeter module gets
# published by a different account and therefore lives at a distinct address
# from counter — which is the whole point of this second example.
localnet-init-greeter:
    aptos init --profile local_greeter \
        --network custom \
        --rest-url http://localhost:8080 \
        --faucet-url http://localhost:8081 \
        --assume-yes

# Publish the counter module to the localnet using the `local` profile.
# Resolves the profile's account address automatically.
contract-publish-local:
    #!/usr/bin/env bash
    set -euo pipefail
    raw=$(aptos config show-profiles --profile local \
        | awk '/account/ {print $2; exit}')
    # Strip surrounding quotes, trailing commas, and a leading 0x if present,
    # then re-prefix with 0x so we end up with exactly one.
    addr=$(printf '%s' "$raw" | tr -d '",' | sed 's/^0x//')
    if [[ -z "$addr" ]]; then
        echo "no 'local' profile found — run 'just localnet-init' first" >&2
        exit 1
    fi
    echo "publishing counter to 0x$addr on localnet"
    aptos move publish \
        --package-dir {{contract_dir}} \
        --named-addresses counter=0x"$addr" \
        --profile local \
        --assume-yes

# Resolve the local profile's 0x-prefixed account address.
# Used by counter-* recipes below.
_local-addr:
    #!/usr/bin/env bash
    set -euo pipefail
    raw=$(aptos config show-profiles --profile local \
        | awk '/account/ {print $2; exit}')
    addr=$(printf '%s' "$raw" | tr -d '",' | sed 's/^0x//')
    if [[ -z "$addr" ]]; then
        echo "no 'local' profile found — run 'just localnet-init' first" >&2
        exit 1
    fi
    printf '0x%s\n' "$addr"

# Resolve the local_greeter profile's 0x-prefixed account address, or fall
# back to the `local` profile if `local_greeter` has not been initialized.
# The fallback keeps the single-module workflow (counter-only) working for
# users who haven't run `just localnet-init-greeter`.
_local-greeter-addr:
    #!/usr/bin/env bash
    set -euo pipefail
    if aptos config show-profiles --profile local_greeter >/dev/null 2>&1; then
        raw=$(aptos config show-profiles --profile local_greeter \
            | awk '/account/ {print $2; exit}')
        addr=$(printf '%s' "$raw" | tr -d '",' | sed 's/^0x//')
        if [[ -n "$addr" ]]; then
            printf '0x%s\n' "$addr"
            exit 0
        fi
    fi
    # Fallback: reuse the counter address so the indexer still starts.
    just _local-addr

# Publish the greeter module to the localnet using the `local_greeter`
# profile. Creates a `GreetedEvent`-emitting module at an address distinct
# from counter's, wired up so the example indexer picks it up automatically.
greeter-publish-local:
    #!/usr/bin/env bash
    set -euo pipefail
    raw=$(aptos config show-profiles --profile local_greeter \
        | awk '/account/ {print $2; exit}')
    addr=$(printf '%s' "$raw" | tr -d '",' | sed 's/^0x//')
    if [[ -z "$addr" ]]; then
        echo "no 'local_greeter' profile — run 'just localnet-init-greeter' first" >&2
        exit 1
    fi
    echo "publishing greeter to 0x$addr on localnet"
    aptos move publish \
        --package-dir examples/greeter-contract \
        --named-addresses greeter=0x"$addr" \
        --profile local_greeter \
        --assume-yes

# Emit one GreetedEvent. Usage: `just greeter-hello` or
# `just msg="hi there" greeter-hello`.
msg := "hello from localnet"
greeter-hello:
    #!/usr/bin/env bash
    set -euo pipefail
    addr=$(just _local-greeter-addr)
    aptos move run --profile local_greeter --assume-yes \
        --function-id "$addr"::greeter::greet \
        --args "string:{{msg}}"

# Initialize the caller's Counter resource on localnet. Run once per account.
counter-init:
    #!/usr/bin/env bash
    set -euo pipefail
    addr=$(just _local-addr)
    aptos move run --profile local --assume-yes \
        --function-id "$addr"::counter::initialize

# Increment the caller's counter on localnet by `by` (default 1).
# Usage: `just counter-increment` or `just by=5 counter-increment`.
by := "1"
counter-increment:
    #!/usr/bin/env bash
    set -euo pipefail
    addr=$(just _local-addr)
    aptos move run --profile local --assume-yes \
        --function-id "$addr"::counter::increment \
        --args u64:{{by}}

# Decrement the caller's counter on localnet by `by` (default 1).
# Usage: `just counter-decrement` or `just by=2 counter-decrement`.
counter-decrement:
    #!/usr/bin/env bash
    set -euo pipefail
    addr=$(just _local-addr)
    aptos move run --profile local --assume-yes \
        --function-id "$addr"::counter::decrement \
        --args u64:{{by}}

# Print the current counter value for the local profile's account.
counter-value:
    #!/usr/bin/env bash
    set -euo pipefail
    addr=$(just _local-addr)
    aptos move view \
        --profile local \
        --function-id "$addr"::counter::get_value \
        --args address:"$addr"

# Run the indexer against the localnet config. Auto-injects both
# `COUNTER_MODULE_ADDR` and `GREETER_MODULE_ADDR` from their respective
# aptos CLI profiles so the indexer picks up whichever addresses the
# contracts were most recently published to. If `local_greeter` has not
# been initialized, the greeter address falls back to the counter address
# (the greeter processor then simply never matches anything).
run-example-local:
    #!/usr/bin/env bash
    set -euo pipefail
    counter_addr=$(just _local-addr)
    greeter_addr=$(just _local-greeter-addr)
    COUNTER_MODULE_ADDR="$counter_addr" \
    GREETER_MODULE_ADDR="$greeter_addr" \
        cargo run -p counter-indexer -- -c examples/counter-indexer/config.localnet.yaml

# ────────────────────────────────────────────────────────────────────────────
# Infra + indexer
# ────────────────────────────────────────────────────────────────────────────

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

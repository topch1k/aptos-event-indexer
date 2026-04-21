# greeter-contract

Minimal second Move module that only emits a `GreetedEvent { who, message }`
when `greet` is called. Its purpose is to demonstrate that a single indexer
can consume events from multiple Move modules published at **different**
on-chain addresses — see the top-level `README.md` "Multiple modules"
section.

## Layout

- `sources/greeter.move` — one stateless `entry fun greet(signer, String)`.
- `[addresses] greeter = "_"` in `Move.toml`; the real address is supplied
  via `--named-addresses greeter=0x...` at publish time.

## Localnet publish

Uses a **separate** CLI profile (`local_greeter`) so the module ends up at
a different address from `counter-contract`:

```sh
just localnet-init-greeter     # one-time: creates + funds profile
just greeter-publish-local     # publishes under that profile's address
just greeter-hello msg="hello" # emits one GreetedEvent
```

`just run-example-local` auto-injects both `COUNTER_MODULE_ADDR` and
`GREETER_MODULE_ADDR` so the indexer picks up whichever addresses the
contracts were most recently published to.

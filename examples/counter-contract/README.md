# counter-contract

Minimal Move module demonstrating the indexer. Publishes a per-account
`Counter` resource and emits two events consumed by
[`examples/counter-indexer`](../counter-indexer):

- `CounterIncrementedEvent { account, old_value, new_value, increment_by }`
- `CounterDecrementedEvent { account, old_value, new_value, decrement_by }`

## Publish

```sh
aptos init --network testnet
aptos move publish \
  --package-dir examples/counter-contract \
  --named-addresses counter=<YOUR_ACCOUNT_ADDRESS>
```

After publishing, export `COUNTER_MODULE_ADDR=0x<your deployed address>`
before running the indexer. The `just run-example-local` recipe auto-injects
this from the `local` CLI profile; for other targets set it yourself.

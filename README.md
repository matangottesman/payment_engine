# Payments engine

Small Rust binary that streams a transactions CSV, applies each operation, and writes final account balances to stdout
as CSV.

## Running

- `cargo run -- <transactions.csv> > accounts.csv`
- Input CSV must have the headers `type,client,tx,amount`; `amount` is empty for dispute/resolve/chargeback rows.

## Behavior

- Deposits increase `available` and `total`.
- Withdrawals decrease `available` and `total` when funds are available; otherwise the withdrawal is ignored.
- Disputes move funds from `available` to `held` for the referenced deposit transaction.
- Resolves move disputed funds back to `available`.
- Chargebacks remove disputed funds from `held`, reduce `total`, and lock the account. Locked accounts ignore further
  transactions.

## Code Gen Tool Use

- Codex and ChatGPT were use primarily for initial repo setup, test framework generation, and minimal refactoring.

## Assumptions

- Only deposits can be disputed/resolved/charged back. Transaction modification requests against withdrawals are
  ignored. It wouldn't make sense for a customer to dispute a withdrawal, and that is not the problem we are trying to
  solve with this payment tracking system.
- Duplicate transaction IDs are ignored.
- Transactions that invalid based on existing state are ignored.
- Withdrawals that do not have the required balance are still valid transactions to be tracked, even though they are
  rejected.
- Invalid or malformed CSV rows are ignored with a io::stderr log warning.

## General Improvements

- Error handling should be much more robust and log to a file in unexpected cases, so that we have a history of what
  information has been passed through our payment system but ignored. For now, I don't want to mess with potential
  issues for the environment in which this program is run.
- I've chosen to keep everything contained within a single module due to the relatively small size of the solution. As
  the solution expands, breaking things into modules based on responsibility would improve quality.
- There are some unused variables that in a completed system probably shouldn't exist. However, I see the scope of this
  sort of project increasing, and therefore maintaining a structure to represent previous and existing state
  seems reasonable.

## Concurrency Considerations

- The current scope of solution does not require multithreading or async handling to process the input file stream.
  However, it is mentioned that it could be a consideration for the future. If we're receiving CSVs multiple TCP
  connections, we could alter the data structures to allow for concurrent mutation to values within our account Hashmap.
  A library like DashMap shards its internal storage to allow this sort of behavior, so we could explore a similar
  solution. We could potentially get even more granular and lock on transactions instead of client accounts, but it
  depends on the full problem space and how these transactions are processed in a more real-world scenario.

## Testing

Unit tests and a full end-to-end test using the provided `tests/sample_transactions.csv` file

```cargo test```

## Format and Lint

```cargo +nightly fmt```

```cargo clippy --fix --allow-dirty --allow-staged```

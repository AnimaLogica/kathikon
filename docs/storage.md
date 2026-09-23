# Storage

Kathikon v0.2.0 introduces a formal `Kathikon.Storage` behaviour. Mnesia remains the default backend (`Kathikon.Storage.Mnesia`), but the public API does not assume Mnesia.

## Why a behaviour?

Alternate backends can implement the same atomic callbacks without changing dispatchers or management APIs. Planned backends are Ecto (v0.5.0), MongoDB (v0.6.0), and SQS (v0.7.0).

## Atomic operations

All lifecycle transitions (`claim_job`, `complete_job`, `fail_job`, `retry_job`, `discard_job`, `cancel_job`, `move_to_dead_letter`) run inside Mnesia transactions. Avoid read-then-update patterns across separate calls.

## Result storage

Configure per job:

```elixir
Kathikon.insert(MyWorker, args, result: :store)   # default
Kathikon.insert(MyWorker, args, result: :discard)
```

Errors are always stored in an inspectable form on the job record and in history events.

## Mnesia term storage tradeoff

Jobs are serialized with `:erlang.term_to_binary/1`. This is fast and native on the BEAM, but:

- Payloads are not portable across languages
- Large or deeply nested terms increase memory and copy cost
- Untrusted binary input must never be decoded with `:safe` disabled

For cross-node clusters, keep args small and prefer IDs referencing external data.

## Tables

| Table | Purpose |
|-------|---------|
| `:kathikon_jobs` | Job payloads |
| `:kathikon_history` | Durable lifecycle events |
| `:kathikon_batches` | Batch coordination |
| `:kathikon_schedules` | Built-in recurring registrations |

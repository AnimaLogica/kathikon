# Configuration

All options are set under `config :kathikon, ...`.

Requires `config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase` when using `:timezone` or cron scheduling.

## Full example

```elixir
# config/config.exs
config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase

config :kathikon,
  queues: [
    default: [concurrency: 10],
    emails: [concurrency: 5],
    reports: [concurrency: 2]
  ],
  poll_interval: 200,
  scheduler_interval: 1_000,
  prune_interval: 60_000,
  retention_period: :timer.hours(24 * 7),
  max_attempts: 20,
  insert_many_chunk_size: 500,
  timezone: "Etc/UTC",
  mnesia_copies: :auto,
  storage_backend: Kathikon.Storage.Mnesia,
  scheduler: Kathikon.Scheduler.BuiltIn,
  cron_tick: true,
  result: :store
```

## Options reference

| Key | Default | Description |
|-----|---------|-------------|
| `:queues` | `[default: [concurrency: 10]]` | Queue names and per-queue options |
| `:poll_interval` | `200` | Dispatcher poll period (ms) |
| `:scheduler_interval` | `1000` | Scheduler tick period (ms) |
| `:prune_interval` | `60000` | Pruner tick period (ms) |
| `:retention_period` | `604800000` (7 days) | How long to keep terminal jobs (ms) |
| `:max_attempts` | `20` | Default retry limit for new jobs |
| `:insert_many_chunk_size` | `500` | Jobs per `insert_many/2` storage transaction |
| `:timezone` | `"Etc/UTC"` | IANA zone for cron and naive `schedule_at` |
| `:mnesia_copies` | `:auto` | Mnesia table storage — `:ram`, `:disc`, or `:auto` |
| `:storage_backend` | `Kathikon.Storage.Mnesia` | Storage behaviour implementation |
| `:scheduler` | `Kathikon.Scheduler.BuiltIn` | Scheduler adapter module |
| `:cron_tick` | `true` | Start `Kathikon.Scheduler.BuiltIn.Tick` (set `false` in tests) |
| `:result` | `:store` | Persist worker return values (`:store` or `:discard`) |
| `:quantum_scheduler` | — | Quantum scheduler module when using `Scheduler.Quantum` |

### Queue options

Each queue entry is a keyword list:

```elixir
[concurrency: 10]   # max simultaneous Task workers
```

Access at runtime:

```elixir
Kathikon.Config.queue_names()      # [:default, :emails, ...]
Kathikon.Config.concurrency(:emails) # 5
Kathikon.Config.queue_config(:emails)
```

### mnesia_copies

| Value | Behaviour |
|-------|-----------|
| `:auto` | `ram` on `nonode@nohost` and Livebook nodes; `disc` on other named nodes |
| `:ram` | In-memory tables — dev, test, Livebook |
| `:disc` | Durable on disk — production named nodes |

```elixir
# Livebook / local scripts
config :kathikon, mnesia_copies: :ram
```

## Environment-specific overrides

```elixir
# config/test.exs
import Config

config :kathikon,
  poll_interval: 50,
  scheduler_interval: 50,
  retention_period: 1,
  max_attempts: 3,
  queues: [default: [concurrency: 10], priority: [concurrency: 1]]
```

## Reading config in code

```elixir
Kathikon.Config.poll_interval()
Kathikon.Config.retention_period()
Kathikon.Config.mnesia_copies()
```

## Related

- [Storage & embedding](storage-and-embedding.md)
- [Module reference: Kathikon.Config](../reference/modules.md#kathikonconfig)

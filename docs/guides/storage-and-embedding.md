# Storage and embedding

Kathikon persists jobs in **Mnesia** via `Kathikon.Storage` and `Kathikon.Storage.Mnesia`.

## Automatic startup

When Kathikon is a dependency, `Kathikon.Application` runs on boot:

1. `Kathikon.Storage.setup/0` — Mnesia schema + `:kathikon_jobs` table
2. Starts Registry, Queue supervisor, Scheduler, Pruner
3. `Kathikon.Queue.start_configured/0` — dispatchers for configured queues

No manual Mnesia setup is required in a standard Phoenix or Mix release.

## Manual embedding

If you start Kathikon outside its application (scripts, Livebook, tests):

```elixir
Application.put_env(:kathikon, :mnesia_copies, :ram)

{:ok, _} = Application.ensure_all_started(:kathikon)
# or, without full app:
:ok = Kathikon.Storage.setup()
```

## Livebook

Livebook nodes are named but lack a Mnesia disc directory. Use RAM copies:

```elixir
Mix.install(
  [{:kathikon, path: "..", env: :prod}],
  config: [kathikon: [mnesia_copies: :ram, poll_interval: 150, ...]]
)
```

See the [interactive demo](../../livebooks/kathikon_demo.livemd).

## Test helpers

```elixir
# Clear jobs between integration tests
:ok = Kathikon.Storage.clear_jobs!()

# Facade tests (Kathikon.cancel/1, Kathikon.fetch/1) — mock scoped to the test process
Kathikon.TestSupport.use_mock_storage!(context)

# Runtime unit tests — inject storage at start_link/1
{:ok, pid} =
  Kathikon.Dispatcher.start_link(
    queue: :test,
    config: [concurrency: 1],
    storage: Kathikon.Storage.Mock
  )

Mox.allow(Kathikon.Storage.Mock, self(), pid)
```

Do **not** reconfigure the global `storage_backend` in tests.
The application scheduler, pruner, and dispatchers always use Mnesia configured at boot.
Mock tests start their own processes with `storage:` or scope mocks with `use_mock_storage!/1`.

`clear_jobs!/0` and `reset!/0` are intended for tests — not public production APIs.

## Storage facade

`Kathikon.Storage` delegates to a configurable backend:

| Function | Purpose |
|----------|---------|
| `setup/0` | Bootstrap schema and tables |
| `insert/1`, `update/1`, `fetch/1` | Job CRUD |
| `claim/2` | Atomic claim for a queue |
| `promote_scheduled/1` | Scheduler batch promotion |
| `prunable_jobs/1`, `delete/1` | Pruner support |
| `all/0` | List all jobs |

Application code should use `Kathikon.insert/3` rather than calling `Storage` directly.

## Backend plug-in

```elixir
config :kathikon, storage_backend: Kathikon.Storage.Mnesia
```

The behaviour is `Kathikon.Storage`. v0.3.0 still ships one implementation (`Kathikon.Storage.Mnesia`). Planned backends: Ecto in v0.5.0, MongoDB in v0.6.0, and SQS in v0.7.0.

## Tables

| Table | Contents |
|-------|----------|
| `:kathikon_jobs` | Job records (id, serialized `%Kathikon.Job{}`) |
| `:kathikon_history` | Durable lifecycle events |
| `:kathikon_batches` | Batch coordination records |
| `:kathikon_schedules` | Built-in cron schedule registrations |

Queue configuration lives in `config :kathikon, queues:` (`Kathikon.Config`), not in Mnesia.

## Related

- [Configuration](configuration.md) — `mnesia_copies`, `storage_backend`
- [Module reference: Kathikon.Storage](../reference/modules.md#kathikonstorage)

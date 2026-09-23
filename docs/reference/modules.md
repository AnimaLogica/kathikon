# Module reference

Public API and runtime modules for Kathikon v0.3.0. For narrative guides see the [documentation index](../documentation.html) or [quick start](../guides/quick-start.html).

---

## Kathikon

Main entry point. Enqueue, cancel, and inspect jobs.

### `insert/3`

```elixir
@spec insert(module(), map(), keyword()) :: {:ok, Job.t()} | {:error, term()}
```

Enqueues a job. Starts the target queue dispatcher if needed. Emits `[:kathikon, :job, :inserted]`.

**Options**

| Option | Default | Description |
|--------|---------|-------------|
| `:queue` | `:default` | Target queue atom |
| `:priority` | `0` | Higher values claim first |
| `:max_attempts` | `Kathikon.Config.max_attempts()` | Retry limit |
| `:schedule_in` | — | Seconds until available |
| `:schedule_at` | — | `DateTime` when job becomes available |

**Examples**

```elixir
# Immediate job
{:ok, job} = Kathikon.insert(MyWorker, %{"id" => "1"})

# Email queue, high priority
{:ok, job} =
  Kathikon.insert(EmailWorker, %{"to" => "a@b.com"},
    queue: :emails,
    priority: 5
  )

# Delayed job
{:ok, job} =
  Kathikon.insert(DigestWorker, %{},
    schedule_in: 3600
  )

# Fixed time
{:ok, job} =
  Kathikon.insert(ReportWorker, %{},
    schedule_at: ~U[2026-12-25 09:00:00Z]
  )
```

### `cancel/1`

```elixir
@spec cancel(String.t()) :: {:ok, Job.t()} | {:error, term()}
```

```elixir
{:ok, job} = Kathikon.cancel("abc123...")
# {:error, :executing}
# {:error, {:invalid_state, :completed}}
```

### `fetch/1`

```elixir
@spec fetch(String.t()) :: {:ok, Job.t()} | {:error, :not_found}
```

```elixir
{:ok, job} = Kathikon.fetch(job_id)
job.state
job.attempts
job.errors
```

### `all/0`

```elixir
@spec all() :: [Job.t()]
```

Returns every job in storage. Intended for tests and debugging.

```elixir
Kathikon.all()
|> Enum.count(&(&1.state == :completed))
```

### `start_queue/1`

```elixir
@spec start_queue(atom()) :: :ok
```

Ensures a dispatcher is running for the queue.

```elixir
:ok = Kathikon.start_queue(:imports)
```

### `schedule/3`

```elixir
@spec schedule(module(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
```

Enqueue a one-time or recurring job. Returns the new job id. Supports `:at`, `:in`, and `:cron` (requires `Tzdata` and `config :elixir, :time_zone_database`).

```elixir
{:ok, job_id} = Kathikon.schedule(DigestWorker, %{}, in: 3600)
{:ok, job_id} = Kathikon.schedule(ReportWorker, %{}, cron: "0 9 * * 1")
```

### `status/1` and `history/1`

```elixir
{:ok, %{state: :completed, attempts: 1}} = Kathikon.status(job_id)
events = Kathikon.history(job_id)
```

### `claim/2` and `claim_available/2`

Management APIs for external runners. Prefer normal dispatcher execution for app workers.

```elixir
{:ok, job} = Kathikon.claim(job_id)
{:ok, job} = Kathikon.claim_available(:imports)
```

### `retry/2`, `rerun/2`, `dead_jobs/1`

```elixir
{:ok, job} = Kathikon.retry(job_id)
{:ok, job} = Kathikon.rerun(dead_job_id)
Kathikon.dead_jobs(queue: :default)
```

---

## Kathikon.Job

Struct representing a durable job obligation.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String.t()` | Unique hex id |
| `queue` | `atom()` | Target queue |
| `worker` | `module()` | Worker module |
| `args` | `map()` | Arguments for `perform/1` |
| `state` | atom | Current lifecycle state |
| `priority` | `non_neg_integer()` | Claim ordering |
| `max_attempts` | `pos_integer()` | Retry ceiling |
| `attempts` | `non_neg_integer()` | Completed attempts |
| `scheduled_at` | `DateTime \| nil` | When scheduled job becomes due |
| `available_at` | `DateTime \| nil` | When job can be claimed |
| `inserted_at` | `DateTime \| nil` | Insert time |
| `started_at` | `DateTime \| nil` | Last claim time |
| `completed_at` | `DateTime \| nil` | Success or discard time |
| `cancelled_at` | `DateTime \| nil` | Cancel time |
| `errors` | `[map()]` | Failure history |

### States

`:scheduled` → `:available` → `:claimed` → `:running` → `:completed` | `:retryable` | `:dead` | `:cancelled`

### `build/3`

```elixir
@spec build(module(), map(), keyword()) :: t()
```

Low-level job construction. Prefer `Kathikon.insert/3`.

```elixir
job = Kathikon.Job.build(MyWorker, %{"x" => 1}, queue: :default, priority: 2)
```

### `claimable?/2`

```elixir
@spec claimable?(t(), DateTime.t()) :: boolean()
```

```elixir
Kathikon.Job.claimable?(job, DateTime.utc_now())
# true when state in [:available, :retryable] and available_at <= now
```

### `backoff_seconds/1`

```elixir
Kathikon.Job.backoff_seconds(2)  # 20
```

---

## Kathikon.Worker

Behaviour for job workers.

### Callback

```elixir
@callback perform(Kathikon.Job.t()) ::
            :ok
            | {:ok, term()}
            | {:error, term()}
            | {:discard, term()}
            | {:retry, term()}
            | {:sleep, pos_integer()}
```

### Example

```elixir
defmodule MyApp.Workers.Webhook do
  use Kathikon.Worker

  @impl true
  def perform(%{args: %{"url" => url, "body" => body}}) do
    case MyApp.HTTP.post(url, body) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

---

## Kathikon.Config

Runtime configuration readers. Set values in `config :kathikon, ...`.

| Function | Returns |
|----------|---------|
| `queues/0` | Keyword of queue configs |
| `queue_names/0` | `[atom()]` |
| `queue_config/1` | Keyword for one queue |
| `concurrency/1` | Max tasks for queue |
| `poll_interval/0` | Dispatcher poll ms |
| `scheduler_interval/0` | Scheduler tick ms |
| `prune_interval/0` | Pruner tick ms |
| `retention_period/0` | Terminal job retention ms |
| `max_attempts/0` | Default retry limit |
| `mnesia_copies/0` | `:ram` or `:disc` |
| `storage_backend/0` | Configured storage module |
| `scheduler/0` | Scheduler adapter module |
| `timezone/0` | IANA timezone for cron |
| `result_storage/0` | `:store` or `:discard` worker results |

```elixir
Kathikon.Config.concurrency(:emails)  # 5
```

---

## Kathikon.Storage

Facade over the configured storage backend. Used internally; embedders may call `setup/0`.

| Function | Description |
|----------|-------------|
| `setup/0` | Create Mnesia schema and tables |
| `clear_jobs!/0` | Delete all jobs (tests) |
| `reset!/0` | Drop and recreate tables (tests) |
| `insert/1`, `update/1`, `fetch/1`, `delete/1` | Job CRUD |
| `claim/2` | Atomic queue claim |
| `promote_scheduled/1` | Batch schedule promotion |
| `all/0` | List jobs |
| `backend/0`, `backend/1` | Get/set backend module |

```elixir
:ok = Kathikon.Storage.setup()
```

---

## Kathikon.Telemetry

### `attach_default_logger/0`

Attaches a logger handler for all Kathikon events.

```elixir
Kathikon.Telemetry.attach_default_logger()
```

### Events

See [Telemetry guide](../guides/telemetry-and-observability.md).

---

## Runtime processes

These modules are started by `Kathikon.Application`. You typically do not call them directly.

### Kathikon.Application

OTP application callback. Calls `Storage.setup/0` and starts the supervision tree.

### Kathikon.Queue

`DynamicSupervisor` for per-queue dispatchers.

| Function | Description |
|----------|-------------|
| `ensure_started/1` | Start dispatcher for queue |
| `start_configured/0` | Start all configured queues |

### Kathikon.Dispatcher

`GenServer` — polls Mnesia, claims jobs, spawns `Task` workers.

Registered as `{:dispatcher, queue}` in `Kathikon.Registry`.

`start_link/1` options: `:queue`, `:config`, `:poll_interval`, `:storage` (defaults to `Kathikon.Storage`).

### Kathikon.Scheduler.Promoter

`GenServer` — ticks on `scheduler_interval`, calls `storage.promote_scheduled/1`.

Registered as `Kathikon.Scheduler.Promoter` when started by the application.

`start_link/1` options: `:interval`, `:storage`, `:name` (`false` for unnamed test instances).

### Kathikon.Scheduler

Facade for scheduling APIs (`schedule/3`, `schedule_once/3`, etc.). Not a registered process.

### Kathikon.Pruner

`GenServer` — deletes terminal jobs past `retention_period`.

Registered as `Kathikon.Pruner` when started by the application.

`start_link/1` options: `:interval`, `:storage`, `:name` (`false` for unnamed test instances).

### Kathikon.Batch

Fan-out/fan-in workflows. Parent job enters `:waiting_for_children`; children carry `batch_id` and `parent_job_id` metadata.

```elixir
{:ok, batch} = Kathikon.Batch.start(parent_id, [
  {ChildWorker, %{"id" => 1}, [queue: :default]}
], on_complete: {ReportWorker, %{}})
```

### Kathikon.Report

Queue and failure summaries for dashboards and ops.

```elixir
{:ok, summaries} = Kathikon.Report.queue_summary()
Kathikon.Report.failure_summary()
```

### Kathikon.Dashboard

Operations facade for CLIs, LiveView, and RPC. See [Management API](../management_api.md) and [dashboard spec](../dashboard_spec.md).

```elixir
{:ok, queues} = Kathikon.Dashboard.queue_summary()
{:ok, page} = Kathikon.Dashboard.list_jobs(queue: :default, tab: :completed, limit: 50)
{:ok, detail} = Kathikon.Dashboard.fetch_job(job_id)
:ok = Kathikon.Dashboard.pause_all()
```

CLI: `mix kathikon.ops summary`

### Kathikon.LiveDashboard.Page

Optional LiveDashboard tab. Compiled only when `phoenix_live_dashboard` is available. Pause, kill, and retry require `allow_destructive_actions: true`. See [LiveDashboard in a Phoenix app](../guides/live-dashboard.md).

```elixir
live_dashboard "/dashboard",
  additional_pages: [kathikon: Kathikon.LiveDashboard.Page],
  allow_destructive_actions: true
```

Demo: [LiveDashboard livebook](../../livebooks/live_dashboard.livemd) or `iex examples/live_dashboard_ops.exs`.

### Kathikon.Cron

Recurring cron schedules. Each match enqueues a durable job.

```elixir
Kathikon.Cron.insert(worker, args, cron: "0 9 * * *", queue: :email)
Kathikon.Cron.update(schedule_id, cron: "0 10 * * *")
Kathikon.Cron.fetch(schedule_id)
Kathikon.Cron.list()
Kathikon.Cron.cancel(schedule_id)
Kathikon.Cron.valid?(expression)
```

---

## Placeholders (not implemented)

### Kathikon.Lifeline

```elixir
Kathikon.Lifeline.start_link()
# {:error, :not_implemented}  — Phase 2
```

---

## Internal (extension points)

| Module | Role |
|--------|------|
| `Kathikon.Storage` | Storage behaviour and facade |
| `Kathikon.Storage.Mnesia` | Default Mnesia implementation |

Configure via `storage_backend: Kathikon.Storage.Mnesia`.

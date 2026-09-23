# Kathikon

[![CI](https://github.com/thanos/kathikon/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/kathikon/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/thanos/kathikon/badge.svg?branch=main)](https://coveralls.io/github/thanos/kathikon?branch=main)
[![Hex version](https://img.shields.io/hexpm/v/kathikon.svg)](https://hex.pm/packages/kathikon)
[![Hex Docs](https://img.shields.io/badge/hex-docs-8A33A3.svg)](https://hexdocs.pm/kathikon)

**Kathikon** (Greek: καθήκον - duty, obligation) is a BEAM-native durable job queue and task execution platform for Elixir.

Jobs are treated as durable obligations that must eventually be fulfilled: completed, retried, cancelled, or discarded - but never silently lost.

Kathikon uses **Mnesia** as its coordination store and **OTP** as its execution substrate. No PostgreSQL, Redis, RabbitMQ, or external brokers are required.

## Status

**v0.3.0 - LiveDashboard**

- `Kathikon.LiveDashboard.Page` — Kathikon tab in Phoenix LiveDashboard
- Pause, resume, cancel, filter, search, retry, and paginated job lists
- Phoenix setup guide, Playground demo, and Livebook

![Kathikon LiveDashboard](docs/screenshots/dashboard-screenshot.png)

**v0.2.1 - Operations tooling**

- `Kathikon.Dashboard` - inspect and control facade for UIs and RPC
- `mix kathikon.ops` - terminal CLI (`summary`, `jobs`, pause/resume, retry, purge, …)
- Remote ops over Erlang distribution (`Kathikon.Dashboard.RPC`)

**v0.2.0 - Control, scheduling, batches, and correctness**

- Storage behaviour with atomic lifecycle operations
- Formal job state machine and durable history
- Dead-letter queue with rerun semantics
- `Kathikon.schedule/3` and scheduler adapters (built-in + optional Quantum)
- Management and reporting APIs
- Parent/child batch workflows

Previous: **v0.1.0 - Phase 1: Durable Job Queue**

## Installation

```elixir
def deps do
  [
    {:kathikon, "~> 0.3.0"}
  ]
end
```

The [Quick start](docs/guides/quick-start.md) guide covers configuration, a worker, and the first job (v0.3.0). A short version:

Define a worker:

```elixir
defmodule MyApp.EmailWorker do
  use Kathikon.Worker

  @impl true
  def perform(%Kathikon.Job{args: %{"email" => email}}) do
    MyApp.Mailer.deliver(email)
    :ok
  end
end
```

Configure queues:

```elixir
# config/config.exs
config :kathikon,
  queues: [
    default: [concurrency: 10],
    emails: [concurrency: 5]
  ]
```

Enqueue jobs:

```elixir
{:ok, job} = Kathikon.insert(MyApp.EmailWorker, %{"email" => "user@example.com"})

{:ok, job} =
  Kathikon.insert(MyApp.EmailWorker, %{"email" => "user@example.com"},
    queue: :emails,
    priority: 5,
    schedule_in: 60,
    max_attempts: 10
  )
```

Cancel a pending job:

```elixir
{:ok, job} = Kathikon.cancel(job_id)
```

## Architecture

```
Kathikon.Supervisor
├── Registry
├── Kathikon.Queue (DynamicSupervisor)
│   └── Kathikon.Dispatcher (one per queue)
├── Kathikon.Scheduler.Promoter
└── Kathikon.Pruner
```

| Module | Role |
|--------|------|
| `Kathikon.Job` | Job struct and state machine |
| `Kathikon.Worker` | Worker behaviour (`perform/1`) |
| `Kathikon.Storage` | Storage behaviour (default: `Kathikon.Storage.Mnesia`) |
| `Kathikon.Dashboard` | Ops facade - queue summary, job lists, bulk control |
| `Kathikon.Report` | Queue/job reporting helpers |
| `Kathikon.Batch` | Fan-out/fan-in batch workflows |
| `Kathikon.Scheduler.Promoter` | Promotes scheduled jobs to available |
| `Kathikon.Pruner` | Enforces retention on terminal jobs |
| `Kathikon.Telemetry` | `[:kathikon, ...]` telemetry events |

## Job states

```
scheduled → available → claimed → running → completed
                    ↘           ↘ retryable → available
                      cancelled   failed → dead
```

## Telemetry

Kathikon emits standard telemetry events. See [Telemetry guide](docs/guides/telemetry-and-observability.md) for the full list. Highlights:

- Job: `[:kathikon, :job, :inserted]`, `:claimed`, `:started`, `:completed`, `:failed`, `:sleep`, `:retry`, `:discard`, `:cancel`, `:dead`, `:prune`
- Runtime: `[:kathikon, :scheduler, :tick]`, `[:kathikon, :scheduler, :fired]`, `[:kathikon, :pruner, :tick]`, `[:kathikon, :dispatcher, :poll]`

Attach the default logger in development:

```elixir
Kathikon.Telemetry.attach_default_logger()
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `:queues` | `[default: [concurrency: 10]]` | Queue names and concurrency |
| `:poll_interval` | `200` | Dispatcher poll interval (ms) |
| `:scheduler_interval` | `1000` | Scheduler tick interval (ms) |
| `:prune_interval` | `60000` | Pruner tick interval (ms) |
| `:retention_period` | `7 days` | How long to keep terminal jobs (ms) |
| `:max_attempts` | `20` | Default retry limit |
| `:storage_backend` | `Kathikon.Storage.Mnesia` | Storage implementation |
| `:mnesia_copies` | `:auto` | Mnesia storage: `:ram`, `:disc`, or `:auto` (`ram` on `nonode@nohost` and Livebook nodes) |

## Examples

Runnable scripts under `examples/` (use `mix run examples/<name>.exs`):

| Script | Demonstrates |
|--------|----------------|
| [basic_worker.exs](examples/basic_worker.exs) | Insert and poll status |
| [scheduled_job.exs](examples/scheduled_job.exs) | `schedule` with `:at` / `:in` |
| [dead_letter_retry.exs](examples/dead_letter_retry.exs) | Failures, dead letter, rerun |
| [batch_fanout_fanin.exs](examples/batch_fanout_fanin.exs) | Parent/child batches |
| [reporting.exs](examples/reporting.exs) | `Kathikon.Report` summaries |
| [quantum_scheduler_adapter.exs](examples/quantum_scheduler_adapter.exs) | Optional Quantum scheduler |

## Operations CLI

Inspect and control a running node from the terminal:

```bash
mix kathikon.ops summary
mix kathikon.ops jobs --queue default --tab completed --limit 20
mix kathikon.ops pause --all
```

Remote (named node + matching cookie on both sides):

```bash
elixir --name ops@127.0.0.1 --cookie SECRET -S mix kathikon.ops --node kathikon@127.0.0.1 summary
```

See [Management API](docs/management_api.md) and `Kathikon.Dashboard` docs.

## Roadmap

| Release | Focus |
|---------|--------|
| v0.1.0 | Durable job queue (done) |
| v0.2.0 | Control, scheduling, batches, and correctness (done) |
| v0.2.1 | Operations tooling: Dashboard facade, CLI, and RPC (done) |
| v0.3.0 | LiveDashboard page for a Phoenix app (done) |
| v0.4.0 | Workflows and DAGs |
| v0.5.0 | Ecto storage backend (PostgreSQL) |
| v0.6.0 | MongoDB storage backend |
| v0.7.0 | SQS backend, for jobs that should leave the BEAM |
| v0.8.0 | Distributed coordination: leases, lifeline, worker ownership |
| v0.9.0 | Uniqueness, dynamic queues, and rate limits |
| v1.0.0 | Stable public API and storage contract, plus a full operator LiveView |

## Documentation

Generate HTML docs with [ExDoc](https://github.com/elixir-lang/ex_doc):

```bash
mix docs
open doc/index.html
```

- **[Documentation index](docs/documentation.md)** - guides and module reference (source)
- [CHANGELOG](CHANGELOG.md) - release history
- [Quick start](docs/guides/quick-start.md)
- [LiveDashboard in a Phoenix app](docs/guides/live-dashboard.md)
- [Module reference](docs/reference/modules.md)
- [Configuration](docs/guides/configuration.md)
- [Interactive demo (Livebook)](livebooks/kathikon_demo.livemd)


## License

MIT. See [LICENSE](LICENSE).

# Kathikon documentation

Kathikon (καθήκον — duty, obligation) is a BEAM-native durable job queue for Elixir. Jobs are persisted in Mnesia, executed by OTP supervisors and dispatchers, and tracked through an explicit state machine.

**Current release:** v0.3.0 — LiveDashboard page (`Kathikon.LiveDashboard.Page`)

Previous: **v0.2.1** — operations tooling (`Kathikon.Dashboard`, `mix kathikon.ops`). **v0.2.0** — control, scheduling, batches, and correctness.

## Guides

| Guide | What you will learn |
|-------|---------------------|
| [Quick start](guides/quick-start.md) | Install, configure, define a worker, enqueue your first job |
| [Workers](guides/workers.md) | The `Kathikon.Worker` behaviour, return values, errors |
| [Queues & concurrency](guides/queues-and-concurrency.md) | Multiple queues, dispatcher concurrency, isolation |
| [Scheduling](guides/scheduling.md) | `schedule_in`, `schedule_at`, cron, scheduler promotion |
| [Scheduling reference](scheduling.md) | One-time and recurring schedules, timezone |
| [Retries & errors](guides/retries-and-errors.md) | Backoff, `max_attempts`, dead-letter, error recording |
| [Cancellation](guides/cancellation.md) | When jobs can be cancelled, API usage |
| [Storage](storage.md) | Storage behaviour, Mnesia tables, lifecycle |
| [Storage & embedding](guides/storage-and-embedding.md) | Mnesia setup, Livebook, tests, backends |
| [Job lifecycle](job_lifecycle.md) | State machine and history events |
| [Batches](batches.md) | Fan-out/fan-in parent/child workflows |
| [Management API](management_api.md) | Claim, retry, dead-letter, queue control, ops CLI |
| [LiveDashboard](guides/live-dashboard.md) | Add the Kathikon tab to a Phoenix LiveDashboard |
| [Dashboard spec](dashboard_spec.md) | Operator UI layout, state tabs, and API mapping |
| [Reporting](reporting.md) | Queue and failure summaries |
| [Telemetry](guides/telemetry-and-observability.md) | Events, measurements, metadata, custom handlers |
| [Configuration](guides/configuration.md) | All `config :kathikon` keys and environments |
| [Quantum adapter](quantum_adapter.md) | Optional Quantum scheduler integration |
| [Architecture](architecture.md) | Supervision tree and runtime components |

## Reference

| Document | Contents |
|----------|----------|
| [Module reference](reference/modules.md) | Every module and public function with examples |
| [Interactive demo](../livebooks/kathikon_demo.livemd) | Livebook walkthrough |
| [LiveDashboard](../livebooks/live_dashboard.livemd) | Control panel plus queue health page |

## Examples

Runnable scripts in `examples/` at the project root — run with `mix run examples/<name>.exs`:

| Script | Demonstrates |
|--------|----------------|
| `examples/basic_worker.exs` | Insert and poll status |
| `examples/scheduled_job.exs` | `schedule` with `:at` / `:in` |
| `examples/dead_letter_retry.exs` | Failures, dead letter, rerun |
| `examples/batch_fanout_fanin.exs` | Parent/child batches |
| `examples/reporting.exs` | `Kathikon.Report` summaries |
| `examples/live_dashboard_ops.exs` | Playground control panel and LiveDashboard |
| `examples/quantum_scheduler_adapter.exs` | Optional Quantum scheduler |

## Architecture at a glance

```
Kathikon.Supervisor
├── Registry
├── Kathikon.Queue          (DynamicSupervisor → one Dispatcher per queue)
├── Kathikon.QueueControl
├── Kathikon.Scheduler.Promoter   (promotes :scheduled → :available)
├── Kathikon.Scheduler.BuiltIn.Tick   (cron evaluation, optional)
└── Kathikon.Pruner         (deletes terminal jobs after retention)
```

Public API: `Kathikon.insert/3`, `Kathikon.schedule/3`, `Kathikon.cancel/1`, `Kathikon.fetch/1`, `Kathikon.status/1`, `Kathikon.history/1`, `Kathikon.all/0`, `Kathikon.start_queue/1`, `Kathikon.claim/2`, `Kathikon.retry/2`, `Kathikon.dead_jobs/1`, `Kathikon.Dashboard.*`, `mix kathikon.ops`.

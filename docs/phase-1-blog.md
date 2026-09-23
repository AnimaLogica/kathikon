# Building a BEAM-Native Job Queue: Introducing Kathikon Phase 1

> **Historical:** This article describes Kathikon v0.1.0 (Phase 1). Current release is v0.3.0 — see [documentation index](documentation.md).

*How we designed a durable obligation system using OTP, Mnesia, and explicit state machines — without PostgreSQL or Redis.*

---

Most Elixir teams reach for Oban when they need background jobs. Oban is excellent — battle-tested, PostgreSQL-backed, and deeply integrated with Ecto. But it carries an operational dependency: you need a database that is always on, properly sized, and reachable from every node.

What if the job queue lived entirely on the BEAM?

That question led to **Kathikon** (Greek: καθήκον — duty, obligation). Kathikon is a durable job execution platform that treats work as obligations the system must eventually resolve. Phase 1, released as v0.1.0, delivers the core engine: insert, execute, retry, schedule, prioritize, observe, and prune.

This article explains what we built, why we built it that way, and what trade-offs we accepted.

## The obligation model

A job queue is not a message broker. It is an **obligation ledger**.

When your application calls `Kathikon.insert/3`, it is saying: "I owe the system this unit of work." The system must eventually report one of four outcomes:

1. **Completed** — work succeeded
2. **Retried** — work failed but will be attempted again
3. **Cancelled** — work was explicitly withdrawn
4. **Discarded** — work failed permanently after exhausting retries

What must never happen is silent loss: a job that disappears without a recorded terminal state.

This framing shapes every architectural decision downstream.

## Why not clone Oban?

Kathikon is intentionally **not** an Oban clone. Oban's strength is SQL-backed durability, rich querying, and a mature ecosystem. Kathikon's strength is **BEAM-native coordination**:

- No PostgreSQL, Redis, RabbitMQ, or Kafka
- Mnesia as the active coordination store
- OTP supervision as the execution substrate
- Distributed semantics as a first-class future concern (Phase 2+)

The goal is operational simplicity for teams already running distributed Erlang/Elixir clusters who want job execution without adding infrastructure.

## Architecture overview

Phase 1 is a supervised library application:

```
Kathikon.Supervisor
├── Registry
├── Queue (DynamicSupervisor)
│   └── Dispatcher (one per queue)
├── Scheduler
└── Pruner
```

### Public API

```elixir
defmodule MyApp.EmailWorker do
  use Kathikon.Worker

  @impl true
  def perform(%Kathikon.Job{args: %{"email" => email}}) do
    MyApp.Mailer.deliver(email)
    :ok
  end
end

{:ok, job} = Kathikon.insert(MyApp.EmailWorker, %{"email" => "a@b.com"},
  queue: :emails,
  priority: 5,
  schedule_in: 60
)
```

### Job state machine

Jobs move through explicit states:

```
scheduled → available → executing → completed
                    ↘           ↘ retryable → ...
                      cancelled   discarded
```

There are no implicit states. Every transition is recorded in Mnesia and emitted as a telemetry event.

## Mnesia as coordination store

Mnesia is often misunderstood. It is not a general-purpose database for analytics. It is a **real-time, transactional, distributed key-value store** built into OTP.

For Kathikon Phase 1, Mnesia stores:

- Job records (`:kathikon_jobs`)
- Queue metadata (`:kathikon_queues`)

Jobs are serialized with `:erlang.term_to_binary/1` and stored in an `ordered_set` table keyed by job ID. This keeps the storage layer stable while the job struct evolves.

### Atomic claims

The critical operation is **claim**:

```elixir
:mnesia.transaction(fn ->
  job = highest_priority_claimable_job(queue, now)
  :mnesia.write(%{job | state: :executing})
end)
```

Within a single node, this prevents double execution. Two dispatchers cannot claim the same job because Mnesia serializes conflicting writes.

Cross-node double execution is a Phase 2 problem. Leases and lifeline recovery will extend the claim model to clusters.

### Coordination, not history

Mnesia is not an infinite archive. Terminal jobs (`:completed`, `:cancelled`, `:discarded`) are pruned after a configurable retention window. Long-term analytics belong in your observability stack via telemetry, not in the queue's storage engine.

This is the same principle behind Sidekiq's Redis TTLs and SQS message deletion: the queue stores **active obligations**, not **business history**.

## OTP execution model

### Dispatchers as control plane

Each queue has a `Kathikon.Dispatcher` GenServer that:

1. Polls Mnesia for claimable jobs
2. Respects a concurrency limit
3. Spawns `Task` processes to run `perform/1`
4. Records the outcome and updates job state

The dispatcher is the **control plane**. Worker tasks are the **data plane**. A slow job does not block the dispatcher's ability to manage other work — only the concurrency slot is held.

### Scheduler batch promotion

Scheduled jobs (`schedule_in`, `schedule_at`) begin in `:scheduled`. The scheduler promotes them to `:available`.

An early bug taught us something important: promoting jobs one-at-a-time creates a race. If a low-priority job is promoted before a high-priority sibling is promoted, the dispatcher can claim and execute it first.

The fix: promote all due jobs in a **single Mnesia transaction**:

```elixir
def promote_scheduled(now) do
  :mnesia.transaction(fn ->
    for job <- due_jobs(now) do
      :mnesia.write(%{job | state: :available, available_at: now})
    end
  end)
end
```

This is a small example of how distributed systems bugs hide in ordering assumptions.

### Retries and backoff

Failed jobs increment `attempts` and transition to `:retryable` with exponential backoff:

```
backoff = min(attempt² × 5 seconds, 24 hours)
```

After `max_attempts`, the job is `:discarded` with a full error history attached to the job struct.

## Observability by default

Kathikon emits telemetry events for every lifecycle transition:

- `[:kathikon, :job, :insert]`
- `[:kathikon, :job, :start]`
- `[:kathikon, :job, :stop]`
- `[:kathikon, :job, :retry]`
- `[:kathikon, :job, :discard]`

Attach a handler once:

```elixir
:telemetry.attach_many("kathikon-metrics", events, &MyApp.Metrics.handle/4, nil)
```

The queue engine does not know about Prometheus, Datadog, or OpenTelemetry. It speaks telemetry. Your application decides where metrics go.

## Trade-offs we accepted in Phase 1

Honesty about limitations builds trust.

| Limitation | Phase 1 behavior | Future phase |
|------------|-------------------|--------------|
| Orphaned `:executing` jobs | Manual recovery | Phase 2 lifeline |
| O(n) claim scan | Full table scan | Secondary indexes |
| Single-node claim safety | Mnesia transaction | Distributed leases |
| No cron | Stub module | Phase 3 |
| No uniqueness | Duplicates allowed | Phase 3 |
| `nonode@nohost` durability | RAM only | Named nodes + disc |

We document these explicitly so operators can make informed decisions.

## What we learned

### 1. Config loading matters

A subtle test failure — priority jobs running in wrong order — traced back to `config/test.exs` not being imported. The priority queue dispatcher started with default concurrency 10 instead of 1, causing parallel execution that masked priority ordering.

Always use `import_config "#{config_env()}.exs"` in `config/config.exs`.

### 2. Supervision changes test semantics

Stopping a dispatcher GenServer does not stop it permanently under a `DynamicSupervisor` — it restarts. Tests that need a fresh dispatcher must use `DynamicSupervisor.terminate_child/2`.

### 3. Mnesia API details matter

`:mnesia.create_table` returns `{:atomic, :ok}`, not `:ok`. `persistence: true` is not a valid option — use `disc_copies` or `ram_copies`. On `nonode@nohost`, only `ram_copies` works.

These are the kinds of details that separate a demo from a library.

## Getting started

Add Kathikon to your `mix.exs`:

```elixir
{:kathikon, path: "../kathikon"}
```

Configure queues:

```elixir
config :kathikon,
  queues: [default: [concurrency: 10]]
```

Define a worker, insert a job, and watch telemetry.

For production on a named node:

```bash
elixir --name kathikon@host --cookie $COOKIE -S mix run --no-halt
```

Jobs will use `disc_copies` and survive BEAM restarts.

## What comes next

Phase 1 is the foundation. The roadmap:

- **Phase 2:** Distributed leases, worker ownership, lifeline recovery
- **Phase 3:** Cron, uniqueness, dynamic queues
- **Phase 4:** Rate limits, pause/resume
- **Phase 5–7:** Batches, workflows, DAGs
- **Phase 8:** Optional LiveView dashboard (consumer of APIs only)

Workflow orchestration will emerge from job primitives — not the other way around.

## Conclusion

Kathikon Phase 1 proves that a durable, observable job queue can live entirely on the BEAM. Mnesia provides transactional coordination. OTP provides supervision and execution. Telemetry provides observability.

It is not a replacement for Oban in every context. It is an alternative for teams who want job execution without external brokers — and an educational journey through the systems that make the BEAM unique.

The obligation model is simple: work must be accounted for. Everything else follows from that.

---

*Kathikon is open source. Read the code, run the tests, and follow the implementation plans in `plans/` and `docs/`.*

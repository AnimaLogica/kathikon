# Control, Scheduling, and Batches

A durable job queue treats every unit of work as an obligation that must eventually reach a known terminal state. That sounds simple until you run jobs concurrently on the BEAM.

## Why job state is harder than it looks

A job is not just "pending" or "done". Retries, scheduling, cancellation, dead-letter queues, and parent/child batches each need explicit states. Without a state machine, bugs show up as silent data corruption.

## Read-then-update races

```elixir
# Unsafe across concurrent callers
job = fetch(id)
update(id, %{state: :running})
```

Two dispatchers can both read `:available` and both believe they own the job. Kathikon v0.2.0 uses atomic storage callbacks inside Mnesia transactions.

## Why storage behaviours matter

Mnesia is the default for BEAM-native durability, but the public API goes through `Kathikon.Storage`. Tests and future adapters can swap backends without rewriting dispatchers.

## Scheduling vs execution

`Kathikon.schedule/3` decides **when** a job becomes durable work. Dispatchers decide **how** workers run. Optional Quantum integration fires schedules; Kathikon still owns storage, retries, and history.

## Batches: the missing primitive

Fan-out/fan-in appears in almost every production system. Kathikon persists `:waiting_for_children` instead of blocking a process. When children finish, an explicit continuation job runs — no magic resume of a stale process.

## History enables operations

`Kathikon.history/1` records claims, failures, dead-letter moves, reruns, and batch events. That audit trail supports debugging today and dashboards tomorrow.

## Try it

```elixir
{:ok, job} = Kathikon.insert(MyWorker, %{"id" => "1"})
{:ok, events} = Kathikon.history(job.id)
Kathikon.Report.queue_summary()
```

See the guides under `docs/` for full API reference.

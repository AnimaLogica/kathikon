# Dashboard product spec

Reference for building operator UIs on top of `Kathikon.Dashboard`: Phoenix LiveView,
`mix kathikon.ops`, a TUI, or remote RPC consumers.

## Layout

1. **Global commands** (top bar): pause all, resume all, bulk cancel, bulk retry/rerun, purge
2. **Queue summary** — one row per queue with aggregated counts and pause state
3. **Jobs panel** — state tabs, paginated job table, per-job drill-down

The UX target is similar to Oban Web: queue summary on top, filterable job list below,
bulk actions, row actions, and job detail on ID click.

## State model

From `Kathikon.Job.StateMachine` and `docs/job_lifecycle.md`:

| State | Meaning |
|-------|---------|
| `:scheduled` | Waiting until `scheduled_at` |
| `:available` | Ready to claim |
| `:claimed` | Atomically claimed, not yet running |
| `:running` | Worker executing |
| `:waiting_for_children` | Batch parent waiting on children |
| `:retryable` | Failed, waiting for backoff retry |
| `:completed` | Success |
| `:failed` | Terminal failure (may move to `:dead` or `:discarded`) |
| `:dead` | Dead-letter queue |
| `:cancelled` | Cancelled before completion |
| `:discarded` | Permanently discarded |

Legacy `:executing` normalizes to `:running`.

## Oban mapping

| Oban UI | Kathikon states | Dashboard tab |
|---------|-----------------|---------------|
| Available | `:scheduled`, `:available` | `:available` |
| Executing | `:claimed`, `:running`, `:waiting_for_children` | `:executing` |
| Retryable | `:retryable` | `:retryable` |
| Completed | `:completed` | `:completed` |
| Cancelled | `:cancelled` | `:cancelled` |
| Discarded | `:discarded` only | `:discarded` |
| *(no Oban tab)* | `:failed`, `:dead` | `:dead` |

Kathikon keeps `:dead` as an actionable DLQ (`rerun`, `retry`, `discard`) distinct from
`:discarded` (terminal, purge only). Tab mapping is implemented by
`Kathikon.Dashboard.states_for_tab/1`.

## Queue summary columns

UI columns use `ui_counts` on each `queue_summary/1` row. Raw per-state counts remain in
`counts` for drill-down.

| UI column | States summed |
|-----------|---------------|
| Available | `:scheduled` + `:available` |
| Executing | `:claimed` + `:running` + `:waiting_for_children` |
| Completed | `:completed` |
| Retryable | `:retryable` |
| Cancelled | `:cancelled` |
| Failed | `:failed` + `:dead` |
| Discarded | `:discarded` |
| Total | all states |
| Paused | `queue_status/1` → `paused: true/false` |

## Global commands

| Button | API | Notes |
|--------|-----|-------|
| Pause all | `Dashboard.pause_all/0` | Pauses configured and storage-seen queues |
| Resume all | `Dashboard.resume_all/0` | |
| Kill all | `Dashboard.cancel_jobs/1` | Cancellable only; skips `:running` and `:waiting_for_children` |
| Rerun all | `Dashboard.rerun_jobs/1` | Default states `[:dead, :failed]` |
| Retry | `Dashboard.retry_jobs/1` | Default states `[:retryable, :failed, :dead]` |
| Purge | `Dashboard.purge_jobs/1` or `Dashboard.prune_now/0` | Immediate delete vs retention pruner |
| Promote | `Dashboard.promote_now/0` | Scheduler tick `:scheduled` → `:available` |

`cancel` stops pending work. `discard` moves jobs to `:discarded`. There is no force-kill
of a BEAM task mid-`perform/1` in v0.2.

## Per-job actions

Use `Kathikon.Dashboard.actions_for_state/1` to enable or disable buttons.

| State | cancel | retry | rerun | discard | purge |
|-------|--------|-------|-------|---------|-------|
| `:scheduled` | yes | | | yes | |
| `:available` | yes | | | yes | |
| `:claimed` | yes | | | yes | |
| `:running` | | | | | |
| `:waiting_for_children` | | | | | |
| `:retryable` | yes | yes | | yes | |
| `:failed` | | yes | yes | yes | |
| `:dead` | | yes | yes | yes | |
| `:completed` | | | yes | | yes |
| `:cancelled` | | | | | yes |
| `:discarded` | | | | | yes |

Bulk equivalents: `cancel_jobs/1`, `retry_jobs/1`, `rerun_jobs/1`, `discard_jobs/1`,
`purge_jobs/1`.

## Jobs panel

Tabs: `Dashboard.state_tabs/0`

```elixir
Dashboard.list_jobs(queue: :default, tab: :dead, limit: 50, offset: 0)
```

Footer: `Showing at most N jobs out of M` — always paginate.

Table columns: id, state, queue, worker, attempts (`attempts/max_attempts`), timestamp,
errors, actions (from `actions_for_state/1`).

Job detail: `Dashboard.fetch_job/1` → `job` map + `history` list; show `parent_job_id`,
`batch_id`, `rerun_of` when set.

## Library API

### Read

```elixir
{:ok, queues} = Kathikon.Dashboard.queue_summary()
# each row: %{queue, paused, counts, ui_counts, executing, failed, total}

{:ok, %{jobs: jobs, total: total, limit: 50, offset: 0}} =
  Kathikon.Dashboard.list_jobs(queue: :default, tab: :completed)

{:ok, %{job: job, history: events}} = Kathikon.Dashboard.fetch_job(job_id)

Kathikon.Dashboard.actions_for_state(:dead)
#=> [:retry, :rerun, :discard]
```

`queue_summary/1` builds on `Kathikon.Report.queue_summary/1` and adds dashboard fields
plus queues seen in storage.

### Write

```elixir
Kathikon.Dashboard.pause_all()
Kathikon.Dashboard.resume_all()
Kathikon.Dashboard.pause_queue(:emails)
Kathikon.Dashboard.cancel_job(id)
Kathikon.Dashboard.retry_job(id)
Kathikon.Dashboard.rerun_job(id)
Kathikon.Dashboard.discard_job(id)

Kathikon.Dashboard.cancel_jobs(queue: :default)
Kathikon.Dashboard.retry_jobs(queue: :default)
Kathikon.Dashboard.rerun_jobs(queue: :default, states: [:dead])
Kathikon.Dashboard.discard_jobs(queue: :default)
Kathikon.Dashboard.purge_jobs(queue: :default, states: [:completed])
Kathikon.Dashboard.promote_now()
Kathikon.Dashboard.prune_now()
```

### RPC

```elixir
# Returns the remote Dashboard result directly (not double-wrapped).
{:ok, queues} = Kathikon.Dashboard.RPC.call(:"kathikon@host", :queue_summary, [[]])
:ok = Kathikon.Dashboard.RPC.call(:"kathikon@host", :pause_all, [])
```

Whitelist only — see `Kathikon.Dashboard.RPC.allowed?/1`.

CLI remote (named local node and matching cookie required):

```bash
elixir --name ops@127.0.0.1 --cookie SECRET -S mix kathikon.ops --node kathikon@127.0.0.1 summary
```

## Validation

```bash
mix kathikon.ops summary
mix kathikon.ops jobs --queue default --tab completed --limit 20
mix kathikon.ops show JOB_ID
mix kathikon.ops pause --all
mix kathikon.ops --node kathikon@host summary
```

## LiveDashboard page

Optional Phoenix LiveDashboard tab: `Kathikon.LiveDashboard.Page`. Register it from the host router and set `allow_destructive_actions: true` so pause, kill, and retry are enabled. See [LiveDashboard in a Phoenix app](guides/live-dashboard.md).

Demo: `examples/live_dashboard_ops.exs` and [livebooks/live_dashboard.livemd](../livebooks/live_dashboard.livemd).

## Future work

- Full operator LiveView (state tabs and bulk actions) subscribing to `[:kathikon, :job, :*]` (debounced)
- Optional TUI for SSH-only environments
- Batch status and children links in job detail when `batch_id` is set

## Non-goals (v1)

- Force-killing BEAM tasks mid-`perform/1`
- Editing job args in the UI
- Multi-cluster aggregated view
- Authn/z (host app responsibility)

## Related docs

- [management_api.md](management_api.md)
- [job_lifecycle.md](job_lifecycle.md)
- [reporting.md](reporting.md)

# Management API

Operational functions return tagged tuples for dashboard use.

```elixir
Kathikon.status(job_id)
Kathikon.cancel(job_id, reason)
Kathikon.retry(job_id, opts)
Kathikon.rerun(job_id, opts)
Kathikon.pause_queue(queue)
Kathikon.resume_queue(queue)
Kathikon.queue_status(queue)
Kathikon.children(job_id)
Kathikon.result(job_id)
Kathikon.errors(job_id)
Kathikon.dead_jobs(opts)
Kathikon.retry_dead(job_id, opts)
Kathikon.discard_dead(job_id, reason)
Kathikon.history(job_id)
```

## Rerun semantics

`Kathikon.rerun/2` creates a **new** linked job. The original record is unchanged. The new job sets `rerun_of` and `original_job_id` for audit trails.

## Dashboard facade

`Kathikon.Dashboard` extends `Kathikon.Report.queue_summary/1` with dashboard
fields (`ui_counts`, queues seen in storage) and delegates management to
`Kathikon.*` for CLIs, LiveView, and RPC:

```elixir
{:ok, queues} = Kathikon.Dashboard.queue_summary()
{:ok, page} = Kathikon.Dashboard.list_jobs(queue: :default, tab: :completed, limit: 50)
{:ok, detail} = Kathikon.Dashboard.fetch_job(job_id)
:ok = Kathikon.Dashboard.pause_all()
```

CLI: `mix kathikon.ops summary`

## LiveDashboard (optional)

Add a **Kathikon** tab to Phoenix LiveDashboard. Setup for a host app is in [LiveDashboard in a Phoenix app](guides/live-dashboard.md).

```elixir
live_dashboard "/dashboard",
  additional_pages: [kathikon: Kathikon.LiveDashboard.Page],
  allow_destructive_actions: true
```

`allow_destructive_actions: true` enables pause, kill, and retry. Without a Phoenix app, use `iex examples/live_dashboard_ops.exs` or [livebooks/live_dashboard.livemd](../livebooks/live_dashboard.livemd).

## Remote ops

`mix kathikon.ops --node kathikon@host summary` calls the node through `Kathikon.Dashboard.RPC`.

Both nodes must share the same Erlang cookie. The machine running `mix kathikon.ops` must be a **named node**, for example:

```bash
# Terminal 1 — remote Kathikon node
elixir --name kathikon@127.0.0.1 --cookie SECRET -S mix run --no-halt

# Terminal 2 — ops CLI
elixir --name ops@127.0.0.1 --cookie SECRET -S mix kathikon.ops --node kathikon@127.0.0.1 summary
```

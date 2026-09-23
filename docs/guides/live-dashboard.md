# LiveDashboard in a Phoenix app

`Kathikon.LiveDashboard.Page` adds a **Kathikon** tab to [Phoenix LiveDashboard](https://hexdocs.pm/phoenix_live_dashboard). The tab is the operator panel: pause or resume queues, cancel cancellable jobs, filter the job list, and retry a job. It calls `Kathikon.Dashboard` — it does not start its own queue.

The page module is compiled only when `phoenix_live_dashboard` is available. Without that dependency, the rest of Kathikon is unchanged.

![Kathikon tab in Phoenix LiveDashboard](screenshots/dashboard-screenshot.png)

## 1. Add the dependency

In the Phoenix app:

```elixir
def deps do
  [
    {:kathikon, "~> 0.3.0"},
    {:phoenix_live_dashboard, "~> 0.8"}
  ]
end
```

Then:

```bash
mix deps.get
mix deps.compile kathikon --force
```

The force compile matters the first time you add LiveDashboard. Kathikon defines `Kathikon.LiveDashboard.Page` only if `Phoenix.LiveDashboard.PageBuilder` is already loaded. If the module is missing, compile Kathikon again after `phoenix_live_dashboard` is fetched.

Confirm it in IEx:

```elixir
Code.ensure_loaded?(Kathikon.LiveDashboard.Page)
```

## 2. Register the page

Phoenix already imports `Phoenix.LiveDashboard.Router` in `lib/my_app_web/router.ex`. Add the Kathikon page next to the existing `live_dashboard/2` call:

```elixir
scope "/" do
  pipe_through :browser

  live_dashboard "/dashboard",
    metrics: MyAppWeb.Telemetry,
    additional_pages: [
      kathikon: Kathikon.LiveDashboard.Page
    ],
    allow_destructive_actions: true
end
```

`allow_destructive_actions: true` turns on **Pause all**, **Kill all**, per-queue **Pause** / **Resume**, and **Retry**. LiveDashboard leaves those buttons disabled when the flag is false. Keep the route behind the same authentication you use for the rest of `/dashboard`.

Open `/dashboard/kathikon`.

## What the tab shows

* **Pause all / Resume all** — every queue Kathikon knows about
* **Kill all** — cancels jobs that are still cancellable. A job already in `:running` is left alone
* **Job queue summary** — one row per queue, with a **Pause** / **Resume** button on that row
* **Filters** — available, executing, retryable, completed, plus a text search
* **Jobs** — id, state, queue, worker, attempts, timestamp, and **Retry**. Click a row to open every field. The values and the copy box can be selected and copied. The table pages after 50 rows

## Without a Phoenix app

`iex examples/live_dashboard_ops.exs` or [livebooks/live_dashboard.livemd](../../livebooks/live_dashboard.livemd) boots the same page through Phoenix Playground at <http://localhost:4000/dashboard/kathikon>.

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-09-23

### Added

- `Kathikon.LiveDashboard.Page` — optional Phoenix LiveDashboard tab for queue health and control. Compiled only when `phoenix_live_dashboard` is available.
- Operator controls on that tab: pause or resume all queues, pause or resume one queue, cancel cancellable jobs, filter and search the job list, retry a job, and page the job table after 50 rows.
- `docs/guides/live-dashboard.md` — how to register the tab in a Phoenix router, including `allow_destructive_actions: true`.
- Phoenix Playground demo at `examples/live_dashboard_ops.exs` and `livebooks/live_dashboard.livemd`.

### Documentation

- Guides sidebar is one section. The former v0.2.0 extras group is merged into Guides, with Quick start first.
- Dependency examples and the documentation index now target v0.3.0.

## [0.2.1] - 2026-06-24

### Added

- `Kathikon.Dashboard` — operations facade for queue summaries, paginated job lists, job drill-down, bulk cancel/retry/rerun/discard/purge, and `actions_for_state/1` for UIs.
- `Kathikon.Dashboard.RPC` — whitelisted remote calls over Erlang distribution.
- `mix kathikon.ops` — terminal CLI for inspect and control (`summary`, `jobs`, `show`, `pause`, `resume`, `cancel`, `retry`, `rerun`, `purge`, `prune`).
- `Storage.list_jobs_page/1` — storage-level pagination for dashboard job lists.
- Public `@doc` for `Kathikon.pause_queue/1`, `resume_queue/1`, and `queue_status/1`.
- Public `Kathikon.Report.count_by_state/1` for shared state counting.
- `docs/dashboard_spec.md` — operator UI layout, state tabs, and Dashboard API mapping.
- Expanded dashboard, ops, RPC, cron expression, and queue test coverage.

### Fixed

- `Kathikon.Dashboard.RPC.call/4` no longer double-wraps `{:ok, result}` tuples from remote nodes (fixes `mix kathikon.ops --node … summary`).
- `Dashboard.queue_summary/1` extends `Kathikon.Report` and includes queues present in storage but not in config.
- `Dashboard.pause_all/1` and `resume_all/1` operate on all known queues (storage + config).
- `Dashboard.purge_jobs/1` returns per-job delete failures in `errors`.
- Running jobs no longer expose `:discard` in `actions_for_state/1`.
- `mix kathikon.ops` validates missing job IDs, negative pagination, and unknown state tabs.

### Documentation

- Management API guide expanded with Dashboard and remote ops examples.
- Module reference and documentation index updated for v0.2.1.
- `Kathikon.Dashboard` included in ExDoc module grouping.

## [0.2.0] - 2026-06-23

First feature release after Phase 1. Storage behaviour, formal job state machine, cron scheduling, timezone support, batches, reporting, management APIs, and expanded test coverage. See the [v0.2.0 release on GitHub](https://github.com/thanos/kathikon/releases/tag/v0.2.0).

# Telemetry and observability

Kathikon emits standard [`:telemetry`](https://hexdocs.pm/telemetry) events for every significant lifecycle transition.

## Event prefix

All events start with `[:kathikon, ...]`.

## Job events

| Event | When | Measurements | Metadata |
|-------|------|--------------|----------|
| `[:kathikon, :job, :inserted]` | Job enqueued | `%{}` | `queue`, `job_id`, `worker`, `state` |
| `[:kathikon, :job, :inserted_many]` | One `insert_many/2` chunk committed | `%{count: n}` | `%{}` |
| `[:kathikon, :job, :claimed]` | Job claimed | `%{}` | `queue`, `job_id`, `worker`, `attempt` |
| `[:kathikon, :job, :started]` | `perform/1` begins | `%{}` | `queue`, `job_id`, `worker`, `attempt` |
| `[:kathikon, :job, :completed]` | Success | `%{duration: μs}` | `queue`, `job_id`, `worker`, `attempt` |
| `[:kathikon, :job, :stop]` | Success (legacy alias) | `%{duration: μs}` | `queue`, `job_id`, `worker`, `attempt`, `result: :ok` |
| `[:kathikon, :job, :sleep]` | Deferred (`{:sleep, seconds}`) | `%{duration: μs}` | `queue`, `job_id`, `worker`, `attempt` |
| `[:kathikon, :job, :retry]` | Failure, will retry | `%{duration: μs}` | `queue`, `job_id`, `reason`, `attempt` |
| `[:kathikon, :job, :retried]` | Manual or automatic retry scheduled | `%{}` | `queue`, `job_id`, `worker` |
| `[:kathikon, :job, :failed]` | Terminal failure | `%{duration: μs}` | `queue`, `job_id`, `attempt` |
| `[:kathikon, :job, :dead]` | Moved to dead-letter | `%{duration: μs}` | `queue`, `job_id`, `attempt` |
| `[:kathikon, :job, :discard]` | Worker discarded job | `%{duration: μs}` | `queue`, `job_id`, `attempt` |
| `[:kathikon, :job, :cancel]` | User cancelled | `%{}` | `queue`, `job_id` |
| `[:kathikon, :job, :prune]` | Terminal job deleted | `%{}` | `queue`, `job_id`, `state` |
| `[:kathikon, :batch, :started]` | Batch started | `%{children: n}` | `batch_id`, `parent_job_id` |
| `[:kathikon, :batch, :completed]` | Batch completed | `%{success_count: n}` | `batch_id`, `parent_job_id` |

`duration` is native time units (microseconds on most platforms).

## Runtime events

| Event | When | Measurements | Metadata |
|-------|------|--------------|----------|
| `[:kathikon, :scheduler, :tick]` | Scheduler promoted jobs | `%{promoted: count}` | `%{}` |
| `[:kathikon, :scheduler, :fired]` | Cron or schedule fired | `%{count: 1}` | `queue`, `job_id`, `worker` |
| `[:kathikon, :pruner, :tick]` | Pruner deleted jobs | `%{pruned: count}` | `%{}` |
| `[:kathikon, :dispatcher, :poll]` | Job claimed on poll | `%{count: 1}` | `queue`, `job_id` |

## Default logger

```elixir
# config/dev.exs or IEx
Kathikon.Telemetry.attach_default_logger()
```

Logs lines like:

```
[kathikon] kathikon.job.stop queue=default job=abc... %{duration: 45000} %{result: :ok, ...}
```

## Custom handler

```elixir
:telemetry.attach(
  "my-app-kathikon",
  [[:kathikon, :job, :stop], [:kathikon, :job, :discard]],
  fn event, measurements, metadata, _config ->
    MyApp.Metrics.increment("kathikon.job.#{event |> List.last()}")
    MyApp.Metrics.timing("kathikon.job.duration", measurements[:duration])
  end,
  nil
)
```

## Handler in tests

```elixir
test "emits insert telemetry" do
  ref = :telemetry_test.attach_event_handlers(self(), [[:kathikon, :job, :inserted]])

  {:ok, _} = Kathikon.insert(MyWorker, %{})

  assert_receive {:event, [:kathikon, :job, :inserted], %{}, metadata}
  assert metadata.queue == :default
end
```

## Inspection API

For debugging and tests (not a production dashboard):

```elixir
Kathikon.all()                    # all jobs in Mnesia
Kathikon.fetch(job_id)            # single job
```

Phase 6 will add structured observability APIs.

## Related

- `Kathikon.Telemetry` module
- [Module reference](../reference/modules.md#kathikontelemetry)

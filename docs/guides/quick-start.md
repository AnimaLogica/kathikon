# Quick start

Get a working job queue in your Elixir application in a few minutes.

## 1. Add the dependency

```elixir
# mix.exs
def deps do
  [
    {:kathikon, "~> 0.3.0"}
  ]
end
```

Kathikon starts automatically via `Kathikon.Application` when your app boots. It brings up Mnesia, the scheduler, pruner, and dispatchers for configured queues.

## 2. Configure queues

```elixir
# config/config.exs
config :kathikon,
  queues: [
    default: [concurrency: 10],
    emails: [concurrency: 5]
  ]
```

See [Configuration](configuration.md) for all options.

## 3. Define a worker

Workers implement `perform/1` and return `:ok` on success or `{:error, reason}` to retry:

```elixir
defmodule MyApp.Workers.SendEmail do
  use Kathikon.Worker

  @impl true
  def perform(%Kathikon.Job{args: %{"to" => to, "subject" => subject}}) do
    MyApp.Mailer.deliver(to, subject)
    :ok
  end
end
```

## 4. Enqueue a job

```elixir
{:ok, job} =
  Kathikon.insert(MyApp.Workers.SendEmail, %{
    "to" => "user@example.com",
    "subject" => "Welcome"
  })

# job.id    — unique id for tracking
# job.state — :available (ready to run immediately)
# job.queue — :default
```

The dispatcher claims the job, runs `perform/1` in a `Task`, and transitions the job to `:completed` on success.

## 5. Check job status

```elixir
{:ok, job} = Kathikon.fetch(job.id)
job.state   # :completed | :running | :retryable | :dead | ...
```

## 6. Enable logging (development)

```elixir
# config/dev.exs
Kathikon.Telemetry.attach_default_logger()
```

Or attach your own handler — see [Telemetry](telemetry-and-observability.md).

## 7. Try the Livebook demo

Open the [interactive demo](../../livebooks/kathikon_demo.livemd) for hands-on examples of scheduling, priority, retries, cancellation, and pruning.

## Next steps

- [Workers](workers.md) — error handling, exceptions, idempotency
- [Queues & concurrency](queues-and-concurrency.md) — route work to dedicated queues
- [Scheduling](scheduling.md) — delay execution with `schedule_in` / `schedule_at`

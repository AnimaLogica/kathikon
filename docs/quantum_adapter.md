# Quantum adapter

Quantum owns clock-based triggering. Kathikon owns durable work.

## Setup

```elixir
# mix.exs
{:kathikon, "~> 0.3.0"},
{:quantum, "~> 3.5"}
```

```elixir
defmodule MyApp.KathikonScheduler do
  use Quantum, otp_app: :my_app
end
```

```elixir
config :kathikon,
  scheduler: Kathikon.Scheduler.Quantum,
  quantum_scheduler: MyApp.KathikonScheduler
```

When Quantum fires, the adapter enqueues a normal Kathikon job via `Kathikon.insert/3`.

## What Quantum does not own

- Job storage, retries, history, dead-letter, batches, reporting

## Without Quantum

When Quantum is not installed, `Kathikon.Scheduler.schedule/3` with a `:cron` option returns `{:error, :quantum_not_available}` if the Quantum adapter is configured.

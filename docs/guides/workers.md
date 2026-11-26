# Workers

Workers are modules that implement `Kathikon.Worker`. The dispatcher calls `perform/1` when a job is claimed.

## Defining a worker

```elixir
defmodule MyApp.Workers.ChargeCustomer do
  use Kathikon.Worker

  @impl true
  def perform(%Kathikon.Job{args: %{"invoice_id" => id}} = job) do
  #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  #  `job` includes id, queue, attempts, priority, timestamps, errors

    case MyApp.Billing.charge(id) do
      :ok -> :ok
      {:error, :card_declined} -> {:error, :card_declined}
    end
  end
end
```

`use Kathikon.Worker` sets `@behaviour Kathikon.Worker`. You must implement `perform/1`.

## Uniqueness

Coalesce duplicate enqueues (Oban-compatible subset) with `unique:` on the worker or on `Kathikon.insert/3`:

```elixir
defmodule MyApp.Workers.MediaProcess do
  use Kathikon.Worker, unique: [period: 60, keys: [:args]]

  @impl true
  def perform(%Kathikon.Job{args: %{"media_id" => id}}) do
    MyApp.Media.process(id)
    :ok
  end
end

# Same args within 60s return the existing job
{:ok, job} = Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42})
{:ok, ^job} = Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42})

# Override or disable per insert
Kathikon.insert(MyApp.Workers.MediaProcess, args, unique: [period: 5, keys: [:args]])
Kathikon.insert(MyApp.Workers.MediaProcess, args, unique: false)
```

| Option | Meaning |
|--------|---------|
| `:period` | Seconds; conflict if a matching non-terminal job was inserted within this window |
| `:keys` | Fields to fingerprint. `[:args]` uses the full args map; `:queue` and `:worker` are also allowed |
| `:by` | A one-arity function of the built `%Kathikon.Job{}`, or a `{module, function}` pair. Whatever it returns is hashed. Cannot be combined with `:keys` |
| `:states` | States that count as a conflict (default: scheduled, available, claimed, running, retryable, waiting_for_children) |
| `:on_conflict` | `:coalesce` (default) returns `{:ok, existing}`; `:error` returns `{:error, :unique}` |

Terminal jobs (`:completed`, `:cancelled`, …) do not block a new insert. `insert_many/2` applies the same rules per spec.

### Custom keys with `:by`

When `:keys` is too coarse, say you want uniqueness on one field of the args, pass a function:

```elixir
defmodule MyApp.Workers.MediaProcess do
  use Kathikon.Worker,
    unique: [period: 60, by: &__MODULE__.unique_key/1]

  def unique_key(job), do: job.args["media_id"]

  @impl true
  def perform(%Kathikon.Job{args: %{"media_id" => id}}), do: MyApp.Media.process(id)
end

# Same media_id, different other args: still one job
Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42, "requested_at" => 1})
Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42, "requested_at" => 2})

# At insert time, an anonymous function is fine
Kathikon.insert(MyApp.Workers.MediaProcess, args,
  unique: [period: 5, by: fn job -> {job.queue, job.args["media_id"]} end]
)
```

Rules for the function:

- **Pure and deterministic.** Same job in, same term out. Do not read the clock, a database or a random source. Maps are encoded deterministically, so key order does not matter.
- **It runs once, before storage**, in the process that called `insert/3`, never inside a Mnesia transaction. If it raises, throws or exits, the insert returns `{:error, {:unique_key_failed, reason}}`. In `insert_many/2` the failing spec is reported in `errors` at its input index (or aborts the call with `on_error: :abort`), and the other specs still insert.
- **The worker module is always part of the hash**, so two workers returning the same term never collide.
- **In `use Kathikon.Worker`**, pass an external capture (`&Mod.fun/1`) or `{module, function}`. Anonymous functions cannot be stored in a module attribute.
- **Changing the function changes the fingerprints.** After a deploy that changes it, jobs enqueued under the old function stop matching new inserts until they reach a terminal state or the period ends.

## Return values

| Return | Effect |
|--------|--------|
| `:ok` | Job → `:completed` |
| `{:sleep, seconds}` | Job → `:scheduled` for `seconds`; not a failure (no `attempts` or `errors`) |
| `{:error, reason}` | Job → `:retryable` with backoff, or `:dead` if `max_attempts` exceeded |

```elixir
# Success
:ok

# Defer without failure — scheduler promotes back to :available when due
{:sleep, 60}
{:sleep, 300}

# Transient failure — will retry (increments attempts, records error)
{:error, :timeout}
{:error, %{status: 503}}

# Any term is accepted as the error reason
{:error, "payment gateway unavailable"}
```

`{:sleep, seconds}` requires a positive integer. Invalid values are treated as `{:error, {:invalid_sleep, value}}`.

## Exceptions and throws

Uncaught exceptions and throws are converted to `{:error, reason}` internally:

```elixir
def perform(_job), do: raise("boom")      # → retry as {:exception, ...}
def perform(_job), do: throw(:cancelled)  # → retry as {:throw, :cancelled}
```

Design workers to return `{:error, reason}` for expected failures; reserve exceptions for programmer errors.

## Enqueueing work for a worker

```elixir
{:ok, job} =
  Kathikon.insert(MyApp.Workers.ChargeCustomer, %{"invoice_id" => "INV-99"},
    queue: :billing,
    max_attempts: 5,
    priority: 3
  )
```

Args must be a **map** (JSON-serializable values work best for future cross-node support).

## Idempotency

Kathikon provides **at-least-once** execution. A worker may run more than once for the same job id (retries, future lease recovery). Make `perform/1` safe to repeat:

```elixir
def perform(%{args: %{"order_id" => id}}) do
  # Good: billing API keyed by order_id is idempotent
  MyApp.Billing.capture_once(id)
  :ok
end
```

## IO-bound vs CPU-bound work

- **IO-bound** (HTTP, email, DB): higher concurrency per queue is fine.
- **CPU-bound** (PDF, image processing): use a dedicated queue with low concurrency.

See [Queues & concurrency](queues-and-concurrency.md).

## Related

- `Kathikon.Worker` module — behaviour definition
- [Retries & errors](retries-and-errors.md)
- [Module reference: Kathikon.Worker](../reference/modules.md#kathikonworker)

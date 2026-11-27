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

Coalesce duplicate enqueues with `unique:` on the worker or on `Kathikon.insert/3`. A duplicate returns the existing job instead of inserting a second one.

```elixir
defmodule MyApp.Workers.MediaProcess do
  use Kathikon.Worker,
    unique: [period: 60, criteria: fn job -> job.args["media_id"] end]

  @impl true
  def perform(%Kathikon.Job{args: %{"media_id" => id}}) do
    MyApp.Media.process(id)
    :ok
  end
end

# Same media_id within 60s returns the existing job
{:ok, job} = Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42})
{:ok, ^job} = Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42})

# Override or disable per insert
Kathikon.insert(MyApp.Workers.MediaProcess, args, unique: [period: 5])
Kathikon.insert(MyApp.Workers.MediaProcess, args, unique: false)
```

| Option | Meaning |
|--------|---------|
| `:period` | Seconds, or `:infinity`. A conflict is a matching job inside this window, measured from `:timestamp` |
| `:criteria` | A function of the built `%Kathikon.Job{}`, or a `{module, function}` pair. Its return value is hashed together with the worker module. Defaults to `{queue, args}` |
| `:timestamp` | `:inserted_at` (default) or `:scheduled_at`. The anchor for `:period` |
| `:states` | `:successful` (default) or an explicit list of states that count as a conflict |
| `:on_conflict` | `:coalesce` (default) returns `{:ok, existing}`; `:error` returns `{:error, :unique}` |

The default `:successful` blocks while a job is in flight (`:scheduled`, `:available`, `:claimed`, `:running`, `:retryable`, `:waiting_for_children`) or has `:completed`. It does not block when a job has `:failed`, `:dead`, `:cancelled` or `:discarded`, so a retry can replace a job that gave up. `insert_many/2` applies the same rules per spec.

Because a coalesced insert returns a job that already exists, check `job.state` when it matters. A `:completed` job means no new work will run. Kathikon logs a warning in that case and emits `[:kathikon, :job, :coalesced]` on every coalesce.

### Choosing the criteria

`criteria:` decides what counts as "the same job". Return only the part you care about:

```elixir
defmodule MyApp.Workers.MediaProcess do
  use Kathikon.Worker,
    unique: [period: 60, criteria: &__MODULE__.unique_key/1]

  def unique_key(job), do: job.args["media_id"]

  @impl true
  def perform(%Kathikon.Job{args: %{"media_id" => id}}), do: MyApp.Media.process(id)
end

# Same media_id, different other args: still one job
Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42, "requested_at" => 1})
Kathikon.insert(MyApp.Workers.MediaProcess, %{"media_id" => 42, "requested_at" => 2})

# At insert time, an anonymous function is fine
Kathikon.insert(MyApp.Workers.MediaProcess, args,
  unique: [period: 5, criteria: fn job -> {job.queue, job.args["media_id"]} end]
)
```

Rules for the function:

- **Pure and deterministic.** Same job in, same term out. Do not read the clock, a database or a random source. Maps are encoded deterministically, so key order does not matter.
- **It runs once, before storage**, in the process that called `insert/3`, never inside a Mnesia transaction. If it raises, throws or exits, the insert returns `{:error, {:unique_key_failed, reason}}`. In `insert_many/2` the failing spec is reported in `errors` at its input index (or aborts the call with `on_error: :abort`), and the other specs still insert.
- **The worker module is always part of the hash**, so two workers returning the same term never collide.
- **In `use Kathikon.Worker`**, an anonymous function, an external capture (`&Mod.fun/1`) or `{module, function}` all work.
- **Changing the function changes the fingerprints.** After a deploy that changes it, jobs enqueued under the old function stop matching new inserts until they reach a terminal state or the period ends.

### Migrating from Oban

Oban's `keys: [:args]` has no direct equivalent here, because `:keys` is gone. Rewrite each worker's `unique:` as a criteria function:

| Oban (`Oban.Worker`) | Kathikon |
|---|---|
| `unique: [period: 60]` (default fields: worker, queue, args) | `unique: [period: 60]` (default criteria `{queue, args}`; the worker is always hashed) |
| `unique: [period: 60, keys: [:url]]` (the `url` arg, plus worker and queue) | `unique: [period: 60, criteria: fn job -> {job.queue, job.args["url"]} end]` |
| `unique: [period: 60, fields: [:args]]` | `unique: [period: 60, criteria: fn job -> job.args end]`. Kathikon always includes the worker, so two workers never coalesce here even if Oban's `fields` would allow it |
| `unique: [states: :successful]` (default) | Default `:states`, the same intent |
| `unique: [timestamp: :scheduled_at]` | `unique: [timestamp: :scheduled_at]` |

Oban's `:fields` chooses job columns and `:keys` chooses keys inside `args`. Kathikon has one `criteria:` function for both, so the same word no longer means two things. Check each worker against the table before you port it.

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

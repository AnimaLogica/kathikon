# Bulk enqueue

`Kathikon.insert_many/2` writes many independent jobs in chunks. `Kathikon.Batch` is a different feature: a parent job fans out to children and a reduce job runs after they finish. Large fan-out uses `Batch.append/2`, which writes those children through the same `insert_jobs/2` storage callback.

## Independent jobs

```elixir
{:ok, %{inserted: 2, ids: _ids, errors: []}} =
  Kathikon.insert_many([
    {MyApp.EmailWorker, %{"n" => 1}},
    {MyApp.EmailWorker, %{"n" => 2}, [priority: 5, queue: :emails]}
  ])
```

A spec is `{worker, args}`, `{worker, args, opts}`, or a map with `:worker` and `:args`. Options match `Kathikon.insert/3` (`:queue`, `:priority`, `:max_attempts`, `:id`).

Each distinct queue is started once for the whole call. Jobs are persisted in chunks (default 500, override with `chunk_size:` or `config :kathikon, insert_many_chunk_size: 500`). One Mnesia transaction covers one chunk, so a multi-million enqueue does not hold a single transaction open.

The result is partial success:

* `inserted` — rows that committed
* `ids` — those job ids, in input order, skipping failures
* `errors` — `{index, reason}` into the original enumerable

`:on_error` defaults to `:continue`. `:abort` rolls back the current chunk and stops. Chunks that already committed stay written.

## History and telemetry

Enqueue does not write an `:inserted` history row. The job record already has `inserted_at` and its state. Pass `history: true` to record that audit event. Later transitions such as claim and completion still write history. A backend that falls back to `insert/1` still writes the `:inserted` row, because that is what single insert does.

`[:kathikon, :job, :inserted_many]` fires once per chunk with `%{count: n}`. Per-job `[:kathikon, :job, :inserted]` events are off unless you pass `telemetry: :per_job`.

## Map, then reduce

A crawler should not build one list of every link. Open a batch, append each page of links, then close:

```elixir
{:ok, batch} = Kathikon.Batch.open(parent.id, on_complete: {MyApp.ReduceWorker, %{}})

# each page of links discovered while the parent is still running
{:ok, _} = Kathikon.Batch.append(batch.batch_id, link_specs, chunk_size: 500)

:ok = Kathikon.Batch.close(batch.batch_id)
```

`open/2` writes the batch as `:loading` and leaves the parent `:running`. `append/2` stores children with `parent_job_id` and `batch_id`. `expected_count` grows only by rows that committed. `close/1` moves the parent to `:waiting_for_children` and the batch to `:running`. The reduce job waits until finished children equal that sealed count. `append/2` after `close/1` returns `{:error, :closed}`.

`Batch.start/3` is `open` + one `append` + `close` for the small case. A crawler that passes millions of specs to `start/3` still builds that list in memory first.

If an append chunk fails, the parent stays `:running` and the batch stays `:loading`. Children already written keep their `batch_id` and count toward `expected_count`. Append the failed slice again, or close and accept the children that landed.

## Benchmark

`examples/benchmark_bulk_enqueue.exs` times `insert/3` against `insert_many/2` on a paused queue, so the dispatcher does not claim while the writes are measured. Pass job counts after `--`:

```bash
mix run examples/benchmark_bulk_enqueue.exs
mix run examples/benchmark_bulk_enqueue.exs -- 1000 5000
mix run examples/benchmark_bulk_enqueue.exs -- disc
mix run examples/benchmark_bulk_enqueue.exs -- disc 1000 5000
```

`disc` uses Mnesia `disc_copies` in a temporary directory. That keeps the table in memory and logs it to disk. It is not `disc_only_copies`.

`insert/3` writes one `:inserted` history row per job. `insert_many/2` does not, unless the run passes `history: true`. Speedup is `insert/3` time divided by default `insert_many/2` time. Chunk size is 500. The table is cleared before each measurement.

One sample on a development machine, Mnesia `ram_copies`, queue `:default` paused:

| Jobs | `insert/3` | `insert_many/2` | `history: true` | Speedup |
|------|------------|-----------------|-----------------|---------|
| 1,000 | 49.3 ms, 20,282/s | 18.9 ms, 53,048/s | 28.6 ms, 35,018/s | 2.6x |
| 5,000 | 258.4 ms, 19,349/s | 96.8 ms, 51,647/s | 148.4 ms, 33,698/s | 2.7x |

Default bulk enqueue is about 50,000 jobs per second here, against about 20,000 for one `insert/3` per job. Recording history on the bulk path costs another write per job and lands near 35,000 per second.

The same sizes on verified `disc_copies`, schema in a temporary directory:

| Jobs | `insert/3` | `insert_many/2` | `history: true` | Speedup |
|------|------------|-----------------|-----------------|---------|
| 1,000 | 54.9 ms, 18,202/s | 17.3 ms, 57,707/s | 30.4 ms, 32,872/s | 3.2x |
| 5,000 | 292.1 ms, 17,117/s | 124.7 ms, 40,107/s | 161.6 ms, 30,942/s | 2.3x |

Bulk enqueue stays in the same range as RAM, about 40,000–50,000 jobs per second. One `insert/3` per job slows down, because each transaction waits for the disc log. A second run on this machine was 3.6× at 1,000 jobs and 3.8× at 5,000. `disc_copies` still keeps the table in memory; only the log is on disk. These are single runs, not a guaranteed rate. Run the script on the target machine.

## What this does not scale

v0.4.0 makes the **write** path chunked. Claiming available jobs still scans the jobs table. Millions of concurrent available jobs stress that claim path. Fewer than 100k active jobs is the comfortable range. Indexes and later backends (Ecto `insert_all`, Mongo bulk write, SQS `SendMessageBatch`) are the follow-on work; they should implement `Kathikon.Storage.insert_jobs/2`.

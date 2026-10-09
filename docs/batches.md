# Batches

Fan-out/fan-in without blocking a BEAM process.

## Pattern

1. Parent job runs (e.g. large query, or a crawler discovering links)
2. `Kathikon.Batch.start/3` enqueues a known child list, or `open/2` + `append/2` streams children as they are discovered
3. `close/1` (included in `start/3`) moves the parent to `:waiting_for_children`
4. Each child completion updates batch counters
5. When policy is satisfied, a continuation job is enqueued (`on_complete`)

`insert_many/2` is independent ingest: no parent and no reduce. Batch fan-out writes its children through the same chunked `insert_jobs/2` path. See [Bulk enqueue](guides/bulk-enqueue.md).

## Crawler (map, then reduce)

```elixir
{:ok, batch} = Kathikon.Batch.open(parent.id, on_complete: {ReduceWorker, %{}})

# inside the crawl, each page of links
{:ok, _} = Kathikon.Batch.append(batch.batch_id, link_specs)

# discovery is finished; expected_count is the number appended
:ok = Kathikon.Batch.close(batch.batch_id)
```

The parent stays `:running` until `close/1`, so the reduce cannot start while discovery is still appending.

## API

```elixir
Kathikon.Batch.start(parent_id, child_specs, on_complete: {ReportWorker, args})
Kathikon.Batch.open(parent_id, on_complete: {ReduceWorker, args})
Kathikon.Batch.append(batch_id, child_specs)
Kathikon.Batch.close(batch_id)
Kathikon.Batch.status(batch_id)
Kathikon.Batch.children(batch_id)
Kathikon.Batch.results(batch_id)
Kathikon.Batch.retry_failed(batch_id)
```

## Why not block the parent process?

Blocking would tie up dispatcher concurrency and lose durability on crash. Persisted `:waiting_for_children` survives restarts; continuation jobs keep the workflow explicit.

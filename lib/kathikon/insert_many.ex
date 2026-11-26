defmodule Kathikon.InsertMany do
  @moduledoc false

  alias Kathikon.{Config, Job, Queue, Storage, Telemetry, Unique}

  @spec run(Enumerable.t(), keyword()) ::
          {:ok,
           %{
             inserted: non_neg_integer(),
             ids: [String.t()],
             errors: [{non_neg_integer(), term()}]
           }}
          | {:error, term()}
  def run(specs, opts) when is_list(opts) do
    with {:ok, chunk_size} <- chunk_size(opts) do
      write_specs(specs, chunk_size, opts)
    end
  end

  defp write_specs(specs, chunk_size, opts) do
    ctx = %{
      per_job?: Keyword.get(opts, :telemetry) == :per_job,
      on_error: Keyword.get(opts, :on_error, :continue),
      storage_opts: Keyword.take(opts, [:chunk_size, :history, :on_error])
    }

    specs
    |> Stream.with_index()
    |> Stream.map(fn {spec, index} -> {index, build_spec(spec)} end)
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce_while({[], [], MapSet.new()}, fn chunk, acc ->
      reduce_chunk(chunk, acc, ctx)
    end)
    |> summarize()
  end

  defp reduce_chunk(chunk, {ids, errors, queues}, ctx) do
    {ready, failed} = split_chunk(chunk)

    if failed != [] and ctx.on_error == :abort do
      {_index, reason} = hd(failed)
      {:halt, {:error, reason}}
    else
      write_ready(ready, {ids, errors ++ failed, queues}, ctx)
    end
  end

  defp write_ready([], acc, _ctx), do: {:cont, acc}

  defp write_ready(ready, {ids, errors, queues}, ctx) do
    {:ok, queues} = ensure_queues(ready, queues)
    write_chunk(ready, ctx, ids, errors, queues)
  end

  defp split_chunk(chunk) do
    {ready, failed} =
      Enum.reduce(chunk, {[], []}, fn
        {index, {:ok, job, unique}}, {ready, failed} ->
          {[{index, {job, unique}} | ready], failed}

        {index, {:error, reason}}, {ready, failed} ->
          {ready, [{index, reason} | failed]}
      end)

    {Enum.reverse(ready), Enum.reverse(failed)}
  end

  defp write_chunk(ready, ctx, ids, errors, queues) do
    jobs = Enum.map(ready, fn {_index, {job, _unique}} -> job end)
    uniques = Enum.map(ready, fn {_index, {_job, unique}} -> unique end)
    storage_opts = Keyword.put(ctx.storage_opts, :unique_opts, uniques)

    case Storage.insert_jobs(jobs, storage_opts) do
      {:ok, %{ids: chunk_ids, errors: chunk_errors}} ->
        shifted = shift_errors(ready, chunk_errors)
        emit_chunk(jobs, chunk_ids, ctx.per_job?)
        {:cont, {ids ++ chunk_ids, errors ++ shifted, queues}}

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp summarize({:error, reason}), do: {:error, reason}

  defp summarize({ids, errors, _queues}) do
    {:ok, %{inserted: length(ids), ids: ids, errors: errors}}
  end

  defp ensure_queues(ready, queues) do
    Enum.reduce_while(ready, {:ok, queues}, fn {_index, {job, _unique}}, {:ok, queues} ->
      ensure_queue(job.queue, queues)
    end)
  end

  defp ensure_queue(queue, queues) do
    if MapSet.member?(queues, queue) do
      {:cont, {:ok, queues}}
    else
      start_queue(queue, queues)
    end
  end

  defp start_queue(queue, queues) do
    :ok = Queue.ensure_started(queue)
    {:cont, {:ok, MapSet.put(queues, queue)}}
  end

  defp emit_chunk(_jobs, chunk_ids, false) do
    Telemetry.event([:job, :inserted_many], %{count: length(chunk_ids)}, %{})
  end

  defp emit_chunk(jobs, chunk_ids, true) do
    emit_chunk(jobs, chunk_ids, false)
    written = MapSet.new(chunk_ids)

    jobs
    |> Enum.filter(&MapSet.member?(written, &1.id))
    |> Enum.each(&emit_inserted/1)
  end

  defp emit_inserted(job) do
    Telemetry.event([:job, :inserted], %{}, %{
      job_id: job.id,
      queue: job.queue,
      worker: job.worker,
      state: job.state
    })
  end

  defp shift_errors(ready, errors) do
    indexes = Enum.map(ready, fn {index, _pair} -> index end)

    Enum.map(errors, fn {local_index, reason} ->
      {Enum.at(indexes, local_index), reason}
    end)
  end

  defp build_spec({worker, args}) when is_atom(worker) and is_map(args) do
    build_entry(worker, args, [])
  end

  defp build_spec({worker, args, opts}) when is_atom(worker) and is_map(args) and is_list(opts) do
    build_entry(worker, args, opts)
  end

  defp build_spec(%{worker: worker} = spec) when is_atom(worker) do
    args = Map.get(spec, :args, %{})
    opts = spec |> Map.drop([:worker, :args]) |> Map.to_list()
    build_entry(worker, args, opts)
  end

  defp build_entry(worker, args, opts) do
    unique = Unique.resolve(worker, opts)
    job = Job.build(worker, args, Keyword.delete(opts, :unique))

    case Unique.prepare(job, unique) do
      {:ok, prepared} -> {:ok, job, prepared}
      {:error, reason} -> {:error, reason}
    end
  end

  defp chunk_size(opts) do
    case Keyword.get(opts, :chunk_size, Config.insert_many_chunk_size()) do
      size when is_integer(size) and size > 0 -> {:ok, size}
      other -> {:error, {:invalid_chunk_size, other}}
    end
  end
end

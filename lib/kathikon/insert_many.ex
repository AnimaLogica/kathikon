defmodule Kathikon.InsertMany do
  @moduledoc false

  alias Kathikon.{Config, Job, Queue, Storage, Telemetry}

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
    per_job? = Keyword.get(opts, :telemetry) == :per_job
    storage_opts = Keyword.take(opts, [:chunk_size, :history, :on_error])

    specs
    |> Stream.with_index()
    |> Stream.map(fn {spec, index} -> {index, build_spec(spec)} end)
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce_while({[], [], MapSet.new()}, fn chunk, acc ->
      reduce_chunk(chunk, acc, storage_opts, per_job?)
    end)
    |> summarize()
  end

  defp reduce_chunk(chunk, {ids, errors, queues}, storage_opts, per_job?) do
    {:ok, queues} = ensure_queues(chunk, queues)
    write_chunk(chunk, storage_opts, per_job?, ids, errors, queues)
  end

  defp write_chunk(chunk, storage_opts, per_job?, ids, errors, queues) do
    jobs = Enum.map(chunk, fn {_index, job} -> job end)

    case Storage.insert_jobs(jobs, storage_opts) do
      {:ok, %{ids: chunk_ids, errors: chunk_errors}} ->
        shifted = shift_errors(chunk, chunk_errors)
        emit_chunk(jobs, chunk_ids, per_job?)
        {:cont, {ids ++ chunk_ids, errors ++ shifted, queues}}

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp summarize({:error, reason}), do: {:error, reason}

  defp summarize({ids, errors, _queues}) do
    {:ok, %{inserted: length(ids), ids: ids, errors: errors}}
  end

  defp ensure_queues(chunk, queues) do
    Enum.reduce_while(chunk, {:ok, queues}, fn {_index, job}, {:ok, queues} ->
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

  defp shift_errors(chunk, errors) do
    indexes = Enum.map(chunk, fn {index, _job} -> index end)

    Enum.map(errors, fn {local_index, reason} ->
      {Enum.at(indexes, local_index), reason}
    end)
  end

  defp build_spec({worker, args}) when is_atom(worker) and is_map(args) do
    Job.build(worker, args, [])
  end

  defp build_spec({worker, args, opts}) when is_atom(worker) and is_map(args) and is_list(opts) do
    Job.build(worker, args, opts)
  end

  defp build_spec(%{worker: worker} = spec) when is_atom(worker) do
    args = Map.get(spec, :args, %{})
    opts = spec |> Map.drop([:worker, :args]) |> Map.to_list()
    Job.build(worker, args, opts)
  end

  defp chunk_size(opts) do
    case Keyword.get(opts, :chunk_size, Config.insert_many_chunk_size()) do
      size when is_integer(size) and size > 0 -> {:ok, size}
      other -> {:error, {:invalid_chunk_size, other}}
    end
  end
end

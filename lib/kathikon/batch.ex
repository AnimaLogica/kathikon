defmodule Kathikon.Batch do
  @moduledoc """
  Simple parent/child batch workflows for fan-out/fan-in.

  `start/3` is the small case: it opens a batch, appends every child, and closes.
  A crawler that discovers links while it runs should stream them:

      {:ok, batch} = Kathikon.Batch.open(parent.id, on_complete: {ReduceWorker, %{}})
      {:ok, _} = Kathikon.Batch.append(batch.batch_id, link_specs)
      :ok = Kathikon.Batch.close(batch.batch_id)

  `open/2` leaves the parent `:running` and the batch `:loading`. `append/2`
  writes children through `Kathikon.Storage.insert_jobs/2`. `close/1` is the seal:
  the parent moves to `:waiting_for_children` and the continuation waits until
  finished children equal the sealed count. Append after close returns
  `{:error, :closed}`.

  See `docs/batches.md`.
  """

  alias Kathikon.{Job, Storage, Telemetry}

  @type child_spec :: {module(), term(), keyword()} | map()

  @doc """
  Starts a batch from a parent job, enqueueing child jobs.

  ## Options

    * `:on_complete` — `{WorkerModule, args}` continuation when batch succeeds
    * `:success_policy` — `:all_succeeded` (default), `{:at_least, n}`, or `:allow_partial`
    * `:queue` — queue for child jobs

  ## Examples

      {:ok, batch} =
        Kathikon.Batch.start(parent_job_id, [
          {ProcessRowWorker, %{"row" => 1}, []},
          {ProcessRowWorker, %{"row" => 2}, []}
        ], on_complete: {SummarizeWorker, %{}})
  """
  @spec start(String.t(), [child_spec()], keyword()) :: {:ok, map()} | {:error, term()}
  def start(parent_job_id, child_specs, opts \\ []) when is_list(child_specs) do
    append_opts = Keyword.put_new(opts, :on_error, :abort)

    with {:ok, opened} <- open(parent_job_id, opts),
         {:ok, result} <- append(opened.batch_id, child_specs, append_opts),
         :ok <- require_clean_append(result),
         :ok <- close(opened.batch_id) do
      status(opened.batch_id)
    end
  end

  @doc """
  Opens a batch for a running parent and leaves that parent `:running`.

  Children are added with `append/2`. The parent waits only after `close/1`.

  ## Options

    * `:on_complete` — `{WorkerModule, args}` continuation when the batch succeeds
    * `:success_policy` — `:all_succeeded` (default), `{:at_least, n}`, or `:allow_partial`
    * `:queue` — default queue for child jobs

  ## Examples

      {:ok, batch} =
        Kathikon.Batch.open(parent.id, on_complete: {ReduceWorker, %{}})
  """
  @spec open(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def open(parent_job_id, opts \\ []) when is_binary(parent_job_id) and is_list(opts) do
    with {:ok, parent} <- Storage.fetch(parent_job_id) do
      now = DateTime.utc_now()

      batch_attrs = %{
        batch_id: generate_id(),
        status: :loading,
        success_count: 0,
        failure_count: 0,
        cancelled_count: 0,
        success_policy: Keyword.get(opts, :success_policy, :all_succeeded),
        on_complete: Keyword.get(opts, :on_complete),
        created_at: now,
        completed_at: nil,
        metadata: %{},
        queue: Keyword.get(opts, :queue, parent.queue),
        parent_job_id: parent_job_id,
        child_job_ids: [],
        pending_count: 0,
        expected_count: 0
      }

      Storage.open_batch(parent_job_id, batch_attrs)
    end
  end

  @doc """
  Appends child jobs to a `:loading` batch.

  Only committed rows increase `expected_count`. A failed chunk leaves the parent
  `:running` so the caller can append the failed slice again or `close/1`.

  ## Examples

      {:ok, %{inserted: 2}} =
        Kathikon.Batch.append(batch_id, [
          {FetchWorker, %{"url" => "https://example.com/a"}},
          {FetchWorker, %{"url" => "https://example.com/b"}}
        ])
  """
  @spec append(String.t(), [child_spec()], keyword()) ::
          {:ok,
           %{
             inserted: non_neg_integer(),
             ids: [String.t()],
             errors: [{non_neg_integer(), term()}]
           }}
          | {:error, term()}
  def append(batch_id, child_specs, opts \\ []) when is_list(child_specs) and is_list(opts) do
    with {:ok, batch} <- status(batch_id) do
      if batch.status == :loading do
        append_loading(batch, child_specs, opts)
      else
        {:error, :closed}
      end
    end
  end

  @doc """
  Seals a batch. The parent moves to `:waiting_for_children`.

  Further `append/2` calls fail. The continuation runs only after finished
  children equal the sealed `expected_count`.

  ## Examples

      :ok = Kathikon.Batch.close(batch_id)
  """
  @spec close(String.t()) :: :ok | {:error, term()}
  def close(batch_id) when is_binary(batch_id) do
    case Storage.close_batch(batch_id) do
      {:ok, :pending, batch} ->
        emit_started(batch)
        :ok

      {:ok, :complete, batch, parent} ->
        emit_started(batch)
        complete_batch(batch, parent)
        :ok

      {:ok, :fail, batch, parent} ->
        emit_started(batch)
        :ok = fail_batch(batch, parent)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Returns batch status by batch id.

  ## Examples

      {:ok, batch} = Kathikon.Batch.status(batch_id)
      batch.status
      #=> :running
  """
  @spec status(String.t()) :: {:ok, map()} | {:error, term()}
  def status(batch_id) do
    case Storage.fetch_batch(batch_id) do
      {:ok, batch} -> {:ok, batch}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Lists child job ids for a batch.

  ## Examples

      {:ok, child_ids} = Kathikon.Batch.children(batch_id)
  """
  @spec children(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def children(batch_id) do
    with {:ok, batch} <- status(batch_id) do
      {:ok, batch.child_job_ids}
    end
  end

  @doc """
  Returns results for completed child jobs in a batch.

  ## Examples

      {:ok, results} = Kathikon.Batch.results(batch_id)

      Enum.filter(results, &(&1.state == :completed))
  """
  @spec results(String.t()) :: {:ok, [map()]} | {:error, term()}
  def results(batch_id) do
    with {:ok, batch} <- status(batch_id),
         {:ok, jobs} <- fetch_child_jobs(batch.child_job_ids) do
      results =
        Enum.map(jobs, fn job ->
          %{job_id: job.id, state: job.state, result: job.result, error: job.last_error}
        end)

      {:ok, results}
    end
  end

  @doc """
  Retries failed children in a batch.

  ## Examples

      {:ok, retried} = Kathikon.Batch.retry_failed(batch_id)
      length(retried)
  """
  @spec retry_failed(String.t()) :: {:ok, [Job.t()]} | {:error, term()}
  def retry_failed(batch_id) do
    with {:ok, batch} <- status(batch_id),
         {:ok, jobs} <- fetch_child_jobs(batch.child_job_ids) do
      retried =
        jobs
        |> Enum.filter(&(&1.state in [:failed, :dead, :retryable]))
        |> Enum.map(fn job ->
          {:ok, retried} = Storage.retry_job(job.id, [])
          retried
        end)

      updated = %{
        batch
        | pending_count: batch.pending_count + length(retried),
          failure_count: max(0, batch.failure_count - length(retried)),
          status: :running
      }

      {:ok, _} = Storage.write_batch(updated)
      {:ok, retried}
    end
  end

  @doc false
  def handle_child_finished(child_job) do
    case Storage.record_batch_child_finished(child_job) do
      {:ok, :ignored} -> :ok
      {:ok, :already_finished} -> :ok
      {:ok, :pending, _batch} -> :ok
      {:ok, :complete, batch, parent} -> complete_batch(batch, parent)
      {:ok, :fail, batch, parent} -> fail_batch(batch, parent)
      _ -> :ok
    end
  end

  defp append_loading(batch, child_specs, opts) do
    jobs =
      Enum.map(child_specs, fn spec ->
        {worker, args, child_opts} = normalize_spec(spec, batch.queue)

        Job.build(
          worker,
          args,
          Keyword.merge(child_opts, parent_job_id: batch.parent_job_id, batch_id: batch.batch_id)
        )
      end)

    storage_opts =
      opts
      |> Keyword.take([:chunk_size, :history, :on_error])
      |> Keyword.put(:batch_id, batch.batch_id)

    with :ok <- ensure_queues(jobs),
         {:ok, result} <- Storage.insert_jobs(jobs, storage_opts) do
      _ = record_appended_fallback(batch.batch_id, result.ids)
      {:ok, %{inserted: length(result.ids), ids: result.ids, errors: result.errors}}
    end
  end

  defp record_appended_fallback(_batch_id, []), do: :ok

  defp record_appended_fallback(batch_id, ids) do
    if function_exported?(Storage.backend(), :insert_jobs, 2) do
      :ok
    else
      {:ok, _} = Storage.record_batch_appended(batch_id, ids)
      :ok
    end
  end

  defp ensure_queues(jobs) do
    jobs
    |> Enum.map(& &1.queue)
    |> Enum.uniq()
    |> Enum.each(fn queue -> :ok = Kathikon.Queue.ensure_started(queue) end)

    :ok
  end

  defp require_clean_append(%{errors: []}), do: :ok
  defp require_clean_append(%{errors: errors}), do: {:error, errors}

  defp emit_started(batch) do
    Telemetry.event([:batch, :started], %{children: length(batch.child_job_ids)}, %{
      batch_id: batch.batch_id,
      parent_job_id: batch.parent_job_id
    })
  end

  defp complete_batch(batch, parent) do
    now = DateTime.utc_now()

    {:ok, _} =
      Storage.complete_job(parent.id, %{batch_id: batch.batch_id}, %{
        batch_id: batch.batch_id
      })

    completed_batch = %{batch | status: :completed, completed_at: now}
    {:ok, _} = Storage.write_batch(completed_batch)

    _ = enqueue_continuation(batch)

    Telemetry.event([:batch, :completed], %{success_count: batch.success_count}, %{
      batch_id: batch.batch_id,
      parent_job_id: parent.id
    })

    _ =
      Storage.insert_history_event(parent.id, %{
        id: generate_id(),
        job_id: parent.id,
        event: :batch_completed,
        from_state: :waiting_for_children,
        to_state: :completed,
        metadata: %{batch_id: batch.batch_id},
        inserted_at: now
      })
  end

  defp fail_batch(batch, parent) do
    {:ok, _} =
      Storage.fail_job(parent.id, :batch_failed, %{
        batch_id: batch.batch_id,
        attempt: parent.max_attempts
      })

    failed_batch = %{batch | status: :failed, completed_at: DateTime.utc_now()}
    {:ok, _} = Storage.write_batch(failed_batch)
    :ok
  end

  defp enqueue_continuation(%{on_complete: {worker, args}}) when is_atom(worker) do
    Kathikon.insert(worker, args || %{})
  end

  defp enqueue_continuation(_), do: :ok

  defp fetch_child_jobs(ids) do
    jobs =
      Enum.map(ids, fn id ->
        {:ok, job} = Storage.fetch(id)
        job
      end)

    {:ok, jobs}
  end

  defp normalize_spec({worker, args, opts}, default_queue) do
    {worker, args || %{}, Keyword.put_new(opts || [], :queue, default_queue)}
  end

  defp normalize_spec(%{worker: worker} = spec, default_queue) do
    {
      worker,
      Map.get(spec, :args, %{}),
      [
        queue: Map.get(spec, :queue, default_queue),
        max_attempts: Map.get(spec, :max_attempts, Kathikon.Config.max_attempts())
      ]
    }
  end

  defp generate_id do
    Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end

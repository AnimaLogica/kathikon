defmodule Kathikon.Storage do
  @moduledoc """
  Storage behaviour and facade for job persistence.

  Application code should prefer `Kathikon.insert/3` and management APIs.
  Configure the backend via `config :kathikon, storage_backend: module`.

  Default: `Kathikon.Storage.Mnesia`.

  ## Examples

      :ok = Kathikon.Storage.setup()

      # Tests and Livebook embedding
      Kathikon.Storage.reset!()

  See `docs/storage.md`.
  """

  alias Kathikon.Job

  @backend_key :storage_backend
  @storage_override {:kathikon, :storage_override}

  @type job :: Job.t() | map()

  @callback setup() :: :ok
  @callback clear_jobs!() :: :ok
  @callback reset!() :: :ok

  @callback insert_job(job()) :: {:ok, String.t()} | {:ok, Job.t()} | {:error, term()}
  @callback get_job(String.t()) :: {:ok, map()} | {:ok, Job.t()} | {:error, :not_found | term()}
  @callback update_job(String.t(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, :not_found | term()}
  @callback claim_job(String.t(), map()) ::
              {:ok, map()}
              | {:ok, Job.t()}
              | {:error, :not_found | :already_claimed | :not_claimable | term()}
  @callback claim_available_jobs(atom(), pos_integer(), map()) ::
              {:ok, [map()]} | {:ok, [Job.t()]} | {:error, term()}
  @callback claim_and_start_available_jobs(atom(), pos_integer(), map()) ::
              {:ok, [map()]} | {:ok, [Job.t()]} | {:error, term()}
  @callback defer_job(String.t(), DateTime.t(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback complete_job(String.t(), term(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback fail_job(String.t(), term(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback retry_job(String.t(), keyword()) :: {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback discard_job(String.t(), term(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback cancel_job(String.t(), term(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback list_jobs(keyword()) :: {:ok, [map()]} | {:ok, [Job.t()]} | {:error, term()}
  @callback list_jobs_page(keyword()) ::
              {:ok, %{jobs: [map()] | [Job.t()], total: non_neg_integer()}} | {:error, term()}
  @callback insert_history_event(String.t(), map()) :: :ok | {:error, term()}
  @callback list_history(String.t()) :: {:ok, [map()]} | {:error, term()}
  @callback move_to_dead_letter(String.t(), term(), map()) ::
              {:ok, map()} | {:ok, Job.t()} | {:error, term()}
  @callback list_dead_jobs(keyword()) :: {:ok, [map()]} | {:ok, [Job.t()]} | {:error, term()}

  @callback insert(Job.t()) :: {:ok, Job.t()} | {:error, term()}
  @callback update(Job.t()) :: {:ok, Job.t()} | {:error, term()}
  @callback fetch(String.t()) :: {:ok, Job.t()} | {:error, term()}
  @callback claim(atom(), DateTime.t()) :: {:ok, Job.t()} | :not_found | {:error, term()}
  @callback promote_scheduled(DateTime.t()) :: non_neg_integer()
  @callback prunable_jobs(DateTime.t()) :: [Job.t()]
  @callback delete(String.t()) :: :ok
  @callback all() :: [Job.t()]

  @callback start_job(Job.t(), map(), DateTime.t()) :: {:ok, Job.t()} | {:error, term()}

  @callback insert_jobs([Job.t()], keyword()) ::
              {:ok, %{ids: [String.t()], errors: [{non_neg_integer(), term()}]}}
              | {:error, term()}

  @optional_callbacks [
    start_job: 3,
    claim_and_start_available_jobs: 3,
    defer_job: 3,
    insert: 1,
    update: 1,
    fetch: 1,
    claim: 2,
    promote_scheduled: 1,
    prunable_jobs: 1,
    delete: 1,
    all: 0,
    list_jobs_page: 1,
    insert_jobs: 2
  ]

  @doc """
  Ensures the storage backend schema and tables exist.

  Called automatically on application start. Call explicitly when embedding
  Kathikon in scripts, Livebook, or tests without the full application.

  ## Examples

      :ok = Kathikon.Storage.setup()
  """
  @spec setup() :: :ok
  def setup, do: backend_module().setup()

  @doc false
  @spec clear_jobs!() :: :ok
  def clear_jobs!, do: backend_module().clear_jobs!()

  @doc false
  @spec reset!() :: :ok
  def reset!, do: backend_module().reset!()

  @doc false
  @spec backend() :: module()
  def backend, do: backend_module()

  @doc false
  @spec backend(module()) :: :ok
  def backend(module) when is_atom(module) do
    Application.put_env(:kathikon, @backend_key, module)
  end

  @doc false
  @spec set_test_backend!(module()) :: :ok
  def set_test_backend!(module) when is_atom(module) do
    Process.put(@storage_override, module)
    :ok
  end

  @doc false
  @spec clear_test_backend!() :: :ok
  def clear_test_backend! do
    Process.delete(@storage_override)
    :ok
  end

  @doc false
  @spec with_backend(module(), (-> term())) :: term()
  def with_backend(module, fun) when is_atom(module) and is_function(fun, 0) do
    previous = Process.get(@storage_override)
    Process.put(@storage_override, module)

    try do
      fun.()
    after
      case previous do
        nil -> Process.delete(@storage_override)
        mod -> Process.put(@storage_override, mod)
      end
    end
  end

  @doc false
  def start_job(job, claimant, now \\ DateTime.utc_now()),
    do: backend_module().start_job(job, claimant, now)

  @doc false
  def insert(job), do: backend_module().insert(job)

  @doc """
  Inserts many jobs through the storage backend.

  Backends can implement the optional `insert_jobs/2` callback. When a backend
  does not, each job is written on its own.

  ## Options

    * `:chunk_size` — jobs per storage transaction
    * `:history` — `false` (default) skips the `:inserted` row; `true` records it
    * `:on_error` — `:continue` (default) or `:abort`

  ## Examples

      {:ok, %{ids: ids, errors: []}} =
        Kathikon.Storage.insert_jobs(jobs, chunk_size: 500)
  """
  @spec insert_jobs([Job.t()], keyword()) ::
          {:ok, %{ids: [String.t()], errors: [{non_neg_integer(), term()}]}}
          | {:error, term()}
  def insert_jobs(jobs, opts \\ []) when is_list(jobs) and is_list(opts) do
    mod = backend_module()

    if function_exported?(mod, :insert_jobs, 2) do
      mod.insert_jobs(jobs, opts)
    else
      insert_jobs_fallback(jobs, opts)
    end
  end

  @doc false
  def update(job), do: backend_module().update(job)

  @doc false
  def fetch(id), do: backend_module().fetch(id)

  @doc false
  def claim(queue, now), do: backend_module().claim(queue, now)

  @doc false
  def promote_scheduled(now), do: backend_module().promote_scheduled(now)

  @doc false
  def prunable_jobs(cutoff), do: backend_module().prunable_jobs(cutoff)

  @doc false
  def delete(id), do: backend_module().delete(id)

  @doc false
  def all, do: backend_module().all()

  @doc false
  def insert_job(job), do: backend_module().insert_job(job)

  @doc false
  def get_job(id), do: backend_module().get_job(id)

  @doc false
  def update_job(id, changes), do: backend_module().update_job(id, changes)

  @doc false
  def claim_job(id, claimant), do: backend_module().claim_job(id, claimant)

  @doc false
  def claim_available_jobs(queue, limit, claimant),
    do: backend_module().claim_available_jobs(queue, limit, claimant)

  @doc false
  def claim_and_start_available_jobs(queue, limit, claimant),
    do: backend_module().claim_and_start_available_jobs(queue, limit, claimant)

  @doc false
  def defer_job(id, scheduled_at, metadata),
    do: backend_module().defer_job(id, scheduled_at, metadata)

  @doc false
  def fetch_batch(batch_id), do: backend_module().fetch_batch(batch_id)

  @doc false
  def write_batch(batch), do: backend_module().write_batch(batch)

  @doc false
  def start_batch(parent_id, child_jobs, batch_attrs),
    do: backend_module().start_batch(parent_id, child_jobs, batch_attrs)

  @doc false
  def open_batch(parent_id, batch_attrs),
    do: backend_module().open_batch(parent_id, batch_attrs)

  @doc false
  def close_batch(batch_id), do: backend_module().close_batch(batch_id)

  @doc false
  def record_batch_appended(batch_id, ids),
    do: backend_module().record_batch_appended(batch_id, ids)

  @doc false
  def record_batch_child_finished(child_job),
    do: backend_module().record_batch_child_finished(child_job)

  @doc false
  def complete_job(id, result, metadata), do: backend_module().complete_job(id, result, metadata)

  @doc false
  def fail_job(id, error, metadata), do: backend_module().fail_job(id, error, metadata)

  @doc false
  def retry_job(id, opts \\ []), do: backend_module().retry_job(id, opts)

  @doc false
  def discard_job(id, reason, metadata), do: backend_module().discard_job(id, reason, metadata)

  @doc false
  def cancel_job(id, reason, metadata), do: backend_module().cancel_job(id, reason, metadata)

  @doc false
  def list_jobs(opts \\ []), do: backend_module().list_jobs(opts)

  @doc false
  def list_jobs_page(opts \\ []) do
    mod = backend_module()

    if function_exported?(mod, :list_jobs_page, 1) do
      mod.list_jobs_page(opts)
    else
      list_jobs_page_fallback(opts)
    end
  end

  @doc false
  def insert_history_event(job_id, event),
    do: backend_module().insert_history_event(job_id, event)

  @doc false
  def list_history(job_id), do: backend_module().list_history(job_id)

  @doc false
  def move_to_dead_letter(id, reason, metadata),
    do: backend_module().move_to_dead_letter(id, reason, metadata)

  @doc false
  def list_dead_jobs(opts \\ []), do: backend_module().list_dead_jobs(opts)

  defp insert_jobs_fallback(jobs, opts) do
    on_error = Keyword.get(opts, :on_error, :continue)

    jobs
    |> Enum.with_index()
    |> Enum.reduce_while({[], []}, fn {job, index}, {ids, errors} ->
      case insert(job) do
        {:ok, saved} ->
          {:cont, {[saved.id | ids], errors}}

        {:error, reason} when on_error == :abort ->
          {:halt, {:error, reason}}

        {:error, reason} ->
          {:cont, {ids, [{index, reason} | errors]}}
      end
    end)
    |> case do
      {:error, reason} ->
        {:error, reason}

      {ids, errors} ->
        {:ok, %{ids: Enum.reverse(ids), errors: Enum.reverse(errors)}}
    end
  end

  defp list_jobs_page_fallback(opts) do
    queue = Keyword.get(opts, :queue)
    states = Keyword.get(opts, :states)
    limit = Keyword.get(opts, :limit, 50)
    offset = Keyword.get(opts, :offset, 0)
    order = Keyword.get(opts, :order, :newest)

    with {:ok, jobs} <- list_jobs(if queue, do: [queue: queue], else: []) do
      filtered =
        jobs
        |> filter_jobs_by_states(states)
        |> sort_jobs_for_page(order)

      page = filtered |> Enum.drop(offset) |> Enum.take(limit)
      {:ok, %{jobs: page, total: length(filtered)}}
    end
  end

  defp filter_jobs_by_states(jobs, nil), do: jobs
  defp filter_jobs_by_states(jobs, states), do: Enum.filter(jobs, &(&1.state in states))

  defp sort_jobs_for_page(jobs, :newest) do
    Enum.sort_by(jobs, &job_page_sort_time/1, {:desc, DateTime})
  end

  defp sort_jobs_for_page(jobs, :oldest) do
    Enum.sort_by(jobs, &job_page_sort_time/1, DateTime)
  end

  defp job_page_sort_time(job) do
    job.inserted_at || job.available_at || DateTime.utc_now()
  end

  defp backend_module do
    Process.get(@storage_override) ||
      Application.get_env(:kathikon, @backend_key, Kathikon.Storage.Mnesia)
  end
end

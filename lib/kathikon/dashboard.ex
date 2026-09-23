defmodule Kathikon.Dashboard do
  @moduledoc """
  Operations and reporting facade for dashboards, CLIs, and RPC consumers.

  Extends `Kathikon.Report.queue_summary/1` with dashboard fields (`ui_counts`,
  dynamic queues from storage) and delegates job control to `Kathikon.*`.

  See `docs/dashboard_spec.md` and `docs/management_api.md`.

  ## Examples

      {:ok, queues} = Kathikon.Dashboard.queue_summary()

      {:ok, %{jobs: jobs, total: total}} =
        Kathikon.Dashboard.list_jobs(queue: :default, states: [:completed], limit: 50)

      {:ok, %{job: job, history: history}} = Kathikon.Dashboard.fetch_job(job_id)

      :ok = Kathikon.Dashboard.pause_all()
  """

  alias Kathikon.{Config, Job, QueueControl, Report, Storage}

  @type state_tab ::
          :available
          | :executing
          | :retryable
          | :completed
          | :cancelled
          | :dead
          | :discarded

  # Dialyzer: job map includes many struct fields; minimum key for contract is :attempt.
  @type job_map :: %{attempt: non_neg_integer()}

  @type fetch_result :: %{history: [map()], job: job_map()}

  @default_limit 50
  @default_offset 0

  @state_tab_order [
    :available,
    :executing,
    :retryable,
    :completed,
    :cancelled,
    :dead,
    :discarded
  ]

  @state_tabs %{
    available: [:scheduled, :available],
    executing: [:claimed, :running, :waiting_for_children],
    retryable: [:retryable],
    completed: [:completed],
    cancelled: [:cancelled],
    dead: [:failed, :dead],
    discarded: [:discarded]
  }

  # Per-state dashboard actions (UI enable/disable). Does not include force-kill of :running.
  @actions %{
    scheduled: [:cancel, :discard],
    available: [:cancel, :discard],
    claimed: [:cancel, :discard],
    running: [],
    waiting_for_children: [],
    retryable: [:cancel, :retry, :discard],
    completed: [:rerun, :purge],
    cancelled: [:purge],
    failed: [:retry, :rerun, :discard],
    dead: [:retry, :rerun, :discard],
    discarded: [:purge]
  }

  @doc """
  Returns queue summary rows for the dashboard table.

  Each row includes `counts`, `ui_counts`, `executing` (`:claimed` + `:running`),
  `failed` (`:failed` + `:dead`), `total`, and `paused`.

  Builds on `Kathikon.Report.queue_summary/1` and includes queues seen in storage
  even when not in `config :kathikon, :queues`.
  """
  @spec queue_summary(keyword()) :: {:ok, [map()]} | {:error, term()}
  def queue_summary(opts \\ []) do
    with {:ok, base} <- Report.queue_summary(opts),
         {:ok, jobs} <- Storage.list_jobs([]) do
      base_by_queue = Map.new(base, &{&1.queue, &1})
      queues = all_queue_names(jobs)

      summaries =
        Enum.map(queues, fn queue ->
          row =
            Map.get(base_by_queue, queue, %{
              queue: queue,
              paused: QueueControl.paused?(queue),
              counts: %{}
            })

          counts = row.counts

          %{
            queue: queue,
            paused: QueueControl.paused?(queue),
            counts: counts,
            ui_counts: ui_counts(counts),
            executing: in_flight(counts) + Map.get(counts, :waiting_for_children, 0),
            failed: failed_count(counts),
            total: Enum.sum(Map.values(counts))
          }
        end)

      {:ok, summaries}
    end
  end

  @doc """
  Lists jobs with optional queue, state, and pagination filters.

  ## Options

    * `:queue` — filter by queue atom
    * `:states` — list of job states (or a single state atom)
    * `:tab` — preset tab (`:available`, `:executing`, `:retryable`, etc.)
    * `:limit` — max rows (default `50`)
    * `:offset` — skip rows (default `0`)
    * `:order` — `:newest` (default) or `:oldest` by `inserted_at`

  Returns `{:ok, %{jobs: [map()], total: count, limit: n, offset: n}}`.
  """
  @spec list_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def list_jobs(opts \\ []) do
    limit = Keyword.get(opts, :limit, @default_limit)
    offset = Keyword.get(opts, :offset, @default_offset)
    order = Keyword.get(opts, :order, :newest)
    states = resolve_states(opts)

    page_opts =
      []
      |> maybe_put(:queue, Keyword.get(opts, :queue))
      |> maybe_put(:states, if(states == [], do: nil, else: states))
      |> Keyword.put(:limit, limit)
      |> Keyword.put(:offset, offset)
      |> Keyword.put(:order, order)

    with {:ok, %{jobs: jobs, total: total}} <- Storage.list_jobs_page(page_opts) do
      {:ok,
       %{
         jobs: Enum.map(jobs, &job_row/1),
         total: total,
         limit: limit,
         offset: offset
       }}
    end
  end

  @doc """
  Fetches a job and its history for drill-down views.

  The job is a plain map from `Kathikon.Job.to_map/1`.
  """
  @spec fetch_job(String.t()) :: {:ok, fetch_result()} | {:error, term()}
  def fetch_job(job_id) when is_binary(job_id) do
    with {:ok, job} <- Kathikon.fetch(job_id),
         {:ok, history} <- Kathikon.history(job_id) do
      {:ok, %{job: Job.to_map(job), history: history}}
    end
  end

  @doc "Maps a dashboard tab name to its job states."
  @spec states_for_tab(atom()) :: [atom()]
  def states_for_tab(tab) when is_atom(tab) do
    Map.get(@state_tabs, tab, [])
  end

  @doc "Returns configured dashboard state tab names in stable UI order."
  def state_tabs, do: @state_tab_order

  @doc """
  Returns UI action atoms enabled for a job state.

  Actions: `:cancel`, `:retry`, `:rerun`, `:discard`, `:purge`.

  `:running` and `:waiting_for_children` cannot be cancelled (v0.2 limitation).
  """
  @spec actions_for_state(atom()) :: [atom()]
  def actions_for_state(state) when is_atom(state) do
    Map.get(@actions, state, [])
  end

  @doc "Sends a scheduler promoter tick (promotes `:scheduled` → `:available`)."
  @spec promote_now() :: :ok
  def promote_now do
    send(Kathikon.Scheduler.Promoter, :tick)
    :ok
  end

  @doc "Pauses every known queue."
  @spec pause_all() :: :ok
  def pause_all do
    Enum.each(all_known_queues(), &Kathikon.pause_queue/1)
    :ok
  end

  @doc "Resumes every known queue."
  @spec resume_all() :: :ok
  def resume_all do
    Enum.each(all_known_queues(), &Kathikon.resume_queue/1)
    :ok
  end

  @spec pause_queue(atom()) :: :ok
  def pause_queue(queue) when is_atom(queue), do: Kathikon.pause_queue(queue)

  @spec resume_queue(atom()) :: :ok
  def resume_queue(queue) when is_atom(queue), do: Kathikon.resume_queue(queue)

  @spec queue_status(atom()) :: map()
  def queue_status(queue) when is_atom(queue), do: Kathikon.queue_status(queue)

  @spec cancel_job(String.t(), term()) :: {:ok, Job.t()} | {:error, term()}
  def cancel_job(job_id, reason \\ nil), do: Kathikon.cancel(job_id, reason)

  @spec retry_job(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def retry_job(job_id, opts \\ []), do: Kathikon.retry(job_id, opts)

  @spec rerun_job(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def rerun_job(job_id, opts \\ []), do: Kathikon.rerun(job_id, opts)

  @spec discard_job(String.t(), term()) :: {:ok, Job.t()} | {:error, term()}
  def discard_job(job_id, reason \\ nil) do
    with {:ok, job} <- Kathikon.fetch(job_id) do
      reason = reason || :dashboard_discard

      case job.state do
        :dead ->
          Kathikon.discard_dead(job_id, reason)

        state when state in [:failed, :retryable, :available, :scheduled, :claimed] ->
          Storage.discard_job(job_id, reason, %{})

        _ ->
          {:error, {:invalid_state, job.state}}
      end
    end
  end

  @doc """
  Cancels all cancellable jobs (not `:running`).

  ## Options

    * `:queue` — limit to a queue

  Returns `{:ok, %{succeeded: n, errors: [{id, reason}]}}`.
  """
  @spec cancel_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def cancel_jobs(opts \\ []) do
    cancellable = [:scheduled, :available, :retryable, :claimed]

    with {:ok, jobs} <- matching_jobs(Keyword.put(opts, :states, cancellable)) do
      bulk_map(jobs, &Kathikon.cancel(&1.id))
    end
  end

  @doc """
  Retries jobs in retryable, failed, or dead states.

  ## Options

    * `:queue` — limit to a queue
    * `:states` — defaults to `[:retryable, :failed, :dead]`

  Returns `{:ok, %{succeeded: n, errors: [{id, reason}]}}`.
  """
  @spec retry_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def retry_jobs(opts \\ []) do
    states = Keyword.get(opts, :states, [:retryable, :failed, :dead])

    with {:ok, jobs} <- matching_jobs(Keyword.put(opts, :states, states)) do
      bulk_map(jobs, &Kathikon.retry(&1.id))
    end
  end

  @doc """
  Creates linked reruns for dead (or failed) jobs.

  ## Options

    * `:queue` — limit to a queue
    * `:states` — defaults to `[:dead, :failed]`

  Returns `{:ok, %{succeeded: n, errors: [{id, reason}]}}`.
  """
  @spec rerun_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def rerun_jobs(opts \\ []) do
    states = Keyword.get(opts, :states, [:dead, :failed])

    with {:ok, jobs} <- matching_jobs(Keyword.put(opts, :states, states)) do
      bulk_map(jobs, &Kathikon.rerun(&1.id))
    end
  end

  @doc """
  Deletes jobs matching filters.

  ## Options

    * `:queue` — limit to a queue
    * `:states` — defaults to `[:completed, :cancelled, :discarded]`
    * `:older_than` — `DateTime` — only delete when terminal timestamp is before this

  Returns `{:ok, %{purged: count, errors: [{id, reason}]}}`.
  """
  @spec purge_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def purge_jobs(opts \\ []) do
    states = Keyword.get(opts, :states, [:completed, :cancelled, :discarded])
    older_than = Keyword.get(opts, :older_than)

    with {:ok, jobs} <- matching_jobs(Keyword.put(opts, :states, states)) do
      {purged, errors} = purge_matching_jobs(jobs, older_than)
      {:ok, %{purged: purged, errors: errors}}
    end
  end

  @doc """
  Discards jobs in discardable states.

  ## Options

    * `:queue` — limit to a queue
    * `:states` — defaults to `[:failed, :dead, :retryable]`

  Returns `{:ok, %{succeeded: n, errors: [{id, reason}]}}`.
  """
  @spec discard_jobs(keyword()) :: {:ok, map()} | {:error, term()}
  def discard_jobs(opts \\ []) do
    states = Keyword.get(opts, :states, [:failed, :dead, :retryable])

    with {:ok, jobs} <- matching_jobs(Keyword.put(opts, :states, states)) do
      bulk_map(jobs, &discard_job(&1.id))
    end
  end

  @doc "Triggers a pruner tick on the local node."
  @spec prune_now() :: :ok
  def prune_now do
    send(Kathikon.Pruner, :tick)
    :ok
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp list_jobs_storage_opts(opts) do
    case Keyword.get(opts, :queue) do
      nil -> []
      queue -> [queue: queue]
    end
  end

  defp resolve_states(opts) do
    cond do
      states = Keyword.get(opts, :states) ->
        List.wrap(states)

      tab = Keyword.get(opts, :tab) ->
        states_for_tab(tab)

      true ->
        []
    end
  end

  defp filter_states(jobs, []), do: jobs

  defp filter_states(jobs, states) do
    Enum.filter(jobs, &(&1.state in states))
  end

  defp purge_matching_jobs(jobs, older_than) do
    jobs
    |> Enum.filter(&purgeable?(&1, older_than))
    |> Enum.reduce({0, []}, &accumulate_purge/2)
    |> then(fn {purged, errors} -> {purged, Enum.reverse(errors)} end)
  end

  defp accumulate_purge(job, {count, err_list}) do
    case delete_job(job.id) do
      :ok -> {count + 1, err_list}
      {:error, reason} -> {count, [{job.id, reason} | err_list]}
    end
  end

  defp delete_job(job_id) do
    case Storage.delete(job_id) do
      :ok -> :ok
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end

  defp job_row(%Job{} = job) do
    %{
      id: job.id,
      state: job.state,
      queue: job.queue,
      worker: job.worker,
      attempts: job.attempts,
      max_attempts: job.max_attempts,
      attempts_label: "#{job.attempts}/#{job.max_attempts}",
      timestamp: job_timestamp(job),
      error_count: length(job.errors),
      last_error: job.last_error || job.error
    }
  end

  defp job_timestamp(job) do
    job.completed_at || job.inserted_at
  end

  defp matching_jobs(opts) do
    with {:ok, jobs} <- Storage.list_jobs(list_jobs_storage_opts(opts)) do
      states = resolve_states(opts)
      {:ok, filter_states(jobs, states)}
    end
  end

  defp bulk_map(jobs, fun) do
    {succeeded, errors} =
      Enum.reduce(jobs, {0, []}, fn job, {count, err_list} ->
        case fun.(job) do
          {:ok, _} -> {count + 1, err_list}
          {:error, reason} -> {count, [{job.id, reason} | err_list]}
        end
      end)

    {:ok, %{succeeded: succeeded, errors: Enum.reverse(errors)}}
  end

  defp purgeable?(_job, nil), do: true

  defp purgeable?(job, %DateTime{} = older_than) do
    case job_timestamp(job) do
      %DateTime{} = at -> DateTime.compare(at, older_than) == :lt
      _ -> false
    end
  end

  defp all_queue_names(jobs) do
    stored = Enum.map(jobs, & &1.queue)

    (all_configured_queues() ++ stored)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp all_known_queues do
    case Storage.list_jobs([]) do
      {:ok, jobs} -> all_queue_names(jobs)
      {:error, _} -> all_configured_queues()
    end
  end

  defp all_configured_queues, do: Config.queue_names()

  defp in_flight(counts) do
    Map.get(counts, :running, 0) + Map.get(counts, :claimed, 0)
  end

  defp failed_count(counts) do
    Map.get(counts, :failed, 0) + Map.get(counts, :dead, 0)
  end

  defp ui_counts(counts) do
    %{
      available: Map.get(counts, :available, 0) + Map.get(counts, :scheduled, 0),
      executing: in_flight(counts) + Map.get(counts, :waiting_for_children, 0),
      completed: Map.get(counts, :completed, 0),
      retryable: Map.get(counts, :retryable, 0),
      cancelled: Map.get(counts, :cancelled, 0),
      failed: failed_count(counts),
      discarded: Map.get(counts, :discarded, 0)
    }
  end
end

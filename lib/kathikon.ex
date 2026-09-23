defmodule Kathikon do
  @moduledoc """
  BEAM-native durable job queue and task execution platform.

  ## Examples

      # Enqueue work
      {:ok, job} = Kathikon.insert(MyApp.EmailWorker, %{"to" => "user@example.com"})

      # Schedule for later
      {:ok, job_id} = Kathikon.schedule(MyApp.DigestWorker, %{}, in: 3600)

      # Inspect
      {:ok, %{state: :completed}} = Kathikon.status(job.id)

  See the [README](readme.html) and `docs/` guides for v0.2.0+ features:
  atomic claiming, job history, dead-letter queue, scheduling, batches,
  management APIs, reporting, v0.2.1 operations tooling (`Kathikon.Dashboard`,
  `mix kathikon.ops`), and the v0.3.0 LiveDashboard page
  (`Kathikon.LiveDashboard.Page`).
  """

  alias Kathikon.{Job, Queue, QueueControl, Storage, Telemetry}

  @doc """
  Inserts a job into the durable queue.

  Starts the target queue dispatcher if needed. Emits `[:kathikon, :job, :inserted]`.

  ## Options

    * `:queue` — target queue (default `:default`)
    * `:priority` — higher values claim first (default `0`)
    * `:max_attempts` — retry limit (default from config)
    * `:schedule_in` — seconds until the job becomes available
    * `:schedule_at` — `DateTime` or `NaiveDateTime` when the job becomes available

  ## Examples

      {:ok, job} = Kathikon.insert(MyApp.EmailWorker, %{"to" => "user@example.com"})

      {:ok, job} =
        Kathikon.insert(MyApp.EmailWorker, %{"to" => "user@example.com"},
          queue: :emails,
          priority: 5,
          max_attempts: 10
        )

      {:ok, job} = Kathikon.insert(MyApp.DigestWorker, %{}, schedule_in: 3600)
  """
  @spec insert(module(), map(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def insert(worker, args, opts \\ []) when is_atom(worker) and is_map(args) do
    with {:ok, opts} <- Kathikon.Timezone.normalize_opts(opts),
         job = Job.build(worker, args, opts),
         :ok <- Queue.ensure_started(job.queue),
         {:ok, job} <- Storage.insert(job) do
      Telemetry.event([:job, :inserted], %{}, metadata(job, worker: worker))
      {:ok, job}
    end
  end

  @doc """
  Schedules a job at a datetime, after a duration, or with a cron expression.

  Returns `{:ok, job_id}` for one-time schedules or `{:ok, schedule_id}` for cron.

  ## Options

    * `:at` — `DateTime` or `NaiveDateTime` for a one-time run
    * `:in` — seconds from now for a one-time run
    * `:cron` — 5-field cron expression for recurring schedules
    * `:queue`, `:priority`, `:max_attempts` — same as `insert/3`

  Requires `config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase` for
  naive datetimes and cron.

  ## Examples

      {:ok, job_id} = Kathikon.schedule(MyApp.DigestWorker, %{}, in: 3600)

      at = DateTime.add(DateTime.utc_now(), 5, :second)
      {:ok, job_id} = Kathikon.schedule(MyApp.ReportWorker, %{}, at: at)

      {:ok, schedule_id} =
        Kathikon.schedule(MyApp.DigestWorker, %{"list" => "all"}, cron: "0 9 * * *")

  See `Kathikon.Scheduler` and `docs/scheduling.md`.
  """
  @spec schedule(module(), map(), keyword()) :: {:ok, term()} | {:error, term()}
  def schedule(worker, args, opts \\ []) do
    cond do
      Keyword.has_key?(opts, :at) ->
        Kathikon.Scheduler.schedule(worker, args, opts)

      Keyword.has_key?(opts, :in) ->
        Kathikon.Scheduler.schedule(worker, args, opts)

      Keyword.has_key?(opts, :cron) ->
        Kathikon.Scheduler.schedule(worker, args, opts)

      true ->
        {:error, :missing_schedule_option}
    end
  end

  @doc """
  Atomically claims a specific job by id and transitions it to `:running`.

  Intended for external runners and management tools. Normal application code
  should let dispatchers claim jobs automatically.

  ## Options

    * `:node` — claimant node (default `node()`)
    * `:pid` — claimant pid (default `self()`)
    * `:dispatcher_id` — optional dispatcher reference

  ## Examples

      {:ok, job} = Kathikon.claim(job_id)
      # perform work, then complete via storage callbacks or worker return

      {:error, :not_claimable} = Kathikon.claim(already_running_id)
  """
  @spec claim(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def claim(job_id, opts \\ []) do
    claimant = build_claimant(opts)

    with {:ok, claimed} <- Storage.claim_job(job_id, claimant),
         {:ok, running} <- Storage.start_job(claimed, claimant) do
      Telemetry.event([:job, :claimed], %{}, metadata(running))
      Telemetry.event([:job, :started], %{}, metadata(running))
      {:ok, running}
    end
  end

  @doc """
  Atomically claims up to `:limit` available jobs from a queue (default 1).

  Each returned job is already in `:running` state.

  ## Examples

      {:ok, [job]} = Kathikon.claim_available(:imports)
      {:ok, jobs} = Kathikon.claim_available(:imports, limit: 5)
  """
  @spec claim_available(atom(), keyword()) :: {:ok, [Job.t()]} | {:error, term()}
  def claim_available(queue, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1)
    claimant = build_claimant(opts)

    with {:ok, running} <- Storage.claim_and_start_available_jobs(queue, limit, claimant) do
      for job <- running do
        Telemetry.event([:job, :claimed], %{}, metadata(job))
        Telemetry.event([:job, :started], %{}, metadata(job))
      end

      {:ok, running}
    end
  end

  @doc """
  Returns durable history events for a job.

  ## Examples

      {:ok, events} = Kathikon.history(job_id)

      Enum.map(events, & &1.event)
      #=> [:inserted, :claimed, :started, :completed]
  """
  @spec history(String.t()) :: {:ok, [map()]} | {:error, term()}
  def history(job_id), do: Storage.list_history(job_id)

  @doc """
  Returns job status suitable for dashboards.

  ## Examples

      {:ok, status} = Kathikon.status(job_id)
      status.state
      #=> :completed

      status.history_count
      #=> 4
  """
  @spec status(String.t()) :: {:ok, map()} | {:error, term()}
  def status(job_id) do
    with {:ok, job} <- Storage.fetch(job_id),
         {:ok, events} <- Storage.list_history(job_id) do
      {:ok, %{job: Job.to_map(job), history_count: length(events), state: job.state}}
    end
  end

  @doc """
  Cancels a pending job.

  Only jobs in cancellable states (`:scheduled`, `:available`, `:retryable`,
  `:claimed`) succeed. Running jobs return `{:error, :executing}`.

  ## Examples

      {:ok, job} = Kathikon.cancel(job_id)
      job.state
      #=> :cancelled

      {:ok, job} = Kathikon.cancel(job_id, :user_requested)

      {:error, :executing} = Kathikon.cancel(running_job_id)
  """
  @spec cancel(String.t(), term()) :: {:ok, Job.t()} | {:error, term()}
  def cancel(job_id, reason \\ nil) do
    case Storage.cancel_job(job_id, reason, %{}) do
      {:error, :running} ->
        {:error, :executing}

      other ->
        tap(other, fn
          {:ok, job} -> Telemetry.event([:job, :cancel], %{}, metadata(job))
          _ -> :ok
        end)
    end
  end

  @doc """
  Retries a retryable, failed, or dead job in place.

  Resets availability and increments nothing on the original record — the same
  job id is retried.

  ## Examples

      {:ok, job} = Kathikon.retry(retryable_job_id)
      job.state
      #=> :available
  """
  @spec retry(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def retry(job_id, opts \\ []) do
    Storage.retry_job(job_id, opts)
    |> tap(fn
      {:ok, job} -> Telemetry.event([:job, :retried], %{}, metadata(job))
      _ -> :ok
    end)
  end

  @doc """
  Creates a linked rerun of a job (original is unchanged).

  The new job references the original via `rerun_of` and `original_job_id`.

  ## Examples

      {:ok, original} = Kathikon.fetch(dead_job_id)
      {:ok, rerun} = Kathikon.rerun(original.id)
      rerun.rerun_of
      #=> original.id
  """
  @spec rerun(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def rerun(job_id, opts \\ []) do
    with {:ok, original} <- Storage.fetch(job_id) do
      rerun_opts =
        opts
        |> Keyword.merge(
          queue: Keyword.get(opts, :queue, original.queue),
          rerun_of: job_id,
          original_job_id: original.original_job_id || job_id
        )

      insert(original.worker, original.args, rerun_opts)
      |> tap(fn
        {:ok, job} ->
          Storage.insert_history_event(job_id, %{
            id: random_id(),
            job_id: job_id,
            event: :rerun_requested,
            from_state: original.state,
            to_state: job.state,
            metadata: %{rerun_job_id: job.id},
            inserted_at: DateTime.utc_now()
          })

        _ ->
          :ok
      end)
    end
  end

  @doc """
  Lists dead-letter jobs.

  ## Options

    * `:queue` — filter by queue

  ## Examples

      {:ok, dead} = Kathikon.dead_jobs()
      {:ok, dead} = Kathikon.dead_jobs(queue: :default)
  """
  @spec dead_jobs(keyword()) :: {:ok, [Job.t()]} | {:error, term()}
  def dead_jobs(opts \\ []), do: Storage.list_dead_jobs(opts)

  @doc """
  Retries a dead-letter job by creating a linked rerun.

  Alias for `rerun/2`.

  ## Examples

      {:ok, job} = Kathikon.retry_dead(dead_job_id)
  """
  @spec retry_dead(String.t(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def retry_dead(job_id, opts \\ []), do: rerun(job_id, opts)

  @doc """
  Discards a dead-letter job permanently.

  ## Examples

      {:ok, job} = Kathikon.discard_dead(dead_job_id)
      job.state
      #=> :discarded

      {:ok, job} = Kathikon.discard_dead(dead_job_id, :no_longer_needed)
  """
  @spec discard_dead(String.t(), term()) :: {:ok, Job.t()} | {:error, term()}
  def discard_dead(job_id, reason \\ nil),
    do: Storage.discard_job(job_id, reason || :dead_discarded, %{})

  @doc """
  Pauses a queue — dispatchers stop claiming new jobs.

  ## Examples

      :ok = Kathikon.pause_queue(:emails)
  """
  @spec pause_queue(atom()) :: :ok
  def pause_queue(queue), do: QueueControl.pause(queue)

  @doc """
  Resumes a paused queue.

  ## Examples

      :ok = Kathikon.resume_queue(:emails)
  """
  @spec resume_queue(atom()) :: :ok
  def resume_queue(queue), do: QueueControl.resume(queue)

  @doc """
  Returns pause status for a queue.

  ## Examples

      %{queue: :default, paused: false} = Kathikon.queue_status(:default)
  """
  @spec queue_status(atom()) :: map()
  def queue_status(queue), do: QueueControl.status(queue)

  @doc """
  Returns child jobs for a parent or batch id.

  ## Examples

      {:ok, children} = Kathikon.children(parent_job_id)
      Enum.map(children, & &1.state)
  """
  @spec children(String.t()) :: {:ok, [Job.t()]} | {:error, term()}
  def children(job_id) do
    with {:ok, jobs} <- Storage.list_jobs([]) do
      {:ok,
       Enum.filter(jobs, fn job -> job.parent_job_id == job_id or job.batch_id == job_id end)}
    end
  end

  @doc """
  Returns the persisted result for a completed job.

  Requires `config :kathikon, result: :store` (the default).

  ## Examples

      {:ok, result} = Kathikon.result(completed_job_id)

      {:error, {:invalid_state, :running}} = Kathikon.result(running_job_id)
  """
  @spec result(String.t()) :: {:ok, term()} | {:error, term()}
  def result(job_id) do
    with {:ok, job} <- Storage.fetch(job_id) do
      if job.state == :completed,
        do: {:ok, job.result},
        else: {:error, {:invalid_state, job.state}}
    end
  end

  @doc """
  Returns errors recorded for a job.

  ## Examples

      {:ok, errors} = Kathikon.errors(job_id)
      List.last(errors)
      #=> %{"at" => ~U[...], "error" => "...", "attempt" => 1}
  """
  @spec errors(String.t()) :: {:ok, [map()]} | {:error, term()}
  def errors(job_id) do
    with {:ok, job} <- Storage.fetch(job_id) do
      {:ok, job.errors}
    end
  end

  @doc """
  Fetches a job by id.

  ## Examples

      {:ok, job} = Kathikon.fetch(job_id)
      job.state

      {:error, :not_found} = Kathikon.fetch("missing")
  """
  @spec fetch(String.t()) :: {:ok, Job.t()} | {:error, term()}
  def fetch(job_id), do: Storage.fetch(job_id)

  @doc """
  Lists all jobs currently in storage.

  Intended for tests, debugging, and reporting — not for hot paths in production.

  ## Examples

      Kathikon.all()
      |> Enum.count(&(&1.state == :completed))
  """
  @spec all() :: [Job.t()]
  def all, do: Storage.all()

  @doc """
  Ensures a dispatcher is running for the given queue.

  Called automatically by `insert/3` and `schedule/3`. Use when starting
  queues dynamically.

  ## Examples

      :ok = Kathikon.start_queue(:imports)
  """
  @spec start_queue(atom()) :: :ok
  def start_queue(queue) when is_atom(queue), do: Queue.ensure_started(queue)

  defp build_claimant(opts) do
    %{
      node: Keyword.get(opts, :node, node()),
      pid: inspect(Keyword.get(opts, :pid, self())),
      claimed_at: DateTime.utc_now(),
      dispatcher_id: Keyword.get(opts, :dispatcher_id, self())
    }
  end

  defp metadata(%Job{} = job, extra \\ []) do
    Enum.into(extra, %{
      queue: job.queue,
      job_id: job.id,
      worker: job.worker,
      state: job.state,
      attempt: job.attempts,
      node: job.node || node()
    })
  end

  defp random_id do
    Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end

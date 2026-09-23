defmodule Kathikon.Storage.Mnesia do
  @moduledoc """
  Mnesia implementation of `Kathikon.Storage`.

  Tables:

    * `:kathikon_jobs` — job payloads
    * `:kathikon_history` — durable lifecycle events
    * `:kathikon_batches` — batch coordination records
    * `:kathikon_schedules` — built-in scheduler registrations

  All state transitions use Mnesia transactions.
  """

  @behaviour Kathikon.Storage

  alias Kathikon.{History, Job, Job.StateMachine}
  alias Kathikon.Storage.Mnesia.Context

  @dialyzer {:no_return, abort: 1}

  @tables [:kathikon_jobs, :kathikon_history, :kathikon_batches, :kathikon_schedules]
  @jobs :kathikon_jobs
  @history :kathikon_history
  @batches :kathikon_batches
  @schedules :kathikon_schedules

  @impl true
  def setup do
    _ = ensure_schema()
    _ = ensure_tables()
    :ok
  end

  @impl true
  def clear_jobs! do
    Enum.each([@jobs, @history, @batches, @schedules], fn table ->
      if table_exists?(table), do: _ = :mnesia.clear_table(table)
    end)

    :ok
  end

  @impl true
  def reset! do
    copies = Kathikon.Config.mnesia_copies()

    with_mnesia_for_reset(fn -> delete_existing_tables() end)

    if copies == :disc do
      _ = stop_mnesia()
      _ = delete_schema_if_present()
    end

    setup()
  end

  @impl true
  def insert(%Job{} = job) do
    insert_job(job)
    |> case do
      {:ok, id} -> fetch(id)
      other -> other
    end
  end

  @impl true
  def insert_job(%Job{} = job) do
    transaction(fn ->
      case read_job(job.id) do
        nil ->
          job = Job.normalize(job)

          _ =
            commit_job_with_history!(
              job,
              job.id,
              :inserted,
              nil,
              job.state,
              %{queue: job.queue}
            )

          job.id

        _ ->
          abort({:already_exists, job.id})
      end
    end)
    |> normalize_transaction()
  end

  def insert_job(job) when is_map(job) do
    job
    |> map_to_job()
    |> insert_job()
  end

  @impl true
  def update(%Job{} = job) do
    transaction(fn ->
      case read_job(job.id) do
        nil -> abort({:not_found, job.id})
        _ -> _ = write_job(Job.normalize(job))
      end
    end)
    |> normalize_transaction()
  end

  @impl true
  def update_job(id, changes) when is_map(changes) do
    transaction(fn ->
      case read_job(id) do
        nil -> abort(:not_found)
        job -> apply_job_changes(job, changes)
      end
    end)
    |> normalize_transaction()
  end

  @impl true
  def fetch(id), do: get_job(id)

  @impl true
  def get_job(id) do
    transaction(fn ->
      case read_job(id) do
        nil -> abort(:not_found)
        job -> job
      end
    end)
    |> normalize_transaction()
  end

  @impl true
  def claim(queue, now) do
    claimant = default_claimant()

    case claim_available_jobs(queue, 1, claimant) do
      {:ok, [job | _]} ->
        case start_job(job, claimant, now) do
          {:ok, running} -> {:ok, running}
          {:error, reason} -> {:error, reason}
        end

      {:ok, []} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def claim_job(job_id, claimant) do
    now = DateTime.utc_now()

    transaction(fn ->
      case read_job(job_id) do
        nil -> abort(:not_found)
        %{state: :claimed} = job -> resolve_claimed_job(job, claimant)
        job -> claim_available_job(job_id, job, claimant, now)
      end
    end)
    |> normalize_claim_transaction()
  end

  @impl true
  def claim_available_jobs(queue, limit, claimant) when limit > 0 do
    now = DateTime.utc_now()

    transaction(fn ->
      all_jobs()
      |> Enum.filter(fn job -> job.queue == queue and Job.claimable?(job, now) end)
      |> sort_claimable()
      |> Enum.take(limit)
      |> Enum.map(fn job ->
        write_claimed_job(job.id, job, claimant, now)
      end)
    end)
    |> normalize_transaction()
  end

  @doc """
  Atomically claims and starts up to `limit` jobs from `queue`.

  Returns running jobs so callers never observe a stranded `:claimed` state.
  """
  @spec claim_and_start_available_jobs(atom(), pos_integer(), map()) ::
          {:ok, [Job.t()]} | {:error, term()}
  @impl true
  def claim_and_start_available_jobs(queue, limit, claimant) when limit > 0 do
    now = DateTime.utc_now()

    transaction(fn ->
      all_jobs()
      |> Enum.filter(fn job -> job.queue == queue and Job.claimable?(job, now) end)
      |> sort_claimable()
      |> Enum.take(limit)
      |> Enum.map(fn job ->
        claimed = write_claimed_job(job.id, job, claimant, now)
        start_claimed_job!(claimed, claimant, now)
      end)
    end)
    |> normalize_transaction()
  end

  @doc """
  Defers a running job to `:scheduled` with a durable history event.
  """
  @spec defer_job(String.t(), DateTime.t(), map()) :: {:ok, Job.t()} | {:error, term()}
  @impl true
  def defer_job(job_id, scheduled_at, metadata) do
    transaction(fn ->
      job = fetch_job!(job_id)
      from = job.state

      if from != :running do
        abort({:invalid_state, from})
      end

      deferred =
        job
        |> Map.merge(%{
          state: :scheduled,
          scheduled_at: scheduled_at,
          available_at: scheduled_at,
          started_at: nil,
          claimed_at: nil,
          claimant: nil
        })
        |> Job.normalize()

      :ok = transition!(from, :scheduled)

      commit_job_with_history!(
        deferred,
        job_id,
        :deferred,
        from,
        :scheduled,
        metadata
      )
    end)
    |> normalize_transaction()
  end

  @impl true
  def complete_job(job_id, result, metadata) do
    now = DateTime.utc_now()

    transaction(fn ->
      job = fetch_job!(job_id)
      from = job.state

      if from not in [:running, :waiting_for_children] do
        abort({:invalid_state, from})
      end

      stored_result = if job.result_mode == :store, do: result, else: nil

      completed =
        job
        |> Map.merge(%{
          state: :completed,
          result: stored_result,
          completed_at: now,
          attempts: Map.get(metadata, :attempt, job.attempts)
        })
        |> Job.normalize()

      :ok = transition!(from, :completed)

      commit_job_with_history!(completed, job_id, :completed, from, :completed, metadata)
    end)
    |> normalize_transaction()
  end

  @impl true
  def fail_job(job_id, error, metadata) do
    now = DateTime.utc_now()
    reason = inspect_error(error)

    transaction(fn ->
      job = fetch_job!(job_id)
      from = job.state
      attempt = Map.get(metadata, :attempt, job.attempts + 1)
      error_entry = %{at: DateTime.to_iso8601(now), attempt: attempt, reason: reason}
      errors = job.errors ++ [error_entry]

      cond do
        from not in [:running, :waiting_for_children] ->
          abort({:invalid_state, from})

        attempt >= job.max_attempts ->
          failed =
            job
            |> Map.merge(%{
              state: :failed,
              attempts: attempt,
              last_error: reason,
              error: reason,
              failed_at: now,
              errors: errors
            })
            |> Job.normalize()

          :ok = transition!(from, :failed)

          _ =
            commit_job_with_history!(
              failed,
              job_id,
              :failed,
              from,
              :failed,
              Map.put(metadata, :reason, reason)
            )

          dead =
            failed
            |> Map.merge(%{state: :dead})
            |> Job.normalize()

          :ok = transition!(:failed, :dead)

          commit_job_with_history!(dead, job_id, :moved_to_dead, :failed, :dead, metadata)

        true ->
          backoff = Job.backoff_seconds(attempt)
          available_at = DateTime.add(now, backoff, :second)

          retryable =
            job
            |> Map.merge(%{
              state: :retryable,
              attempts: attempt,
              last_error: reason,
              error: reason,
              available_at: available_at,
              started_at: nil,
              claimed_at: nil,
              claimant: nil,
              errors: errors
            })
            |> Job.normalize()

          :ok = transition!(from, :retryable)

          commit_job_with_history!(
            retryable,
            job_id,
            :failed,
            from,
            :retryable,
            Map.put(metadata, :backoff, backoff)
          )
      end
    end)
    |> normalize_transaction()
  end

  @impl true
  def retry_job(job_id, opts \\ []) do
    now = DateTime.utc_now()
    schedule_in = Keyword.get(opts, :in, 0)
    available_at = DateTime.add(now, schedule_in, :second)

    transaction(fn ->
      job = fetch_job!(job_id)

      unless job.state in [:retryable, :failed, :dead] do
        abort({:invalid_state, job.state})
      end

      target = if schedule_in > 0, do: :scheduled, else: :available

      retried =
        job
        |> Map.merge(%{
          state: target,
          scheduled_at: if(target == :scheduled, do: available_at),
          available_at: available_at,
          failed_at: nil,
          started_at: nil,
          claimed_at: nil,
          claimant: nil
        })
        |> Job.normalize()

      :ok = transition!(job.state, target)

      commit_job_with_history!(
        retried,
        job_id,
        :retry_scheduled,
        job.state,
        target,
        %{in: schedule_in}
      )
    end)
    |> normalize_transaction()
  end

  @impl true
  def discard_job(job_id, reason, metadata) do
    now = DateTime.utc_now()

    transaction(fn ->
      job_id
      |> fetch_job!()
      |> discard_job_record(job_id, reason, metadata, now)
    end)
    |> normalize_transaction()
  end

  @impl true
  def cancel_job(job_id, reason, metadata) do
    now = DateTime.utc_now()

    transaction(fn ->
      job = fetch_job!(job_id)

      if job.state in [:completed, :cancelled, :discarded, :dead] do
        abort({:invalid_state, job.state})
      end

      if job.state == :running do
        abort(:running)
      end

      :ok = transition!(job.state, :cancelled)

      cancelled =
        job
        |> Map.merge(%{state: :cancelled, cancelled_at: now})
        |> Job.normalize()

      commit_job_with_history!(
        cancelled,
        job_id,
        :cancelled,
        job.state,
        :cancelled,
        Map.put(metadata, :reason, reason)
      )
    end)
    |> normalize_transaction()
  end

  @impl true
  def move_to_dead_letter(job_id, reason, metadata) do
    transaction(fn ->
      job = fetch_job!(job_id)

      unless job.state in [:failed, :running, :retryable] do
        abort({:invalid_state, job.state})
      end

      from = job.state
      if from != :failed, do: :ok = transition!(from, :failed)
      :ok = transition!(:failed, :dead)

      dead =
        job
        |> Map.merge(%{
          state: :dead,
          last_error: inspect_error(reason),
          failed_at: DateTime.utc_now()
        })
        |> Job.normalize()

      commit_job_with_history!(
        dead,
        job_id,
        :moved_to_dead,
        from,
        :dead,
        Map.put(metadata, :reason, reason)
      )
    end)
    |> normalize_transaction()
  end

  @impl true
  def list_jobs(opts \\ []) do
    queue = Keyword.get(opts, :queue)
    state = Keyword.get(opts, :state)

    transaction(fn ->
      all_jobs()
      |> Enum.filter(fn job ->
        (is_nil(queue) or job.queue == queue) and (is_nil(state) or job.state == state)
      end)
    end)
    |> normalize_transaction()
  end

  @impl true
  def list_jobs_page(opts \\ []) do
    queue = Keyword.get(opts, :queue)
    states = Keyword.get(opts, :states)
    limit = Keyword.get(opts, :limit, 50)
    offset = Keyword.get(opts, :offset, 0)
    order = Keyword.get(opts, :order, :newest)

    transaction(fn ->
      jobs =
        all_jobs()
        |> Enum.filter(fn job ->
          (is_nil(queue) or job.queue == queue) and
            (is_nil(states) or job.state in states)
        end)
        |> sort_jobs_for_page(order)

      %{jobs: jobs |> Enum.drop(offset) |> Enum.take(limit), total: length(jobs)}
    end)
    |> normalize_transaction()
  end

  defp sort_jobs_for_page(jobs, :newest) do
    Enum.sort_by(jobs, &job_page_sort_time/1, {:desc, DateTime})
  end

  defp sort_jobs_for_page(jobs, :oldest) do
    Enum.sort_by(jobs, &job_page_sort_time/1, DateTime)
  end

  defp job_page_sort_time(job) do
    job.inserted_at || job.available_at || DateTime.utc_now()
  end

  @impl true
  def list_dead_jobs(opts \\ []) do
    list_jobs(Keyword.put(opts, :state, :dead))
  end

  @impl true
  def insert_history_event(_job_id, event) do
    case transaction(fn ->
           write_history_record(event)
           :ok
         end) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl true
  def list_history(job_id) do
    transaction(fn ->
      :mnesia.select(@history, [
        {
          {:"$1", :"$2", :"$3", :"$4"},
          [{:==, :"$3", job_id}],
          [:"$4"]
        }
      ])
      |> Enum.map(&decode_term/1)
      |> Enum.sort_by(& &1.inserted_at, DateTime)
    end)
    |> normalize_transaction()
  end

  @impl true
  def promote_scheduled(now) do
    if table_exists?(@jobs) do
      transaction(fn ->
        all_jobs()
        |> Enum.filter(&scheduled_due?(&1, now))
        |> Enum.map(&promote_job!(&1, now))
        |> length()
      end)
      |> promote_scheduled_result()
    else
      0
    end
  end

  defp scheduled_due?(job, now) do
    (job.state == :scheduled and job.scheduled_at) &&
      DateTime.compare(job.scheduled_at, now) != :gt
  end

  defp promote_job!(job, now) do
    updated = %{job | state: :available, available_at: now} |> Job.normalize()
    :ok = transition!(:scheduled, :available)

    commit_job_with_history!(updated, job.id, :scheduled, :scheduled, :available, %{})
  end

  @impl true
  def prunable_jobs(cutoff) do
    transaction(fn ->
      all_jobs()
      |> Enum.filter(fn job ->
        job.state in [:completed, :cancelled, :discarded] and prunable?(job, cutoff)
      end)
    end)
    |> elem(1)
  end

  @impl true
  def delete(id) do
    _ =
      transaction(fn ->
        _ = :mnesia.delete({@jobs, id})
        _ = delete_history_for_job(id)
      end)

    :ok
  end

  @impl true
  def all, do: transaction(fn -> all_jobs() end) |> elem(1)

  @impl true
  def start_job(%Job{} = job, claimant, now \\ DateTime.utc_now()) do
    do_start_job(job.id, claimant, now)
  end

  defp do_start_job(job_id, claimant, now) do
    transaction(fn ->
      current = fetch_job!(job_id)

      case current.state do
        :claimed ->
          start_claimed_job!(current, claimant, now)

        other ->
          abort({:invalid_state, other})
      end
    end)
    |> normalize_transaction()
  end

  defp start_claimed_job!(current, claimant, now) do
    :ok = transition!(:claimed, :running)

    running =
      current
      |> Map.merge(%{state: :running, started_at: now, node: Map.get(claimant, :node, node())})
      |> Job.normalize()

    commit_job_with_history!(running, current.id, :started, :claimed, :running, claimant)
  end

  @doc false
  def write_batch(batch) do
    transaction(fn ->
      :mnesia.write({@batches, batch.batch_id, :erlang.term_to_binary(batch)})
      batch
    end)
    |> normalize_transaction()
  end

  @doc false
  def fetch_batch(batch_id) do
    transaction(fn ->
      case :mnesia.read(@batches, batch_id) do
        [{_, _, binary}] -> decode_term(binary)
        [] -> abort(:not_found)
      end
    end)
    |> normalize_transaction()
  end

  @doc """
  Atomically transitions a parent to `:waiting_for_children`, inserts child jobs,
  and writes the batch record.
  """
  @spec start_batch(String.t(), [Job.t()], map()) :: {:ok, map()} | {:error, term()}
  def start_batch(parent_job_id, child_jobs, batch_attrs) when is_list(child_jobs) do
    transaction(fn ->
      parent = fetch_job!(parent_job_id)

      unless parent.state == :running do
        abort({:invalid_state, parent.state})
      end

      batch_id = Map.fetch!(batch_attrs, :batch_id)

      parent =
        parent
        |> Map.merge(%{state: :waiting_for_children, batch_id: batch_id})
        |> Job.normalize()

      :ok = transition!(:running, :waiting_for_children)

      _ =
        commit_job_with_history!(
          parent,
          parent_job_id,
          :child_created,
          :running,
          :waiting_for_children,
          %{batch_id: batch_id, child_count: length(child_jobs)}
        )

      child_ids =
        Enum.map(child_jobs, fn job ->
          insert_batch_child!(Job.normalize(job), parent_job_id, batch_id)
        end)

      batch =
        Map.merge(batch_attrs, %{
          batch_id: batch_id,
          parent_job_id: parent_job_id,
          child_job_ids: child_ids,
          pending_count: length(child_ids)
        })

      :mnesia.write({@batches, batch_id, :erlang.term_to_binary(batch)})

      _ =
        commit_job_with_history!(
          parent,
          parent_job_id,
          :batch_started,
          :running,
          :waiting_for_children,
          %{batch_id: batch_id, child_count: length(child_ids)}
        )

      batch
    end)
    |> normalize_transaction()
  end

  @doc """
  Atomically updates batch counters for a finished child and returns completion intent.
  """
  @spec record_batch_child_finished(Job.t()) ::
          {:ok, :pending, map()}
          | {:ok, :complete, map(), Job.t()}
          | {:ok, :fail, map(), Job.t()}
          | {:error, term()}
  def record_batch_child_finished(%Job{batch_id: batch_id} = child) when not is_nil(batch_id) do
    transaction(fn ->
      batch = fetch_batch!(batch_id)

      if batch.status != :running or batch.pending_count <= 0 do
        abort(:already_finished)
      end

      parent = fetch_job!(batch.parent_job_id)
      batch = apply_child_to_batch(batch, child)

      :mnesia.write({@batches, batch.batch_id, :erlang.term_to_binary(batch)})

      cond do
        batch.pending_count > 0 ->
          {:pending, batch}

        batch_succeeded?(batch) ->
          {:complete, batch, parent}

        true ->
          {:fail, batch, parent}
      end
    end)
    |> normalize_batch_child_transaction()
  end

  def record_batch_child_finished(_), do: {:ok, :ignored}

  @doc false
  def list_batches do
    transaction(fn ->
      :mnesia.select(@batches, [{{:"$1", :_, :"$2"}, [], [:"$2"]}])
      |> Enum.map(&decode_term/1)
    end)
    |> elem(1)
  end

  @doc false
  def write_schedule(schedule) do
    transaction(fn ->
      :mnesia.write({@schedules, schedule.id, :erlang.term_to_binary(schedule)})
      schedule
    end)
    |> normalize_transaction()
  end

  @doc false
  def fetch_schedule(id) do
    transaction(fn ->
      case :mnesia.read(@schedules, id) do
        [{_, _, binary}] -> decode_term(binary)
        [] -> abort(:not_found)
      end
    end)
    |> normalize_transaction()
  end

  @doc false
  def delete_schedule(id) do
    _ = transaction(fn -> _ = :mnesia.delete({@schedules, id}) end)
    :ok
  end

  @doc false
  def list_schedules do
    transaction(fn ->
      :mnesia.select(@schedules, [{{:"$1", :_, :"$2"}, [], [:"$2"]}])
      |> Enum.map(&decode_term/1)
    end)
    |> elem(1)
  end

  defp insert_batch_child!(job, parent_job_id, batch_id) do
    case read_job(job.id) do
      nil ->
        _ =
          commit_job_with_history!(
            job,
            job.id,
            :inserted,
            nil,
            job.state,
            %{queue: job.queue, parent_job_id: parent_job_id, batch_id: batch_id}
          )

        _ =
          commit_job_with_history!(
            job,
            job.id,
            :child_created,
            nil,
            job.state,
            %{parent_job_id: parent_job_id, batch_id: batch_id}
          )

        job.id

      _ ->
        abort({:already_exists, job.id})
    end
  end

  defp fetch_job!(id) do
    case read_job(id) do
      nil -> abort(:not_found)
      job -> job
    end
  end

  defp fetch_batch!(batch_id) do
    case :mnesia.read(@batches, batch_id) do
      [{_, _, binary}] -> decode_term(binary)
      [] -> abort(:not_found)
    end
  end

  defp apply_child_to_batch(batch, child) do
    case child.state do
      :completed ->
        %{
          batch
          | pending_count: batch.pending_count - 1,
            success_count: batch.success_count + 1
        }

      state when state in [:failed, :dead, :discarded] ->
        %{
          batch
          | pending_count: batch.pending_count - 1,
            failure_count: batch.failure_count + 1
        }

      :cancelled ->
        %{
          batch
          | pending_count: batch.pending_count - 1,
            cancelled_count: batch.cancelled_count + 1
        }

      _ ->
        batch
    end
  end

  defp batch_succeeded?(%{success_policy: :all_succeeded, failure_count: 0, cancelled_count: 0}),
    do: true

  defp batch_succeeded?(%{success_policy: :allow_partial, success_count: s}) when s > 0, do: true

  defp batch_succeeded?(%{success_policy: {:at_least, n}, success_count: s}), do: s >= n
  defp batch_succeeded?(_), do: false

  defp normalize_batch_child_transaction({:atomic, {:pending, batch}}),
    do: {:ok, :pending, batch}

  defp normalize_batch_child_transaction({:atomic, {:complete, batch, parent}}),
    do: {:ok, :complete, batch, parent}

  defp normalize_batch_child_transaction({:atomic, {:fail, batch, parent}}),
    do: {:ok, :fail, batch, parent}

  defp normalize_batch_child_transaction({:aborted, :not_found}), do: {:error, :not_found}

  defp normalize_batch_child_transaction({:aborted, :already_finished}),
    do: {:ok, :already_finished}

  defp normalize_batch_child_transaction({:aborted, reason}), do: {:error, reason}

  defp read_job(id) do
    case :mnesia.read(@jobs, id) do
      [{_, _, binary}] -> Job.from_record({@jobs, id, binary})
      [] -> nil
    end
  end

  defp write_job(%Job{} = job) do
    :mnesia.write(Job.to_record(job))
    job
  end

  defp commit_job_with_history!(job, job_id, event, from_state, to_state, metadata) do
    _ = write_job(job)
    _ = write_history_event(job_id, event, from_state, to_state, metadata)
    job
  end

  defp write_history_event(job_id, event, from_state, to_state, metadata) do
    History.build_event(job_id, event, from_state, to_state, metadata)
    |> write_history_record()
  end

  defp write_history_record(event) do
    :mnesia.write({@history, event.id, event.job_id, :erlang.term_to_binary(event)})
    :ok
  end

  defp delete_history_for_job(job_id) do
    :mnesia.select(@history, [{{:"$1", :"$2", :"$3", :_}, [{:==, :"$3", job_id}], [:"$2"]}])
    |> Enum.each(fn id -> _ = :mnesia.delete({@history, id}) end)

    :ok
  end

  defp transition!(from, to) do
    case StateMachine.transition(from, to) do
      :ok -> :ok
      {:error, reason} -> abort(reason)
    end
  end

  defp same_claimant?(job, claimant) do
    job.claimant && job.claimant[:dispatcher_id] == claimant[:dispatcher_id]
  end

  defp apply_job_changes(job, changes) do
    changes = normalize_changes(changes)
    new_state = Map.get(changes, :state, job.state)

    if new_state != job.state do
      :ok = transition!(job.state, new_state)
    end

    updated = struct(job, Map.to_list(changes)) |> Job.normalize()
    _ = write_job(updated)
    updated
  end

  defp resolve_claimed_job(job, claimant) do
    if same_claimant?(job, claimant), do: job, else: abort(:already_claimed)
  end

  defp claim_available_job(job_id, job, claimant, now) do
    if Job.claimable?(job, now) do
      write_claimed_job(job_id, job, claimant, now)
    else
      abort(:not_claimable)
    end
  end

  defp write_claimed_job(job_id, job, claimant, now) do
    claimed =
      job
      |> Map.merge(%{
        state: :claimed,
        claimed_at: now,
        claimant: claimant,
        node: Map.get(claimant, :node, node())
      })
      |> Job.normalize()

    :ok = transition!(job.state, :claimed)
    commit_job_with_history!(claimed, job_id, :claimed, job.state, :claimed, claimant)
  end

  defp discard_job_record(%{state: :discarded} = job, _, _, _, _), do: job

  defp discard_job_record(job, job_id, reason, metadata, now) do
    from = job.state
    :ok = validate_discard_transition!(from)

    discarded =
      job
      |> Map.merge(%{
        state: :discarded,
        discarded_at: now,
        last_error: inspect_error(reason),
        completed_at: now
      })
      |> Job.normalize()

    commit_job_with_history!(
      discarded,
      job_id,
      :discarded,
      from,
      :discarded,
      Map.put(metadata, :reason, reason)
    )
  end

  defp validate_discard_transition!(from) do
    case from do
      :failed ->
        :ok = transition!(:failed, :discarded)

      :running ->
        :ok = transition!(:running, :discarded)

      other when other in [:retryable, :available, :scheduled, :claimed] ->
        :ok = transition!(other, :discarded)

      _ ->
        abort({:invalid_state, from})
    end
  end

  defp default_claimant do
    %{
      node: node(),
      pid: inspect(self()),
      claimed_at: DateTime.utc_now(),
      dispatcher_id: self()
    }
  end

  defp map_to_job(map) when is_map(map) do
    struct(Job, Map.to_list(map))
  end

  defp normalize_changes(changes) do
    changes
    |> Enum.map(fn
      {k, v} when is_atom(k) -> {k, v}
      {k, v} -> {String.to_existing_atom(k), v}
    end)
    |> Map.new()
  rescue
    ArgumentError -> changes
  end

  defp inspect_error(error), do: inspect(error, pretty: true, limit: 50)

  defp sort_claimable(jobs) do
    Enum.sort_by(jobs, fn job ->
      {-job.priority, DateTime.to_unix(job.available_at || job.inserted_at, :microsecond)}
    end)
  end

  defp prunable?(job, cutoff) do
    timestamp =
      job.completed_at || job.discarded_at || job.cancelled_at || job.inserted_at

    timestamp && DateTime.compare(timestamp, cutoff) != :gt
  end

  defp all_jobs do
    :mnesia.select(@jobs, [{{:"$1", :_, :"$2"}, [], [:"$2"]}])
    |> Enum.map(&Job.decode_payload/1)
  end

  defp decode_term(binary), do: :erlang.binary_to_term(binary, [:safe])

  defp transaction(fun), do: Context.transaction(fun)

  defp abort(reason), do: Context.abort(reason)

  defp normalize_transaction({:atomic, result}), do: {:ok, result}
  defp normalize_transaction({:aborted, :not_found}), do: {:error, :not_found}
  defp normalize_transaction({:aborted, reason}), do: {:error, reason}

  defp promote_scheduled_result({:atomic, count}) when is_integer(count), do: count
  defp promote_scheduled_result({:aborted, _}), do: 0

  defp normalize_claim_transaction({:atomic, job}), do: {:ok, job}
  defp normalize_claim_transaction({:aborted, :not_found}), do: {:error, :not_found}

  defp normalize_claim_transaction({:aborted, reason})
       when reason in [:already_claimed, :not_claimable],
       do: {:error, reason}

  defp normalize_claim_transaction({:aborted, reason}), do: {:error, reason}

  defp delete_existing_tables do
    Enum.each(@tables, &delete_table_if_exists/1)
  end

  defp with_mnesia_for_reset(fun) do
    case :mnesia.system_info(:is_running) do
      :yes ->
        fun.()

      :stopping ->
        _ = :mnesia.stop()
        _ = :mnesia.start()
        fun.()

      :no ->
        _ = :mnesia.start()
        fun.()
    end
  end

  defp delete_table_if_exists(table) do
    if table_exists?(table), do: safe_delete_table(table)
  end

  defp safe_delete_table(table) do
    case :mnesia.delete_table(table) do
      {:atomic, :ok} -> :ok
      {:aborted, {:no_exists, ^table}} -> :ok
      {:aborted, reason} -> raise "failed to delete mnesia table #{table}: #{inspect(reason)}"
    end
  end

  defp ensure_schema do
    case Kathikon.Config.mnesia_copies() do
      :disc -> ensure_disc_schema()
      :ram -> ensure_running()
    end
  end

  defp ensure_disc_schema do
    _ = stop_mnesia()

    case :mnesia.create_schema([node()]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
      {:error, {:already_exists, _}} -> :ok
      other -> raise "failed to create mnesia schema: #{inspect(other)}"
    end

    ensure_running()
  end

  defp stop_mnesia do
    case :mnesia.system_info(:is_running) do
      :yes -> _ = :mnesia.stop()
      :stopping -> _ = :mnesia.stop()
      :no -> :ok
    end
  end

  defp delete_schema_if_present do
    case :mnesia.delete_schema([node()]) do
      :ok -> :ok
      {:error, {_, :not_exists}} -> :ok
      other -> raise "failed to delete mnesia schema: #{inspect(other)}"
    end
  end

  defp ensure_running do
    case :mnesia.system_info(:is_running) do
      :yes ->
        :ok

      :no ->
        _ = :mnesia.start()

      :stopping ->
        _ = :mnesia.stop()
        _ = :mnesia.start()
    end
  end

  defp ensure_tables do
    create_table(@jobs, [:id, :payload])
    create_table(@history, [:id, :job_id, :payload])
    create_table(@batches, [:batch_id, :payload])
    create_table(@schedules, [:id, :payload])

    case :mnesia.wait_for_tables(@tables, 5_000) do
      :ok -> :ok
      {:timeout, tables} -> raise "timed out waiting for mnesia tables: #{inspect(tables)}"
      {:error, reason} -> raise "mnesia table error: #{inspect(reason)}"
    end
  end

  defp create_table(table, attributes) do
    opts = [attributes: attributes, type: :ordered_set] ++ storage_opts()
    ensure_table(table, opts)
  end

  defp ensure_table(table, opts) do
    if table_exists?(table) and copy_mismatch?(table), do: safe_delete_table(table)
    create_if_missing(table, opts)
  end

  defp copy_mismatch?(table) do
    case Kathikon.Config.mnesia_copies() do
      :ram -> node() not in :mnesia.table_info(table, :ram_copies)
      :disc -> node() not in :mnesia.table_info(table, :disc_copies)
    end
  end

  defp storage_opts do
    case Kathikon.Config.mnesia_copies() do
      :ram -> [ram_copies: [node()]]
      :disc -> [disc_copies: [node()]]
    end
  end

  defp create_if_missing(table, opts) do
    if table_exists?(table) do
      :ok
    else
      case :mnesia.create_table(table, opts) do
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, ^table, _}} -> :ok
        {:aborted, reason} -> raise "failed to create mnesia table #{table}: #{inspect(reason)}"
      end
    end
  end

  defp table_exists?(table) do
    :mnesia.system_info(:is_running) == :yes and table in :mnesia.system_info(:tables)
  catch
    :exit, _ -> false
  end
end

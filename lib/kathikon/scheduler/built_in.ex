defmodule Kathikon.Scheduler.BuiltIn do
  @moduledoc """
  Built-in scheduler for one-time and basic recurring jobs.

  One-time jobs are stored as Kathikon jobs with `:scheduled` state.
  Recurring cron expressions are registered in Mnesia and evaluated by
  `Kathikon.Scheduler.BuiltIn.Tick` (started with the application).

  ## Limitations

    * Recurring schedules use a simple interval tick, not sub-second precision
    * Cron parsing supports standard 5-field expressions via a minimal parser
    * For production cron, prefer `Kathikon.Scheduler.Quantum`
  """

  @behaviour Kathikon.Scheduler.Behaviour

  alias Kathikon.Cron.Expression
  alias Kathikon.{Job, Storage, Telemetry, Unique}

  @impl true
  def schedule_once(worker, args, opts) when is_atom(worker) do
    schedule_opts =
      cond do
        at = Keyword.get(opts, :at) ->
          [schedule_at: at] ++ Keyword.delete(opts, :at)

        in_seconds = Keyword.get(opts, :in) ->
          [schedule_in: in_seconds] ++ Keyword.delete(opts, :in)

        true ->
          opts
      end

    unique = Unique.resolve(worker, schedule_opts)

    with {:ok, schedule_opts} <-
           Kathikon.Timezone.normalize_opts(Keyword.delete(schedule_opts, :unique)),
         job = Job.build(worker, args, schedule_opts),
         {:ok, prepared} <- Unique.prepare(job, unique),
         :ok <- Kathikon.Queue.ensure_started(job.queue),
         {:ok, inserted} <- Storage.insert_prepared(job, prepared) do
      Telemetry.event([:scheduler, :fired], %{count: 1}, %{
        worker: worker,
        job_id: inserted.id,
        queue: inserted.queue
      })

      {:ok, inserted.id}
    end
  end

  @impl true
  def schedule_recurring(worker, args, opts) when is_atom(worker) do
    cron = Keyword.fetch!(opts, :cron)

    with {:ok, _} <- Expression.parse(cron) do
      queue = Keyword.get(opts, :queue, :default)
      id = Keyword.get(opts, :id, generate_id())

      schedule = %{
        id: id,
        worker: worker,
        args: args,
        cron: cron,
        queue: queue,
        opts: Keyword.drop(opts, [:cron, :queue, :id]),
        inserted_at: DateTime.utc_now(),
        last_fired_at: nil
      }

      with {:ok, _} <- Storage.Mnesia.write_schedule(schedule) do
        {:ok, schedule.id}
      end
    end
  end

  @impl true
  def update_schedule(schedule_id, opts) do
    with {:ok, schedule} <- fetch_schedule(schedule_id),
         :ok <- validate_cron_change(opts),
         updated <- build_updated_schedule(schedule, opts),
         {:ok, _} <- Storage.Mnesia.write_schedule(updated) do
      {:ok, updated}
    end
  end

  defp build_updated_schedule(schedule, opts) do
    cron_changed = Keyword.has_key?(opts, :cron)

    schedule
    |> apply_schedule_updates(opts)
    |> maybe_reset_last_fired(cron_changed)
  end

  @impl true
  def fetch_schedule(schedule_id) do
    Storage.Mnesia.fetch_schedule(schedule_id)
  end

  @impl true
  def cancel_schedule(schedule_id) do
    Storage.Mnesia.delete_schedule(schedule_id)
    :ok
  end

  @impl true
  def list_schedules(_opts \\ []) do
    {:ok, Storage.Mnesia.list_schedules()}
  end

  @doc false
  def fire_due_schedules(now \\ DateTime.utc_now()) do
    Storage.Mnesia.list_schedules()
    |> Enum.reduce(0, &fire_schedule_if_due(&1, &2, now))
  end

  defp fire_schedule_if_due(schedule, count, now) do
    if Expression.due?(schedule.cron, schedule.last_fired_at, now) do
      fire_schedule(schedule, count, now)
    else
      count
    end
  end

  defp fire_schedule(schedule, count, now) do
    opts = Keyword.merge(schedule.opts, queue: schedule.queue)

    case Kathikon.insert(schedule.worker, schedule.args, opts) do
      {:ok, job} ->
        mark_schedule_fired(schedule, now, job)
        count + 1

      {:error, _} ->
        count
    end
  end

  defp mark_schedule_fired(schedule, now, job) do
    updated = %{schedule | last_fired_at: now}
    {:ok, _} = Storage.Mnesia.write_schedule(updated)

    Telemetry.event([:scheduler, :fired], %{count: 1}, %{
      worker: schedule.worker,
      schedule_id: schedule.id,
      job_id: job.id
    })
  end

  defp validate_cron_change(opts) do
    case Keyword.get(opts, :cron) do
      nil -> :ok
      cron -> if match?({:ok, _}, Expression.parse(cron)), do: :ok, else: {:error, :invalid_cron}
    end
  end

  defp apply_schedule_updates(schedule, opts) do
    schedule
    |> put_if_present(:cron, opts)
    |> put_if_present(:worker, opts)
    |> put_if_present(:args, opts)
    |> put_if_present(:queue, opts)
    |> merge_schedule_opts(opts)
  end

  defp put_if_present(map, key, opts) do
    case Keyword.get(opts, key) do
      nil -> map
      value -> Map.put(map, key, value)
    end
  end

  defp merge_schedule_opts(schedule, opts) do
    extra_opts = Keyword.drop(opts, [:cron, :worker, :args, :queue, :id])
    %{schedule | opts: Keyword.merge(schedule.opts, extra_opts)}
  end

  defp maybe_reset_last_fired(schedule, true), do: %{schedule | last_fired_at: nil}
  defp maybe_reset_last_fired(schedule, false), do: schedule

  defp generate_id do
    Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end

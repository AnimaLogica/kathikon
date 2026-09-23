defmodule Kathikon.Dispatcher do
  @moduledoc """
  Claims and executes jobs for a single queue.

  Uses atomic storage claiming and lifecycle callbacks.
  """

  use GenServer

  alias Kathikon.{Batch, Job, QueueControl, Storage, Telemetry}

  @poll_message :poll

  def start_link(opts) do
    queue = Keyword.fetch!(opts, :queue)
    GenServer.start_link(__MODULE__, opts, name: via(queue))
  end

  @doc false
  def via(queue), do: {:via, Registry, {Kathikon.Registry, {:dispatcher, queue}}}

  @impl true
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)
    config = Keyword.fetch!(opts, :config)
    poll_interval = Keyword.get(opts, :poll_interval, Kathikon.Config.poll_interval())
    storage = Keyword.get(opts, :storage, Storage)

    state = %{
      queue: queue,
      config: config,
      concurrency: Keyword.get(config, :concurrency, 10),
      poll_interval: poll_interval,
      storage: storage,
      running: %{},
      dispatcher_id: self()
    }

    schedule_poll(poll_interval)
    {:ok, state}
  end

  @impl true
  def handle_info(@poll_message, state) do
    state =
      if QueueControl.paused?(state.queue) do
        state
      else
        maybe_claim_jobs(state)
      end

    schedule_poll(state.poll_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info({ref, _result}, state) when is_reference(ref) do
    {:noreply, %{state | running: Map.delete(state.running, ref)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | running: Map.delete(state.running, ref)}}
  end

  defp maybe_claim_jobs(state) do
    slots = state.concurrency - map_size(state.running)

    if slots > 0 do
      claim_and_run(state, slots)
    else
      state
    end
  end

  defp claim_and_run(state, slots) do
    claimant = build_claimant(state)

    case state.storage.claim_and_start_available_jobs(state.queue, slots, claimant) do
      {:ok, jobs} ->
        Enum.reduce(jobs, state, fn running, acc ->
          emit_claim_telemetry(state.queue, running)
          run_job(acc, running)
        end)

      {:error, _} ->
        state
    end
  end

  defp emit_claim_telemetry(queue, running) do
    Telemetry.event([:dispatcher, :poll], %{count: 1}, %{queue: queue, job_id: running.id})
    Telemetry.event([:job, :claimed], %{}, job_metadata(running))
    Telemetry.event([:job, :started], %{}, job_metadata(running))
  end

  defp build_claimant(state) do
    %{
      node: node(),
      pid: inspect(self()),
      claimed_at: DateTime.utc_now(),
      dispatcher_id: state.dispatcher_id
    }
  end

  defp run_job(state, job) do
    parent = self()

    task =
      Task.async(fn ->
        execute_job(job, parent, state.storage)
      end)

    %{state | running: Map.put(state.running, task.ref, task.pid)}
  end

  defp execute_job(job, dispatcher, storage) do
    start_time = System.monotonic_time()
    attempt = job.attempts + 1

    result =
      try do
        job.worker.perform(job)
      rescue
        exception ->
          {:error, {:exception, exception, __STACKTRACE__}}
      catch
        kind, reason ->
          {:error, {kind, reason}}
      end

    duration = System.monotonic_time() - start_time
    GenServer.cast(dispatcher, {:job_finished, job, result, duration, attempt, storage})
  end

  @impl true
  def handle_cast({:job_finished, job, result, duration, attempt, storage}, state) do
    metadata = Map.merge(job_metadata(job), %{attempt: attempt, duration: duration})
    updated = persist_job_result(storage, job, result, metadata, duration)

    case updated do
      {:ok, finished} -> Batch.handle_child_finished(finished)
      _ -> :ok
    end

    {:noreply, state}
  end

  defp persist_job_result(storage, job, result, metadata, duration) do
    case result do
      :ok ->
        storage.complete_job(job.id, :ok, metadata)
        |> emit_stop(metadata, duration)

      {:ok, value} ->
        storage.complete_job(job.id, value, metadata)
        |> emit_stop(metadata, duration)

      {:sleep, seconds} when is_integer(seconds) and seconds > 0 ->
        defer_job(storage, job, seconds, metadata, duration)

      {:sleep, invalid} ->
        storage.fail_job(job.id, {:invalid_sleep, invalid}, metadata)
        |> emit_failure(metadata, duration)

      {:discard, reason} ->
        storage.discard_job(job.id, reason, metadata)
        |> emit_discard(metadata, duration)

      {:retry, reason} ->
        storage.fail_job(job.id, reason, Map.put(metadata, :force_retry, true))
        |> emit_retry(metadata, duration)

      {:error, reason} ->
        storage.fail_job(job.id, reason, metadata)
        |> emit_failure(metadata, duration)
    end
  end

  defp defer_job(storage, job, seconds, metadata, duration) do
    at = DateTime.add(DateTime.utc_now(), seconds, :second)

    case storage.defer_job(job.id, at, Map.put(metadata, :seconds, seconds)) do
      {:ok, _} ->
        Telemetry.event([:job, :sleep], %{duration: duration}, metadata)
        nil

      {:error, reason} ->
        storage.fail_job(job.id, {:defer_failed, reason}, metadata)
        |> emit_failure(metadata, duration)
    end
  end

  defp emit_stop({:ok, job}, metadata, duration) do
    Telemetry.event(
      [:job, :completed],
      %{duration: duration},
      Map.put(metadata, :state, job.state)
    )

    Telemetry.event([:job, :stop], %{duration: duration}, Map.put(metadata, :result, :ok))
    {:ok, job}
  end

  defp emit_stop({:error, _reason}, _metadata, _duration), do: nil

  defp emit_failure({:ok, job}, metadata, duration) do
    if job.state == :dead do
      Telemetry.event([:job, :dead], %{duration: duration}, metadata)
    end

    if job.state == :retryable do
      Telemetry.event([:job, :retried], %{duration: duration}, metadata)
      Telemetry.event([:job, :retry], %{duration: duration}, metadata)
    else
      Telemetry.event([:job, :failed], %{duration: duration}, metadata)
    end

    {:ok, job}
  end

  defp emit_failure({:error, _reason}, _metadata, _duration), do: nil

  defp emit_retry(result, metadata, duration), do: emit_failure(result, metadata, duration)

  defp emit_discard({:ok, job}, metadata, duration) do
    Telemetry.event([:job, :discard], %{duration: duration}, metadata)
    {:ok, job}
  end

  defp emit_discard({:error, _reason}, _metadata, _duration), do: nil

  defp job_metadata(%Job{} = job) do
    %{
      queue: job.queue,
      job_id: job.id,
      worker: job.worker,
      state: job.state,
      attempt: job.attempts + 1,
      node: node()
    }
  end

  defp schedule_poll(interval) do
    Process.send_after(self(), @poll_message, interval)
  end
end

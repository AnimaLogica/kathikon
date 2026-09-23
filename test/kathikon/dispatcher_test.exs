defmodule Kathikon.DispatcherTest do
  use ExUnit.Case, async: false

  import Mox

  alias Kathikon.{Dispatcher, Job}

  @mock Kathikon.Storage.Mock

  setup :verify_on_exit!

  setup context do
    Kathikon.TestSupport.stub_storage_defaults!()

    queue = :"dispatcher_#{System.unique_integer([:positive])}"

    {:ok, dispatcher} =
      Dispatcher.start_link(
        queue: queue,
        config: [concurrency: 1],
        poll_interval: 60_000,
        storage: @mock
      )

    Process.unlink(dispatcher)
    Mox.allow(@mock, self(), dispatcher)

    on_exit(context, fn ->
      if Process.alive?(dispatcher) do
        try do
          GenServer.stop(dispatcher)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    %{dispatcher: dispatcher, queue: queue}
  end

  defp job(worker, attrs, opts) do
    Job.build(worker, attrs, opts)
    |> Map.put(:state, :available)
    |> Map.put(:available_at, DateTime.utc_now())
  end

  defp expect_claim_and_start(work, queue, test_pid \\ self()) do
    running = Map.put(work, :state, :running)

    Mox.expect(@mock, :claim_and_start_available_jobs, fn ^queue, 1, _claimant ->
      {:ok, [running]}
    end)

    {running, test_pid}
  end

  defp poll_and_await(dispatcher, _test_pid) do
    send(dispatcher, :poll)
    assert_receive {:dispatcher_done, _}, 500
  end

  test "executes a claimed job successfully", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.SuccessWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :complete_job, fn _id, :ok, _meta ->
      send(test_pid, {:dispatcher_done, :complete})
      {:ok, %{running | state: :completed, attempts: 1}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "retries failed jobs", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.FailWorker, %{}, queue: queue, max_attempts: 3)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn id, _reason, _meta ->
      assert id == work.id
      send(test_pid, {:dispatcher_done, :fail})
      {:ok, %{running | state: :retryable, attempts: 1}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "discards jobs after max attempts", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.FailWorker, %{}, queue: queue, max_attempts: 1)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn id, _reason, _meta ->
      assert id == work.id
      send(test_pid, {:dispatcher_done, :dead})
      {:ok, %{running | state: :dead, attempts: 1}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "handles worker exceptions", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.RaiseWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn _id, {:exception, _, _}, _meta ->
      send(test_pid, {:dispatcher_done, :exception})
      {:ok, %{running | state: :retryable, errors: [%{reason: "boom"}]}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "defers jobs via {:sleep, seconds}", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.SleepWorker, %{"seconds" => 60}, queue: queue)
    {_running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :defer_job, fn id, at, meta ->
      assert id == work.id
      assert %DateTime{} = at
      assert meta[:seconds] == 60
      send(test_pid, {:dispatcher_done, :defer})
      {:ok, Map.merge(work, %{state: :scheduled, scheduled_at: at, available_at: at})}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "treats invalid {:sleep, seconds} as failure", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.SleepWorker, %{"seconds" => 0}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn _, {:invalid_sleep, 0}, _ ->
      send(test_pid, {:dispatcher_done, :invalid_sleep})
      {:ok, %{running | state: :retryable}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "handles worker throws", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.ThrowWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn _, {:throw, :thrown}, _ ->
      send(test_pid, {:dispatcher_done, :throw})
      {:ok, %{running | state: :retryable}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "ignores a missing job when the worker finishes", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.SuccessWorker, %{}, queue: queue)
    {_running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :complete_job, fn _id, :ok, _meta ->
      send(test_pid, {:dispatcher_done, :missing})
      {:error, :not_found}
    end)

    poll_and_await(dispatcher, test_pid)
    assert :sys.get_state(dispatcher).queue == queue
  end

  test "ignores claim errors", %{dispatcher: dispatcher, queue: queue} do
    Mox.expect(@mock, :claim_and_start_available_jobs, fn ^queue, 1, _ ->
      {:error, :locked}
    end)

    send(dispatcher, :poll)
    :sys.get_state(dispatcher)
    assert Process.alive?(dispatcher)
  end

  test "skips polling while queue is paused", %{dispatcher: dispatcher, queue: queue} do
    Kathikon.QueueControl.pause(queue)

    Mox.expect(@mock, :claim_and_start_available_jobs, 0, fn _, _, _ ->
      flunk("should not claim while paused")
    end)

    send(dispatcher, :poll)
    Process.sleep(50)
    Kathikon.QueueControl.resume(queue)
  end

  test "stores worker return values", %{dispatcher: dispatcher, queue: queue} do
    work = job(Kathikon.Workers.ResultWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :complete_job, fn _id, %{value: 42}, _meta ->
      send(test_pid, {:dispatcher_done, :result})
      {:ok, %{running | state: :completed, result: %{value: 42}}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "discards jobs when worker returns {:discard, reason}", %{
    dispatcher: dispatcher,
    queue: queue
  } do
    work = job(Kathikon.Workers.DiscardWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :discard_job, fn _id, :not_wanted, _meta ->
      send(test_pid, {:dispatcher_done, :discard})
      {:ok, %{running | state: :discarded}}
    end)

    poll_and_await(dispatcher, test_pid)
  end

  test "forces retry when worker returns {:retry, reason}", %{
    dispatcher: dispatcher,
    queue: queue
  } do
    work = job(Kathikon.Workers.ForceRetryWorker, %{}, queue: queue)
    {running, test_pid} = expect_claim_and_start(work, queue)

    Mox.expect(@mock, :fail_job, fn _id, :try_again, meta ->
      assert meta[:force_retry]
      send(test_pid, {:dispatcher_done, :force_retry})
      {:ok, %{running | state: :retryable}}
    end)

    poll_and_await(dispatcher, test_pid)
  end
end

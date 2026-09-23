defmodule Kathikon.JobTest do
  use ExUnit.Case, async: true

  alias Kathikon.Job

  test "build creates an immediately available job by default" do
    job = Job.build(Kathikon.Workers.SuccessWorker, %{"x" => 1}, [])

    assert job.state == :available
    assert job.queue == :default
    assert job.priority == 0
    assert job.args == %{"x" => 1}
    assert is_binary(job.id)
  end

  test "build respects schedule_in" do
    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, schedule_in: 60)

    assert job.state == :scheduled
    assert DateTime.diff(job.scheduled_at, job.inserted_at, :second) == 60
  end

  test "build respects schedule_at in the future" do
    at = DateTime.add(DateTime.utc_now(), 120, :second)
    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, schedule_at: at)

    assert job.state == :scheduled
    assert job.scheduled_at == at
  end

  test "build uses available state for schedule_at in the past" do
    at = DateTime.add(DateTime.utc_now(), -10, :second)
    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, schedule_at: at)

    assert job.state == :available
  end

  test "backoff grows with attempts" do
    assert Job.backoff_seconds(1) == 5
    assert Job.backoff_seconds(2) == 20
    assert Job.backoff_seconds(3) == 45
  end

  test "decode_payload round-trips a job struct" do
    job = Job.build(Kathikon.Workers.SuccessWorker, %{"x" => 1}, queue: :default)
    binary = Job.to_record(job) |> elem(2)
    decoded = Job.decode_payload(binary)

    assert decoded.worker == job.worker
    assert decoded.args == job.args
    assert decoded.queue == job.queue
  end

  test "claimable? respects state and available_at" do
    now = DateTime.utc_now()
    future = DateTime.add(now, 60, :second)

    available =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, [])
      |> Map.put(:state, :available)
      |> Map.put(:available_at, now)

    scheduled =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, [])
      |> Map.put(:state, :available)
      |> Map.put(:available_at, future)

    assert Job.claimable?(available, now)
    refute Job.claimable?(scheduled, now)
  end
end

defmodule Kathikon.IntegrationTest do
  use ExUnit.Case, async: false

  alias Kathikon.TestSupport

  @queue :integration

  setup do
    TestSupport.ensure_runtime!()
    TestSupport.resume_all_queues!()
    TestSupport.stop_dispatcher(@queue)
    TestSupport.reset!()
    :ok = Kathikon.start_queue(@queue)
    :ok
  end

  @tag :async_timing
  test "insert executes a successful job" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: @queue)

    assert {:ok, completed} = TestSupport.await_state(job.id, :completed)
    assert completed.attempts == 1
  end

  @tag :async_timing
  test "insert retries failed jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.CountingWorker, %{}, queue: @queue)

    assert {:ok, completed} = TestSupport.await_state(job.id, :completed, 15_000)
    assert completed.attempts == 2
    assert length(completed.errors) == 1
  end

  @tag :async_timing
  test "insert discards after max attempts" do
    {:ok, job} =
      Kathikon.insert(Kathikon.Workers.FailWorker, %{}, queue: @queue, max_attempts: 2)

    assert {:ok, dead} = TestSupport.await_state(job.id, :dead, 15_000)
    assert dead.attempts == 2
  end

  test "cancel removes a pending job from execution" do
    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, job} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: @queue, schedule_at: future)

    assert {:ok, cancelled} = Kathikon.cancel(job.id)
    assert cancelled.state == :cancelled
  end

  @tag :async_timing
  test "sleep defers then completes without incrementing attempts" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SleepOnceWorker, %{}, queue: @queue)

    assert {:ok, completed} = TestSupport.await_state(job.id, :completed, 15_000)
    assert completed.attempts == 1
    assert completed.errors == []
  end

  @tag :async_timing
  test "higher priority jobs run first" do
    TestSupport.reset_order!()
    TestSupport.stop_dispatcher(:priority)
    past = DateTime.add(DateTime.utc_now(), -1, :second)

    {:ok, low} =
      Kathikon.insert(Kathikon.Workers.PriorityWorker, %{"label" => "low"},
        queue: :priority,
        schedule_at: past,
        priority: 1
      )

    {:ok, high} =
      Kathikon.insert(Kathikon.Workers.PriorityWorker, %{"label" => "high"},
        queue: :priority,
        schedule_at: past,
        priority: 10
      )

    [{pid, _}] = Registry.lookup(Kathikon.Registry, {:dispatcher, :priority})
    assert :sys.get_state(pid).concurrency == 1

    assert {:ok, _} = TestSupport.await_state(low.id, :completed, 10_000)
    assert {:ok, _} = TestSupport.await_state(high.id, :completed, 10_000)

    assert TestSupport.order() == ["high", "low"]
  end

  @tag :async_timing
  test "scheduler promotes scheduled jobs" do
    {:ok, job} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: @queue, schedule_in: 0)

    assert job.state == :scheduled
    send(Kathikon.Scheduler.Promoter, :tick)

    assert {:ok, completed} = TestSupport.await_state(job.id, :completed, 10_000)
    assert completed.state == :completed
  end

  test "start_queue ensures a dispatcher" do
    queue = :"integration_#{System.unique_integer([:positive])}"
    assert :ok = Kathikon.start_queue(queue)

    assert [{_pid, _}] = Registry.lookup(Kathikon.Registry, {:dispatcher, queue})
  end

  @tag :async_timing
  test "pruner removes old terminal jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: @queue)
    assert {:ok, _} = TestSupport.await_state(job.id, :completed)

    Application.put_env(:kathikon, :retention_period, 0)
    send(Kathikon.Pruner, :tick)
    assert TestSupport.await_gone(job.id)
  end
end

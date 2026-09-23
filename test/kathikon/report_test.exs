defmodule Kathikon.ReportTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Job, Report, Storage}

  setup do
    Storage.setup()
    Storage.clear_jobs!()
    :ok
  end

  test "queue summary and job counts" do
    :ok = Kathikon.pause_queue(:default)
    on_exit(fn -> Kathikon.resume_queue(:default) end)

    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available)

    {:ok, _} = Storage.insert(job)

    assert {:ok, summaries} = Report.queue_summary()
    assert Enum.any?(summaries, &(&1.queue == :default))

    assert {:ok, counts} = Report.job_counts()
    assert counts[:available] == 1
  end

  test "count_by_state groups jobs" do
    jobs = [
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available),
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available),
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :completed)
    ]

    assert Report.count_by_state(jobs) == %{available: 2, completed: 1}
  end

  test "throughput and latency" do
    now = DateTime.utc_now()

    completed =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :completed)
      |> Map.put(:started_at, DateTime.add(now, -1, :second))
      |> Map.put(:completed_at, now)

    {:ok, _} = Storage.insert(completed)

    assert {:ok, throughput} = Report.throughput()
    assert Enum.any?(throughput, &(&1.completed == 1))

    assert {:ok, latency} = Report.latency()
    assert latency.samples == 1
  end

  test "dead letter summary" do
    dead =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default)
      |> Map.put(:state, :dead)

    {:ok, _} = Storage.insert(dead)

    assert {:ok, summary} = Report.dead_letter_summary()
    assert summary.count == 1
  end
end

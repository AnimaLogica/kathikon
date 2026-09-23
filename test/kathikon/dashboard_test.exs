defmodule Kathikon.DashboardTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Dashboard, Job, Storage}

  setup do
    Storage.setup()
    Storage.clear_jobs!()
    Kathikon.resume_queue(:default)
    :ok
  end

  test "states_for_tab matches lifecycle" do
    assert Dashboard.states_for_tab(:executing) == [:claimed, :running, :waiting_for_children]
    assert Dashboard.states_for_tab(:dead) == [:failed, :dead]
    assert :waiting_for_children not in Dashboard.states_for_tab(:discarded)
  end

  test "actions_for_state reflects v0.2 rules" do
    assert :cancel in Dashboard.actions_for_state(:available)
    assert :cancel not in Dashboard.actions_for_state(:running)
    assert :discard not in Dashboard.actions_for_state(:running)
    assert :rerun in Dashboard.actions_for_state(:dead)
    assert Dashboard.actions_for_state(:waiting_for_children) == []
  end

  test "state_tabs returns stable tab order" do
    assert Dashboard.state_tabs() == [
             :available,
             :executing,
             :retryable,
             :completed,
             :cancelled,
             :dead,
             :discarded
           ]
  end

  test "queue_summary ui_counts aggregate scheduled into available" do
    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, _scheduled} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
        queue: :default,
        schedule_at: future
      )

    assert {:ok, [row | _]} = Dashboard.queue_summary()
    assert row.ui_counts.available >= 1
    assert Map.get(row.counts, :scheduled, 0) >= 1
  end

  test "queue_summary includes completed jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    {:ok, _} = await_completed(job.id)

    assert {:ok, [row | _]} = Dashboard.queue_summary()
    assert row.queue == :default
    assert row.ui_counts.completed >= 1
  end

  test "list_jobs filters by state and paginates" do
    {:ok, completed} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    {:ok, _} = await_completed(completed.id)

    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, scheduled} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
        queue: :default,
        schedule_at: future
      )

    assert {:ok, page} =
             Dashboard.list_jobs(queue: :default, states: [:completed], limit: 10, offset: 0)

    assert page.total >= 1
    assert Enum.all?(page.jobs, &(&1.state == :completed))
    refute Enum.any?(page.jobs, &(&1.id == scheduled.id))
  end

  test "list_jobs pagination respects offset and limit at storage level" do
    base = DateTime.utc_now()

    for i <- 0..4 do
      at = DateTime.add(base, i, :second)

      job =
        Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
        |> Map.put(:state, :completed)
        |> Map.put(:inserted_at, at)
        |> Map.put(:completed_at, at)

      {:ok, _} = Storage.insert(job)
    end

    {:ok, _} =
      Storage.insert(
        Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
        |> Map.put(:state, :available)
      )

    assert {:ok, page} =
             Dashboard.list_jobs(
               queue: :default,
               states: [:completed],
               limit: 2,
               offset: 2,
               order: :oldest
             )

    assert page.total == 5
    assert length(page.jobs) == 2
    assert Enum.all?(page.jobs, &(&1.state == :completed))
  end

  test "discard_job rejects running jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)

    running =
      job
      |> Map.put(:state, :running)
      |> Map.put(:started_at, DateTime.utc_now())

    {:ok, _} = Storage.update(running)

    assert {:error, {:invalid_state, :running}} = Dashboard.discard_job(job.id)
    assert {:ok, still_running} = Kathikon.fetch(job.id)
    assert still_running.state == :running
  end

  test "discard_jobs skips running jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)

    running =
      job
      |> Map.put(:state, :running)
      |> Map.put(:started_at, DateTime.utc_now())

    {:ok, _} = Storage.update(running)

    failed =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default, max_attempts: 3)
      |> Map.put(:state, :failed)
      |> Map.put(:attempts, 1)

    {:ok, failed_job} = Storage.insert(failed)

    assert {:ok, %{succeeded: 1, errors: errors}} =
             Dashboard.discard_jobs(queue: :default, states: [:failed, :running])

    assert errors == [{job.id, {:invalid_state, :running}}]

    assert {:ok, discarded} = Kathikon.fetch(failed_job.id)
    assert discarded.state == :discarded
    assert {:ok, still_running} = Kathikon.fetch(job.id)
    assert still_running.state == :running
  end

  test "list_jobs supports tab filter" do
    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, _job} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
        queue: :default,
        schedule_at: future
      )

    assert {:ok, page} = Dashboard.list_jobs(tab: :available, queue: :default)
    assert page.total >= 1
    assert Enum.all?(page.jobs, &(&1.state in Dashboard.states_for_tab(:available)))
  end

  test "discard_job routes dead jobs through discard_dead" do
    dead =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default, max_attempts: 1)
      |> Map.put(:state, :dead)
      |> Map.put(:attempts, 1)

    {:ok, inserted} = Storage.insert(dead)

    assert {:error, {:invalid_state, :dead}} = Dashboard.discard_job(inserted.id)
  end

  test "discard_job discards failed jobs" do
    failed =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default)
      |> Map.put(:state, :failed)

    {:ok, inserted} = Storage.insert(failed)

    assert {:ok, discarded} = Dashboard.discard_job(inserted.id)
    assert discarded.state == :discarded
  end

  test "purge_jobs respects older_than cutoff" do
    old_at = DateTime.add(DateTime.utc_now(), -7200, :second)
    recent_at = DateTime.add(DateTime.utc_now(), -60, :second)
    cutoff = DateTime.add(DateTime.utc_now(), -3600, :second)

    old =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :completed)
      |> Map.put(:completed_at, old_at)
      |> Map.put(:inserted_at, old_at)

    recent =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :completed)
      |> Map.put(:completed_at, recent_at)
      |> Map.put(:inserted_at, recent_at)

    {:ok, old_job} = Storage.insert(old)
    {:ok, recent_job} = Storage.insert(recent)

    assert {:ok, %{purged: 1, errors: []}} =
             Dashboard.purge_jobs(queue: :default, states: [:completed], older_than: cutoff)

    assert {:error, :not_found} = Kathikon.fetch(old_job.id)
    assert {:ok, _} = Kathikon.fetch(recent_job.id)
  end

  test "promote_now and prune_now dispatch ticks" do
    assert :ok = Dashboard.promote_now()
    assert :ok = Dashboard.prune_now()
  end

  test "resume_all resumes every known queue" do
    assert :ok = Dashboard.pause_queue(:default)
    assert :ok = Dashboard.resume_all()
    assert %{paused: false} = Dashboard.queue_status(:default)
  end

  test "states_for_tab and actions_for_state return defaults for unknown atoms" do
    assert Dashboard.states_for_tab(:unknown_tab) == []
    assert Dashboard.actions_for_state(:unknown_state) == []
  end

  test "fetch_job returns map and history" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    {:ok, _} = await_completed(job.id)

    assert {:ok, %{job: row, history: history}} = Dashboard.fetch_job(job.id)
    assert row.id == job.id
    assert row.state == :completed
    assert is_list(history)
  end

  test "pause and resume queue" do
    assert :ok = Dashboard.pause_queue(:default)
    assert %{paused: true} = Dashboard.queue_status(:default)
    assert :ok = Dashboard.resume_queue(:default)
    assert %{paused: false} = Dashboard.queue_status(:default)
  end

  test "discard_job rejects unsupported terminal states" do
    completed =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :completed)

    {:ok, inserted} = Storage.insert(completed)

    assert {:error, {:invalid_state, :completed}} = Dashboard.discard_job(inserted.id)
  end

  test "list_jobs accepts a single state atom" do
    {:ok, completed} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    {:ok, _} = await_completed(completed.id)

    assert {:ok, page} = Dashboard.list_jobs(queue: :default, states: :completed)
    assert page.total >= 1
    assert Enum.all?(page.jobs, &(&1.state == :completed))
  end

  test "job rows include last_error from legacy error field" do
    job =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default)
      |> Map.put(:state, :failed)
      |> Map.put(:error, "legacy boom")

    {:ok, _} = Storage.insert(job)

    assert {:ok, page} = Dashboard.list_jobs(queue: :default, states: [:failed])
    assert hd(page.jobs).last_error =~ "legacy boom"
  end

  test "retry_jobs reports per-job errors" do
    retryable =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default)
      |> Map.put(:state, :retryable)

    {:ok, job} = Storage.insert(retryable)

    running =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :running)
      |> Map.put(:started_at, DateTime.utc_now())

    {:ok, running_job} = Storage.insert(running)

    assert {:ok, %{succeeded: 1, errors: errors}} =
             Dashboard.retry_jobs(queue: :default, states: [:retryable, :running])

    assert errors == [{running_job.id, {:invalid_state, :running}}]
    assert {:ok, retried} = Kathikon.fetch(job.id)
    assert retried.state in [:available, :scheduled]
  end

  test "retry_jobs and purge_jobs" do
    job =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default, max_attempts: 1)
      |> Map.put(:state, :dead)
      |> Map.put(:attempts, 1)

    {:ok, _dead_job} = Storage.insert(job)

    assert {:ok, %{succeeded: 1, errors: []}} =
             Dashboard.rerun_jobs(queue: :default, states: [:dead])

    {:ok, completed} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    {:ok, _} = await_completed(completed.id)

    assert {:ok, %{purged: n, errors: []}} =
             Dashboard.purge_jobs(queue: :default, states: [:completed])

    assert n >= 1
    assert {:error, :not_found} = Kathikon.fetch(completed.id)
  end

  test "cancel_jobs skips running jobs" do
    {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, queue: :default)

    running =
      job
      |> Map.put(:state, :running)
      |> Map.put(:started_at, DateTime.utc_now())

    {:ok, _} = Storage.update(running)

    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, pending} =
      Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
        queue: :default,
        schedule_at: future
      )

    assert {:ok, %{succeeded: 1, errors: []}} = Dashboard.cancel_jobs(queue: :default)
    assert {:ok, cancelled} = Kathikon.fetch(pending.id)
    assert cancelled.state == :cancelled
    assert {:ok, still_running} = Kathikon.fetch(job.id)
    assert still_running.state == :running
  end

  defp await_completed(job_id) do
    Kathikon.TestSupport.await_state(job_id, :completed, 10_000)
  end

  test "queue_summary includes dynamic queues from storage" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :dynamic_ops)
      |> Map.put(:state, :available)

    {:ok, _} = Storage.insert(job)

    assert {:ok, rows} = Dashboard.queue_summary()
    assert Enum.any?(rows, &(&1.queue == :dynamic_ops))
  end

  test "pause_all pauses every queue known to the dashboard" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :dynamic_ops)
      |> Map.put(:state, :available)

    {:ok, _} = Storage.insert(job)

    queues =
      Dashboard.queue_summary()
      |> elem(1)
      |> Enum.map(& &1.queue)

    paused_before = Map.new(queues, &{&1, Kathikon.queue_status(&1).paused})

    try do
      assert :ok = Dashboard.pause_all()
      assert %{paused: true} = Dashboard.queue_status(:dynamic_ops)

      for queue <- queues do
        assert %{paused: true} = Kathikon.queue_status(queue)
      end
    after
      for {queue, was_paused} <- paused_before do
        if was_paused, do: Kathikon.pause_queue(queue), else: Kathikon.resume_queue(queue)
      end
    end
  end
end

defmodule Kathikon.Dashboard.RPCTest do
  use ExUnit.Case, async: false

  alias Kathikon.Dashboard.RPC

  @rpc_node :"kathikon_rpc@127.0.0.1"
  @rpc_cookie :kathikon_rpc_test_cookie

  setup context do
    if Map.get(context, :rpc_node) do
      case maybe_start_rpc_node() do
        :unavailable ->
          {:ok, skip: "Cannot start distributed node for RPC tests"}

        :ok ->
          Kathikon.TestSupport.ensure_runtime!()
          Kathikon.Storage.setup()
          Kathikon.Storage.clear_jobs!()
          :ok
      end
    else
      Kathikon.Storage.setup()
      Kathikon.Storage.clear_jobs!()
      :ok
    end
  end

  test "allowed? whitelists dashboard functions" do
    assert RPC.allowed?(:queue_summary)
    refute RPC.allowed?(:purge_now_and_delete_everything)
  end

  test "call rejects non-whitelisted functions" do
    assert {:error, {:rpc_not_allowed, :evil}} =
             RPC.call(@rpc_node, :evil, [])
  end

  test "call forwards dashboard result without double-wrapping" do
    result = RPC.call(@rpc_node, :list_jobs, [[queue: :default, limit: 1]])

    case result do
      {:ok, page} ->
        assert is_list(page.jobs)
        assert is_integer(page.total)
        refute match?({:ok, {:ok, _}}, result)

      {:error, :nodedown} ->
        assert {:error, :nodedown} =
                 RPC.call(:"kathikon_unreachable@127.0.0.1", :queue_summary, [[]])
    end
  end

  test "call returns nodedown for unreachable nodes" do
    assert {:error, :nodedown} =
             RPC.call(:"kathikon_unreachable@127.0.0.1", :queue_summary, [[]])
  end

  @tag :rpc_node
  test "call returns badrpc when remote invocation fails" do
    assert {:error, {:badrpc, _reason}} = RPC.call(@rpc_node, :fetch_job, [123])
  end

  defp maybe_start_rpc_node do
    with :ok <- start_rpc_node(),
         :ok <- set_rpc_cookie(),
         :pong <- :net_adm.ping(@rpc_node) do
      :ok
    else
      _ -> :unavailable
    end
  end

  defp start_rpc_node do
    case Node.start(@rpc_node) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} when node() == @rpc_node ->
        :ok

      {:error, :already_started} when node() == @rpc_node ->
        :ok

      _ ->
        :unavailable
    end
  end

  defp set_rpc_cookie do
    Node.set_cookie(@rpc_cookie)
    :ok
  end
end

defmodule Kathikon.Dashboard.PurgeMockTest do
  use ExUnit.Case, async: false

  import Mox

  alias Kathikon.{Dashboard, Job, Storage}

  @mock Kathikon.Storage.Mock

  setup :verify_on_exit!
  setup :set_mox_from_context

  setup context do
    Kathikon.TestSupport.use_mock_storage!(context)
    :ok
  end

  test "purge_jobs reports delete failures" do
    ok_job = completed_job("ok-id")
    fail_job = completed_job("fail-id")

    stub(@mock, :list_jobs, fn _ -> {:ok, [ok_job, fail_job]} end)

    expect(@mock, :delete, fn "ok-id" -> :ok end)
    expect(@mock, :delete, fn "fail-id" -> {:error, :boom} end)

    assert {:ok, %{purged: 1, errors: [{"fail-id", :boom}]}} =
             Dashboard.purge_jobs(states: [:completed])
  end

  test "purge_jobs reports unexpected delete return values" do
    job = completed_job("weird-id")

    stub(@mock, :list_jobs, fn _ -> {:ok, [job]} end)
    expect(@mock, :delete, fn "weird-id" -> :weird end)

    assert {:ok, %{purged: 0, errors: [{"weird-id", :weird}]}} =
             Dashboard.purge_jobs(states: [:completed])
  end

  test "pause_all falls back to configured queues when list_jobs fails" do
    stub(@mock, :list_jobs, fn _ -> {:error, :boom} end)

    on_exit(fn ->
      for queue <- Kathikon.Config.queue_names(), do: Kathikon.resume_queue(queue)
    end)

    assert :ok = Dashboard.pause_all()
  end

  defp completed_job(id) do
    %Job{
      id: id,
      queue: :default,
      worker: Kathikon.Workers.SuccessWorker,
      args: %{},
      state: :completed,
      inserted_at: DateTime.utc_now(),
      completed_at: DateTime.utc_now(),
      attempts: 1,
      max_attempts: 3,
      errors: []
    }
  end
end

defmodule Mix.Tasks.Kathikon.OpsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Kathikon.{Dashboard, Job, Storage}
  alias Mix.Tasks.Kathikon.Ops

  setup do
    Kathikon.TestSupport.ensure_runtime!()
    Storage.setup()
    Storage.clear_jobs!()
    on_exit(fn -> Dashboard.resume_all() end)
    :ok
  end

  test "show without job id prints usage error" do
    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/show JOB_ID/, fn ->
               Ops.run(["show"])
             end
           end)
  end

  test "jobs rejects negative limit" do
    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/--limit must be non-negative/, fn ->
               Ops.run(["jobs", "--limit", "-1"])
             end
           end)
  end

  test "jobs rejects negative offset" do
    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/--offset must be non-negative/, fn ->
               Ops.run(["jobs", "--offset", "-1"])
             end
           end)
  end

  test "jobs rejects unknown tab" do
    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/unknown tab/, fn ->
               Ops.run(["jobs", "--tab", "bogus"])
             end
           end)
  end

  test "jobs with unknown state returns empty page without crashing" do
    output =
      capture_io(fn ->
        Ops.run(["jobs", "--state", "bogus_state", "--queue", "default"])
      end)

    assert output =~ "out of 0"
  end

  test "prints usage when no command is given" do
    stderr =
      capture_io(:stderr, fn ->
        stdout = capture_io(fn -> Ops.run([]) end)
        send(self(), {:stdout, stdout})
      end)

    assert_received {:stdout, stdout}
    assert stdout <> stderr =~ "usage: mix kathikon.ops COMMAND [options]"
    assert stdout <> stderr =~ "summary"
  end

  test "summary, jobs, and show print local queue data" do
    job = insert_ops_job(:completed)
    :ok = Dashboard.pause_queue(:default)

    summary =
      capture_io(fn ->
        Ops.run(["summary"])
      end)

    assert summary =~ "Queue"
    assert summary =~ "default"
    assert summary =~ "yes"

    jobs =
      capture_io(fn ->
        Ops.run(["jobs", "--tab", "completed", "--limit", "10", "--offset", "0"])
      end)

    assert jobs =~ job.id
    assert jobs =~ "out of"

    blank =
      insert_ops_job(:completed) |> Map.put(:inserted_at, nil) |> Map.put(:completed_at, nil)

    {:ok, _} = Storage.update(blank)

    undated =
      capture_io(fn ->
        Ops.run(["jobs", "--state", "completed"])
      end)

    assert undated =~ blank.id

    detail =
      capture_io(fn ->
        Ops.run(["show", job.id])
      end)

    assert detail =~ "job:"
    assert detail =~ "history events:"
  end

  test "pause, resume, cancel, retry, rerun, purge, and prune" do
    available = insert_ops_job(:available)
    retryable = insert_ops_job(:retryable)
    completed = insert_ops_job(:completed)
    dead = insert_ops_job(:dead)

    assert capture_io(fn -> Ops.run(["pause", "--all"]) end) =~ "ok"
    assert capture_io(fn -> Ops.run(["resume", "--queue", "default"]) end) =~ "ok"

    assert capture_io(fn -> Ops.run(["cancel", available.id]) end) =~ "state=cancelled"

    assert capture_io(fn -> Ops.run(["retry", retryable.id]) end) =~ "ok"

    assert capture_io(fn -> Ops.run(["rerun", completed.id]) end) =~ "ok"

    bulk =
      capture_io(fn ->
        Ops.run(["retry", "--queue", "default"])
      end)

    assert bulk =~ "succeeded:"
    assert bulk =~ "errors:"
    assert bulk =~ dead.id

    purged =
      capture_io(fn ->
        Ops.run(["purge", "--state", "cancelled"])
      end)

    assert purged =~ "Purged"

    assert capture_io(fn -> Ops.run(["prune"]) end) =~ "ok"
  end

  test "pause without a target and unknown commands fail" do
    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/--queue NAME or --all/, fn ->
               Ops.run(["pause"])
             end
           end)

    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/unknown command/, fn ->
               Ops.run(["nope"])
             end
           end)

    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, ~r/RPC/, fn ->
               Ops.run(["--node", "missing@127.0.0.1", "summary"])
             end
           end)

    assert capture_io(:stderr, fn ->
             assert_raise Mix.Error, fn ->
               Ops.run(["cancel", "missing-job"])
             end
           end)
  end

  defp insert_ops_job(state) do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, state)

    {:ok, inserted} = Storage.insert(job)
    inserted
  end
end

defmodule Kathikon.StorageBehaviourTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Job, Storage}

  @moduletag :storage_behaviour

  setup do
    Storage.setup()
    Storage.clear_jobs!()
    :ok
  end

  test "insert and get job" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available)

    assert {:ok, id} = Storage.insert_job(job)
    assert {:ok, fetched} = Storage.get_job(id)
    assert fetched.id == id
  end

  test "claim complete and history" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available)
      |> Map.put(:available_at, DateTime.utc_now())

    {:ok, id} = Storage.insert_job(job)

    claimant = %{
      node: node(),
      pid: inspect(self()),
      claimed_at: DateTime.utc_now(),
      dispatcher_id: self()
    }

    assert {:ok, claimed} = Storage.claim_job(id, claimant)
    assert claimed.state == :claimed

    assert {:ok, running} = Storage.start_job(claimed, claimant)
    assert running.state == :running

    assert {:ok, completed} = Storage.complete_job(id, :ok, %{attempt: 1})
    assert completed.state == :completed

    assert {:ok, history} = Storage.list_history(id)
    assert Enum.any?(history, &(&1.event == :inserted))
    assert Enum.any?(history, &(&1.event == :claimed))
    assert Enum.any?(history, &(&1.event == :completed))
  end

  test "fail retry and dead letter" do
    job =
      Job.build(Kathikon.Workers.FailWorker, %{}, queue: :default, max_attempts: 1)
      |> Map.put(:state, :available)
      |> Map.put(:available_at, DateTime.utc_now())

    {:ok, id} = Storage.insert_job(job)
    claimant = %{node: node(), pid: "test", claimed_at: DateTime.utc_now(), dispatcher_id: self()}
    {:ok, claimed} = Storage.claim_job(id, claimant)
    {:ok, running} = Storage.start_job(claimed, claimant)

    assert {:ok, dead} = Storage.fail_job(running.id, :boom, %{attempt: 1})
    assert dead.state == :dead
    assert {:ok, dead_jobs} = Storage.list_dead_jobs([])
    assert Enum.any?(dead_jobs, &(&1.id == id))
  end

  test "update_job delegates through the facade" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available)

    {:ok, id} = Storage.insert_job(job)
    assert {:ok, updated} = Storage.update_job(id, %{priority: 7})
    assert updated.priority == 7
  end

  test "with_backend restores the previous override" do
    assert :ok = Storage.set_test_backend!(Kathikon.Storage.Mnesia)

    result =
      Storage.with_backend(Kathikon.Storage.Mock, fn ->
        Storage.backend()
      end)

    assert result == Kathikon.Storage.Mock
    assert Storage.backend() == Kathikon.Storage.Mnesia
    assert :ok = Storage.clear_test_backend!()
  end
end

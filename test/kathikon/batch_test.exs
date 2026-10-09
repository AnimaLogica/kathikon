defmodule Kathikon.BatchTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Batch, Job, Storage}

  setup do
    Storage.setup()
    Storage.clear_jobs!()
    Kathikon.pause_queue(:default)
    on_exit(fn -> Kathikon.resume_queue(:default) end)
    :ok
  end

  test "batch tracks children and enqueues continuation" do
    parent =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :running)

    {:ok, parent} = Storage.insert(parent)

    child_specs = [
      {Kathikon.Workers.SuccessWorker, %{"n" => 1}, [queue: :default]},
      {Kathikon.Workers.SuccessWorker, %{"n" => 2}, [queue: :default]}
    ]

    assert {:ok, batch} =
             Batch.start(parent.id, child_specs,
               on_complete: {Kathikon.Workers.SuccessWorker, %{"done" => true}}
             )

    assert batch.pending_count == 2
    assert {:ok, children} = Batch.children(batch.batch_id)
    assert length(children) == 2

    for child_id <- children do
      claimant = %{
        node: node(),
        pid: "test",
        claimed_at: DateTime.utc_now(),
        dispatcher_id: self()
      }

      {:ok, claimed} = Storage.claim_job(child_id, claimant)
      {:ok, running} = Storage.start_job(claimed, claimant)
      {:ok, _} = Storage.complete_job(running.id, :ok, %{attempt: 1})
      Batch.handle_child_finished(%{running | state: :completed, batch_id: batch.batch_id})
    end

    assert {:ok, status} = Batch.status(batch.batch_id)
    assert status.status == :completed

    assert {:ok, continuation} =
             Storage.list_jobs([])
             |> then(fn {:ok, jobs} ->
               job = Enum.find(jobs, &(&1.args == %{"done" => true}))
               if job, do: {:ok, job}, else: {:error, :not_found}
             end)

    assert continuation.worker == Kathikon.Workers.SuccessWorker
  end

  test "append streams children and close seals before the reduce runs" do
    parent = running_parent()
    {:ok, parent} = Storage.insert(parent)

    assert {:ok, batch} =
             Batch.open(parent.id,
               on_complete: {Kathikon.Workers.SuccessWorker, %{"done" => true}}
             )

    assert batch.status == :loading
    assert {:ok, still_running} = Storage.fetch(parent.id)
    assert still_running.state == :running

    specs = [
      {Kathikon.Workers.SuccessWorker, %{"n" => 1}, [queue: :default]},
      {Kathikon.Workers.SuccessWorker, %{"n" => 2}, [queue: :default]}
    ]

    assert {:ok, %{inserted: 2, errors: []}} =
             Batch.append(batch.batch_id, specs, chunk_size: 1)

    assert {:ok, loading} = Batch.status(batch.batch_id)
    assert loading.status == :loading
    assert loading.expected_count == 2
    assert loading.pending_count == 2
    refute continuation_job()

    assert :ok = Batch.close(batch.batch_id)
    assert {:ok, waiting} = Storage.fetch(parent.id)
    assert waiting.state == :waiting_for_children
    refute continuation_job()

    assert {:error, :closed} = Batch.append(batch.batch_id, specs)

    {:ok, children} = Batch.children(batch.batch_id)
    finish_child(hd(children), batch.batch_id)
    refute continuation_job()

    finish_child(List.last(children), batch.batch_id)
    assert continuation_job()
  end

  test "a child that finishes while loading does not run the reduce until close" do
    parent = running_parent()
    {:ok, parent} = Storage.insert(parent)

    {:ok, batch} =
      Batch.open(parent.id, on_complete: {Kathikon.Workers.SuccessWorker, %{"done" => true}})

    {:ok, %{ids: [child_id]}} =
      Batch.append(batch.batch_id, [
        {Kathikon.Workers.SuccessWorker, %{"n" => 1}, [queue: :default]}
      ])

    finish_child(child_id, batch.batch_id)
    assert {:ok, still_running} = Storage.fetch(parent.id)
    assert still_running.state == :running
    refute continuation_job()

    assert :ok = Batch.close(batch.batch_id)
    assert continuation_job()
  end

  defp running_parent do
    Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
    |> Map.put(:state, :running)
  end

  defp finish_child(child_id, batch_id) do
    claimant = %{
      node: node(),
      pid: "test",
      claimed_at: DateTime.utc_now(),
      dispatcher_id: self()
    }

    {:ok, claimed} = Storage.claim_job(child_id, claimant)
    {:ok, running} = Storage.start_job(claimed, claimant)
    {:ok, _} = Storage.complete_job(running.id, :ok, %{attempt: 1})
    Batch.handle_child_finished(%{running | state: :completed, batch_id: batch_id})
  end

  defp continuation_job do
    {:ok, jobs} = Storage.list_jobs([])
    Enum.find(jobs, &(&1.args == %{"done" => true}))
  end
end

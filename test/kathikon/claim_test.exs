defmodule Kathikon.ClaimTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Job, Storage}

  setup do
    :ok = Kathikon.pause_queue(:default)
    on_exit(fn -> Kathikon.resume_queue(:default) end)
    Storage.setup()
    Storage.clear_jobs!()
    :ok
  end

  test "only one concurrent claimant succeeds for the same job" do
    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :available)
      |> Map.put(:available_at, DateTime.utc_now())

    {:ok, _} = Storage.insert(job)

    claimant = fn i ->
      %{
        node: node(),
        pid: inspect(self()),
        claimed_at: DateTime.utc_now(),
        dispatcher_id: i
      }
    end

    results =
      1..100
      |> Enum.map(fn i ->
        Task.async(fn -> Storage.claim_job(job.id, claimant.(i)) end)
      end)
      |> Task.await_many(10_000)

    successes = Enum.filter(results, &match?({:ok, _}, &1))
    failures = Enum.filter(results, &match?({:error, _}, &1))

    assert length(successes) == 1
    assert length(failures) == 99

    assert Enum.all?(failures, fn
             {:error, reason} when reason in [:already_claimed, :not_claimable] -> true
             _ -> false
           end)
  end
end

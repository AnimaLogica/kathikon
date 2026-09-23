defmodule Kathikon.Storage.MnesiaTest do
  use ExUnit.Case, async: false

  import Mox

  alias Kathikon.{Job, Storage}
  alias Kathikon.Storage.Mnesia
  alias Kathikon.Storage.Mnesia.Context.Mock, as: ContextMock

  setup :verify_on_exit!

  setup context do
    previous_context = Application.get_env(:kathikon, :mnesia_context)

    unless context[:mock_context] do
      Storage.setup()
      Storage.clear_jobs!()
    end

    on_exit(fn ->
      if previous_context,
        do: Application.put_env(:kathikon, :mnesia_context, previous_context),
        else: Application.delete_env(:kathikon, :mnesia_context)
    end)

    :ok
  end

  defp available_job(opts \\ []) do
    Job.build(Kathikon.Workers.SuccessWorker, %{}, opts)
    |> Map.put(:state, :available)
    |> Map.put(:available_at, DateTime.utc_now())
  end

  defp claimant do
    %{
      node: node(),
      pid: inspect(self()),
      claimed_at: DateTime.utc_now(),
      dispatcher_id: self()
    }
  end

  describe "job lifecycle" do
    test "claim starts and returns a running job" do
      {:ok, inserted} = Storage.insert(available_job())
      now = DateTime.utc_now()

      assert {:ok, running} = Mnesia.claim(:default, now)
      assert running.state == :running
      assert running.id == inserted.id
    end

    test "fail_job moves exhausted jobs to dead" do
      job =
        available_job(max_attempts: 1)
        |> Map.put(:state, :running)
        |> Map.put(:attempts, 0)

      {:ok, inserted} = Storage.insert(job)

      assert {:ok, dead} = Mnesia.fail_job(inserted.id, :boom, %{attempt: 1})
      assert dead.state == :dead
      assert dead.last_error =~ "boom"
    end

    test "fail_job retries below max attempts" do
      job =
        available_job(max_attempts: 3)
        |> Map.put(:state, :running)

      {:ok, inserted} = Storage.insert(job)

      assert {:ok, retryable} = Mnesia.fail_job(inserted.id, :boom, %{attempt: 1})
      assert retryable.state == :retryable
      assert retryable.attempts == 1
    end

    test "cancel_job rejects running jobs" do
      job = available_job() |> Map.put(:state, :running)
      {:ok, inserted} = Storage.insert(job)

      assert {:error, :running} = Mnesia.cancel_job(inserted.id, :user, %{})
    end

    test "cancel_job rejects terminal jobs" do
      job = available_job() |> Map.put(:state, :completed)
      {:ok, inserted} = Storage.insert(job)

      assert {:error, {:invalid_state, :completed}} =
               Mnesia.cancel_job(inserted.id, :user, %{})
    end

    test "move_to_dead_letter rejects invalid states" do
      job = available_job() |> Map.put(:state, :available)
      {:ok, inserted} = Storage.insert(job)

      assert {:error, {:invalid_state, :available}} =
               Mnesia.move_to_dead_letter(inserted.id, :forced, %{})
    end

    test "retry_job rejects dead jobs without a transition path" do
      job = available_job() |> Map.put(:state, :dead)
      {:ok, inserted} = Storage.insert(job)

      assert {:error, {:invalid_transition, :dead, :available}} =
               Mnesia.retry_job(inserted.id)
    end

    test "list_dead_jobs returns only dead jobs" do
      {:ok, _} = Storage.insert(available_job() |> Map.put(:state, :dead))
      {:ok, _} = Storage.insert(available_job())

      assert {:ok, dead_jobs} = Mnesia.list_dead_jobs()
      assert Enum.all?(dead_jobs, &(&1.state == :dead))
    end

    test "list_jobs_page filters, sorts, and paginates in storage" do
      base = DateTime.utc_now()

      for i <- 0..4 do
        at = DateTime.add(base, i, :second)

        job =
          available_job()
          |> Map.put(:state, :completed)
          |> Map.put(:inserted_at, at)
          |> Map.put(:completed_at, at)

        {:ok, _} = Storage.insert(job)
      end

      {:ok, _} = Storage.insert(available_job())

      assert {:ok, %{jobs: page, total: 5}} =
               Storage.list_jobs_page(
                 states: [:completed],
                 limit: 2,
                 offset: 1,
                 order: :oldest
               )

      assert length(page) == 2
      assert Enum.all?(page, &(&1.state == :completed))

      [first, second] = page
      assert DateTime.compare(first.inserted_at, second.inserted_at) != :gt
    end

    test "update_job accepts string keys" do
      {:ok, inserted} = Storage.insert(available_job())

      assert {:ok, updated} =
               Mnesia.update_job(inserted.id, %{"args" => %{"k" => "v"}})

      assert updated.args == %{"k" => "v"}
    end

    test "discard_job from running state" do
      job = available_job() |> Map.put(:state, :running)
      {:ok, inserted} = Storage.insert(job)

      assert {:ok, discarded} = Mnesia.discard_job(inserted.id, :manual, %{})
      assert discarded.state == :discarded
    end

    test "start_job rejects invalid transitions" do
      job = available_job() |> Map.put(:state, :available)
      {:ok, inserted} = Storage.insert(job)

      assert {:error, {:invalid_state, :available}} =
               Mnesia.start_job(inserted, claimant())
    end
  end

  describe "schedules and history" do
    test "fetch_schedule returns not_found for missing schedule" do
      assert {:error, :not_found} = Mnesia.fetch_schedule("missing")
    end

    test "list_history returns events sorted by time" do
      {:ok, inserted} = Storage.insert(available_job())

      :ok =
        Mnesia.insert_history_event(inserted.id, %{
          id: "evt-1",
          job_id: inserted.id,
          event: :inserted,
          from_state: nil,
          to_state: :available,
          metadata: %{},
          inserted_at: DateTime.add(DateTime.utc_now(), -10, :second)
        })

      :ok =
        Mnesia.insert_history_event(inserted.id, %{
          id: "evt-2",
          job_id: inserted.id,
          event: :claimed,
          from_state: :available,
          to_state: :claimed,
          metadata: %{},
          inserted_at: DateTime.utc_now()
        })

      assert {:ok, events} = Mnesia.list_history(inserted.id)
      event_ids = Enum.map(events, & &1.id)
      assert "evt-1" in event_ids
      assert "evt-2" in event_ids
      assert events == Enum.sort_by(events, & &1.inserted_at, DateTime)
    end
  end

  describe "promotion and pruning" do
    test "promote_scheduled skips future jobs" do
      now = DateTime.utc_now()
      future = DateTime.add(now, 3600, :second)

      job =
        Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
        |> Map.put(:state, :scheduled)
        |> Map.put(:scheduled_at, future)

      {:ok, _} = Storage.insert(job)
      assert Mnesia.promote_scheduled(now) == 0
    end

    test "promote_scheduled promotes multiple due jobs" do
      now = DateTime.utc_now()

      for _ <- 1..2 do
        job =
          Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
          |> Map.put(:state, :scheduled)
          |> Map.put(:scheduled_at, now)

        {:ok, _} = Storage.insert(job)
      end

      assert Mnesia.promote_scheduled(now) == 2
    end

    test "promote_scheduled returns 0 when mnesia is stopped" do
      on_exit(fn ->
        {:ok, _} = Application.ensure_all_started(:mnesia)
        Kathikon.TestSupport.ensure_runtime!()
        Storage.setup()
      end)

      :ok = Application.stop(:mnesia)
      assert Mnesia.promote_scheduled(DateTime.utc_now()) == 0
    end
  end

  describe "mnesia context mock" do
    @tag mock_context: true
    setup context do
      Application.put_env(:kathikon, :mnesia_context, ContextMock)

      on_exit(fn ->
        Application.delete_env(:kathikon, :mnesia_context)
        Storage.setup()
        Storage.clear_jobs!()
      end)

      context
    end

    test "fetch surfaces aborted transactions" do
      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :not_found} end)

      assert {:error, :not_found} = Mnesia.fetch("missing")
    end

    test "promote_scheduled returns zero when transaction aborts" do
      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :locked} end)

      assert Mnesia.promote_scheduled(DateTime.utc_now()) == 0
    end

    test "claim_job surfaces aborted claim errors" do
      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :not_claimable} end)

      assert {:error, :not_claimable} =
               Mnesia.claim_job("job-id", claimant())
    end

    test "insert_history_event surfaces aborted writes" do
      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :history_failed} end)

      assert {:error, :history_failed} =
               Mnesia.insert_history_event("job-id", %{id: "evt", job_id: "job-id"})
    end

    test "claim and batch aborts use the context mock" do
      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :weird} end)

      assert {:error, :weird} = Mnesia.claim_job("job-id", claimant())

      child =
        available_job()
        |> Map.put(:batch_id, "batch-1")

      Mox.expect(ContextMock, :transaction, fn _fun -> {:aborted, :other_reason} end)

      assert {:error, :other_reason} = Mnesia.record_batch_child_finished(child)
    end
  end

  describe "error branches" do
    test "missing jobs and illegal transitions return errors" do
      assert {:error, :not_found} = Mnesia.update_job("missing", %{priority: 1})
      assert {:error, :not_found} = Mnesia.claim_job("missing", claimant())
      assert {:error, :not_found} = Mnesia.defer_job("missing", DateTime.utc_now(), %{})
      assert {:error, :not_found} = Mnesia.start_batch("missing", [], %{batch_id: "b"})

      {:ok, available} = Storage.insert(available_job())

      assert {:error, {:invalid_state, :available}} =
               Mnesia.defer_job(available.id, DateTime.utc_now(), %{})

      assert {:error, {:invalid_state, :available}} =
               Mnesia.fail_job(available.id, :boom, %{attempt: 1})

      assert {:error, {:invalid_state, :available}} =
               Mnesia.start_batch(available.id, [], %{batch_id: "b-idle"})

      assert {:ok, _jobs} = Mnesia.list_jobs()
      assert {:ok, %{jobs: page, total: total}} = Mnesia.list_jobs_page()
      assert is_list(page)
      assert is_integer(total)

      assert {:ok, [_ | _]} = Mnesia.list_jobs(queue: :default, state: :available)
    end

    test "update_job changes state and rejects unknown string keys" do
      {:ok, job} = Storage.insert(available_job())

      assert {:ok, cancelled} = Mnesia.update_job(job.id, %{state: :cancelled})
      assert cancelled.state == :cancelled

      assert {:ok, unchanged} =
               Mnesia.update_job(job.id, %{"kathikon_unknown_field_zz" => 1})

      assert unchanged.state == :cancelled
    end

    test "discard covers scheduled, failed, and already discarded jobs" do
      {:ok, scheduled} =
        Storage.insert(available_job() |> Map.put(:state, :scheduled))

      assert {:error, {:invalid_transition, :scheduled, :discarded}} =
               Mnesia.discard_job(scheduled.id, :manual, %{})

      {:ok, failed} = Storage.insert(available_job() |> Map.put(:state, :failed))
      assert {:ok, discarded} = Mnesia.discard_job(failed.id, :manual, %{})
      assert discarded.state == :discarded
      assert {:ok, again} = Mnesia.discard_job(failed.id, :again, %{})
      assert again.state == :discarded
    end

    test "start_batch rejects a duplicate child and a finished batch ignores another child" do
      child = available_job()
      {:ok, child} = Storage.insert(child)

      parent =
        available_job()
        |> Map.put(:state, :running)

      {:ok, parent} = Storage.insert(parent)

      assert {:error, {:already_exists, _}} =
               Mnesia.start_batch(parent.id, [child], %{batch_id: "dup"})

      fresh_child =
        available_job()
        |> Map.put(:state, :completed)

      {:ok, batch} =
        Mnesia.start_batch(parent.id, [fresh_child], %{
          batch_id: "once",
          status: :running,
          success_policy: :all_succeeded,
          success_count: 0,
          failure_count: 0,
          cancelled_count: 0
        })

      assert batch.batch_id == "once"

      finished = %{fresh_child | batch_id: "once", state: :completed}
      assert {:ok, :complete, _batch, _parent} = Mnesia.record_batch_child_finished(finished)
      assert {:ok, :already_finished} = Mnesia.record_batch_child_finished(finished)

      missing = %{fresh_child | batch_id: "missing-batch"}
      assert {:error, :not_found} = Mnesia.record_batch_child_finished(missing)
    end
  end
end

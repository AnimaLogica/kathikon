defmodule Kathikon.UniquenessTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Kathikon.{Job, Scheduler, Storage, TestSupport, Unique}
  alias Kathikon.Test.BackendWithoutUnique

  setup do
    TestSupport.ensure_runtime!()
    Kathikon.pause_queue(:default)
    Storage.clear_jobs!()
    :ok
  end

  # Moves a job to another state without going through transitions.
  defp set_state!(job, state) do
    {:ok, _} = Storage.update(%{job | state: state})
    :ok
  end

  # Ages a job's inserted_at by `seconds` in the past.
  defp age!(job, seconds) do
    aged = %{job | inserted_at: DateTime.add(job.inserted_at, -seconds, :second)}
    {:ok, _} = Storage.update(aged)
    :ok
  end

  test "identical args coalesce within the period" do
    args = %{"kind" => "webp", "media_id" => 42}
    unique = [period: 60, criteria: fn job -> job.args end]

    assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

    assert second.id == first.id
    assert length(Storage.all()) == 1
  end

  test "different args insert separately" do
    unique = [period: 60, criteria: fn job -> job.args end]

    assert {:ok, a} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 1}, unique: unique)

    assert {:ok, b} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 2}, unique: unique)

    assert a.id != b.id
    assert length(Storage.all()) == 2
  end

  test "worker unique default applies; unique: false disables it" do
    args = %{"kind" => "all", "media_id" => 7}

    assert {:ok, first} = Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args)
    assert {:ok, second} = Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args)
    assert second.id == first.id

    assert {:ok, third} =
             Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args, unique: false)

    assert third.id != first.id
  end

  test "insert-level unique overrides worker default period" do
    args = %{"media_id" => 9}
    unique = [period: 5, criteria: fn job -> job.args end]

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args, unique: unique)

    assert {:ok, second} =
             Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args, unique: unique)

    assert second.id == first.id
  end

  test "on_conflict: :error returns {:error, :unique}" do
    args = %{"media_id" => 3}
    unique = [period: 60, on_conflict: :error]

    assert {:ok, _} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

    assert {:error, :unique} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
  end

  test "a cancelled job does not block a new insert" do
    args = %{"media_id" => 11}
    unique = [period: 60]

    assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    assert {:ok, _} = Kathikon.cancel(first.id)
    assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

    assert second.id != first.id
  end

  test "period expiry allows a second insert" do
    args = %{"media_id" => 13}
    unique = [period: 60]

    assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    :ok = age!(first, 120)

    assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    assert second.id != first.id
  end

  test "coalesce does not emit inserted telemetry" do
    args = %{"media_id" => 15}
    parent = self()
    unique = [period: 60]

    :telemetry.attach(
      "kathikon-unique-test",
      [:kathikon, :job, :inserted],
      fn _event, _meas, meta, _ -> send(parent, {:inserted, meta.job_id}) end,
      nil
    )

    assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    assert_receive {:inserted, id} when id == first.id

    assert {:ok, ^first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
    refute_receive {:inserted, _}, 50
  after
    :telemetry.detach("kathikon-unique-test")
  end

  describe "coalesce reporting" do
    test "a coalesce emits [:job, :coalesced] once with the existing job" do
      parent = self()
      args = %{"media_id" => 21}
      unique = [period: 60]

      :telemetry.attach(
        "kathikon-coalesced-test",
        [:kathikon, :job, :coalesced],
        fn _event, measurements, meta, _ -> send(parent, {:coalesced, measurements, meta}) end,
        nil
      )

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      refute_receive {:coalesced, _, _}, 50

      assert {:ok, ^first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

      assert_receive {:coalesced, %{count: 1}, meta}
      assert meta.job_id == first.id
      assert meta.state == :available
      assert meta.submitted_queue == :default
      refute_receive {:coalesced, _, _}, 50
    after
      :telemetry.detach("kathikon-coalesced-test")
    end

    test "coalescing into a completed job logs a warning" do
      args = %{"media_id" => 22}
      unique = [period: 60]

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      :ok = set_state!(first, :completed)

      log =
        capture_log(fn ->
          assert {:ok, %{state: :completed}} =
                   Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
        end)

      assert log =~ "coalesced into completed job #{first.id}"
    end

    test "coalescing into a live job logs no warning" do
      args = %{"media_id" => 23}
      unique = [period: 60]

      assert {:ok, _} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

      log =
        capture_log(fn ->
          assert {:ok, _} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
        end)

      refute log =~ "coalesced into completed job"
    end

    test "insert_many reports the coalesced count" do
      args = %{"kind" => "media", "id" => 24}
      spec = {Kathikon.Workers.SuccessWorker, args, [unique: [period: 5]]}

      assert {:ok, %{inserted: 5, coalesced: 4, ids: ids, errors: []}} =
               Kathikon.insert_many(List.duplicate(spec, 5))

      assert length(Enum.uniq(ids)) == 1
      assert length(Storage.all()) == 1
    end

    test "insert_many counts zero coalesced for distinct specs" do
      specs =
        for n <- 1..3, do: {Kathikon.Workers.SuccessWorker, %{"n" => n}, [unique: [period: 5]]}

      assert {:ok, %{inserted: 3, coalesced: 0}} = Kathikon.insert_many(specs)
    end
  end

  describe "states" do
    test "a completed job blocks a duplicate inside the period by default" do
      args = %{"media_id" => 31}
      unique = [period: 60]

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      :ok = set_state!(first, :completed)

      assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert second.id == first.id
    end

    test "failed and dead jobs do not block a retry by default" do
      for state <- [:failed, :dead] do
        Storage.clear_jobs!()
        args = %{"media_id" => 32}
        unique = [period: 60]

        assert {:ok, first} =
                 Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

        :ok = set_state!(first, state)

        assert {:ok, second} =
                 Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

        assert second.id != first.id
      end
    end

    test "an explicit states list replaces the default" do
      args = %{"media_id" => 33}
      unique = [period: 60, states: [:available]]

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      :ok = set_state!(first, :completed)

      assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert second.id != first.id
    end
  end

  describe "index lifetime" do
    test "the index row survives a completed transition and is removed by delete/1" do
      args = %{"media_id" => 41}
      unique = [period: 60]

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      :ok = set_state!(first, :completed)
      assert index_rows_for(first.id) == 1

      :ok = Storage.delete(first.id)
      assert index_rows_for(first.id) == 0
    end

    test "the index row is removed by delete/1 even when the job is still live" do
      args = %{"media_id" => 42}

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: [period: 60])

      assert index_rows_for(first.id) == 1
      :ok = Storage.delete(first.id)
      assert index_rows_for(first.id) == 0
    end
  end

  test "setup adds the job_id index to a unique table created without it" do
    {:atomic, :ok} = :mnesia.del_table_index(:kathikon_unique, :job_id)

    assert :ok = Storage.Mnesia.setup()

    assert {:atomic, _rows} =
             :mnesia.transaction(fn -> :mnesia.index_read(:kathikon_unique, "none", :job_id) end)
  end

  describe "criteria" do
    test "coalesces jobs that differ only in ignored args" do
      criteria = fn job -> job.args["media_id"] end

      assert {:ok, first} =
               Kathikon.insert(
                 Kathikon.Workers.SuccessWorker,
                 %{"media_id" => 1, "requested_at" => 100},
                 unique: [period: 60, criteria: criteria]
               )

      assert {:ok, second} =
               Kathikon.insert(
                 Kathikon.Workers.SuccessWorker,
                 %{"media_id" => 1, "requested_at" => 200},
                 unique: [period: 60, criteria: criteria]
               )

      assert second.id == first.id
      assert length(Storage.all()) == 1
    end

    test "different return values insert separately" do
      criteria = fn job -> job.args["media_id"] end

      assert {:ok, a} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 1},
                 unique: [period: 60, criteria: criteria]
               )

      assert {:ok, b} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 2},
                 unique: [period: 60, criteria: criteria]
               )

      assert a.id != b.id
    end

    test "receives the built job struct" do
      parent = self()

      criteria = fn job ->
        send(parent, {:criteria_called, job})
        job.queue
      end

      assert {:ok, _} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"x" => 1},
                 queue: :default,
                 priority: 3,
                 unique: [period: 60, criteria: criteria]
               )

      assert_receive {:criteria_called, %Job{queue: :default, priority: 3, args: %{"x" => 1}}}
    end

    test "{module, function} pair works" do
      unique = [period: 60, criteria: {Kathikon.Workers.UniqueByWorker, :unique_key}]

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 5, "n" => 1},
                 unique: unique
               )

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 5, "n" => 2},
                 unique: unique
               )

      assert second.id == first.id
    end

    test "external capture works as a worker default via use" do
      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.UniqueByWorker, %{"media_id" => 7, "n" => 1})

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.UniqueByWorker, %{"media_id" => 7, "n" => 2})

      assert second.id == first.id
    end

    test "anonymous closure works as a worker default via use" do
      # UniqueMediaWorker declares its criteria as an anonymous function in `use`.
      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, %{"media_id" => 8, "n" => 1})

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, %{"media_id" => 8, "n" => 2})

      assert second.id != first.id

      assert {:ok, third} =
               Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, %{"media_id" => 8, "n" => 1})

      assert third.id == first.id
    end

    test "omitted criteria defaults to queue and args" do
      unique = [period: 60]
      args = %{"media_id" => 51}

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert {:ok, same} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert same.id == first.id

      assert {:ok, other_queue} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
                 queue: :other,
                 unique: unique
               )

      assert other_queue.id != first.id
    end

    test "the worker module is part of the fingerprint" do
      assert {:ok, a} = Kathikon.insert(Kathikon.Workers.UniqueByWorker, %{"media_id" => 9})

      assert {:ok, b} = Kathikon.insert(Kathikon.Workers.UniqueByOtherWorker, %{"media_id" => 9})

      assert a.id != b.id
    end

    test "equal maps built in different insertion order share a fingerprint" do
      criteria = fn job -> job.args end
      big = for i <- 1..40, into: %{}, do: {"k#{i}", i}
      reversed = big |> Map.to_list() |> Enum.reverse() |> Map.new()

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, big,
                 unique: [period: 60, criteria: criteria]
               )

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, reversed,
                 unique: [period: 60, criteria: criteria]
               )

      assert second.id == first.id
    end

    test "the function runs once per insert" do
      parent = self()

      criteria = fn job ->
        send(parent, :criteria_called)
        job.args
      end

      assert {:ok, _} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, criteria: criteria]
               )

      assert_receive :criteria_called
      refute_receive :criteria_called, 50
    end

    test "a raising function returns an error and writes no job" do
      criteria = fn _job -> raise "boom" end

      assert {:error, {:unique_key_failed, %RuntimeError{message: "boom"}}} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, criteria: criteria]
               )

      assert Storage.all() == []
    end

    test "a throwing function returns an error" do
      criteria = fn _job -> throw(:nope) end

      assert {:error, {:unique_key_failed, {:throw, :nope}}} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, criteria: criteria]
               )
    end

    test "insert_many records the failure at the input index and inserts the rest" do
      criteria = fn job ->
        if job.args["bad"], do: raise("bad spec"), else: job.args["id"]
      end

      unique = [unique: [period: 60, criteria: criteria]]

      specs = [
        {Kathikon.Workers.SuccessWorker, %{"id" => 1}, unique},
        {Kathikon.Workers.SuccessWorker, %{"id" => 2, "bad" => true}, unique},
        {Kathikon.Workers.SuccessWorker, %{"id" => 3}, unique}
      ]

      assert {:ok, %{inserted: 2, errors: [{1, {:unique_key_failed, %RuntimeError{}}}]}} =
               Kathikon.insert_many(specs)

      assert length(Storage.all()) == 2
    end

    test "insert_many with on_error: :abort stops on a failing function" do
      criteria = fn _job -> raise "boom" end

      specs = [
        {Kathikon.Workers.SuccessWorker, %{"id" => 1}, [unique: [period: 60, criteria: criteria]]}
      ]

      assert {:error, {:unique_key_failed, %RuntimeError{}}} =
               Kathikon.insert_many(specs, on_error: :abort)

      assert Storage.all() == []
    end

    test "insert_many coalesces by criteria" do
      unique = [unique: [period: 60, criteria: fn job -> job.args["media_id"] end]]

      specs =
        for n <- 1..4 do
          {Kathikon.Workers.SuccessWorker, %{"media_id" => 1, "n" => n}, unique}
        end

      assert {:ok, %{inserted: 4, coalesced: 3, ids: ids, errors: []}} =
               Kathikon.insert_many(specs)

      assert length(Enum.uniq(ids)) == 1
    end
  end

  describe "timestamp and period" do
    test "timestamp: :scheduled_at keeps a far-future job blocking past the period" do
      unique = [period: 60, timestamp: :scheduled_at]
      args = %{"media_id" => 61}

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
                 schedule_in: 604_800,
                 unique: unique
               )

      # 120 seconds after insertion, but the job is still a week from running.
      :ok = age!(first, 120)

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
                 schedule_in: 604_800,
                 unique: unique
               )

      assert second.id == first.id
    end

    test "the default timestamp: :inserted_at expires a far-future job after the period" do
      unique = [period: 60]
      args = %{"media_id" => 62}

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
                 schedule_in: 604_800,
                 unique: unique
               )

      :ok = age!(first, 120)

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
                 schedule_in: 604_800,
                 unique: unique
               )

      assert second.id != first.id
    end

    test "timestamp: :scheduled_at anchors on scheduled_at for immediate jobs" do
      unique = [period: 60, timestamp: :scheduled_at]
      args = %{"media_id" => 63}

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

      # Immediate jobs carry scheduled_at = insert time, so aging only inserted_at
      # does not expire them under :scheduled_at.
      assert {:ok, same} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert same.id == first.id

      aged = %{
        first
        | inserted_at: DateTime.add(first.inserted_at, -120, :second),
          scheduled_at: DateTime.add(first.scheduled_at, -120, :second)
      }

      {:ok, _} = Storage.update(aged)

      assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert second.id != first.id
    end

    test "conflict? falls back to inserted_at when scheduled_at is nil" do
      now = DateTime.utc_now()

      unique = %{
        period: 60,
        timestamp: :scheduled_at,
        states: [:available],
        on_conflict: :coalesce
      }

      recent = %Job{
        id: "a",
        queue: :default,
        worker: Kathikon.Workers.SuccessWorker,
        args: %{},
        state: :available,
        inserted_at: now,
        scheduled_at: nil
      }

      stale = %{recent | inserted_at: DateTime.add(now, -120, :second)}

      assert Unique.conflict?(recent, unique, now)
      refute Unique.conflict?(stale, unique, now)
      refute Unique.conflict?(%{recent | inserted_at: nil}, unique, now)
    end

    test "period: :infinity blocks for as long as the job is live" do
      unique = [period: :infinity]
      args = %{"media_id" => 64}

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      :ok = age!(first, 10 * 24 * 60 * 60)

      assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert second.id == first.id
    end
  end

  describe "scheduler path" do
    test "schedule_once honours the worker's unique default" do
      args = %{"media_id" => 71}

      assert {:ok, first_id} =
               Scheduler.BuiltIn.schedule_once(Kathikon.Workers.UniqueMediaWorker, args, in: 3600)

      assert {:ok, second_id} =
               Scheduler.BuiltIn.schedule_once(Kathikon.Workers.UniqueMediaWorker, args, in: 3600)

      assert second_id == first_id
      assert length(Storage.all()) == 1
    end

    test "schedule_once with unique: false inserts a second job" do
      args = %{"media_id" => 72}

      assert {:ok, first_id} =
               Scheduler.BuiltIn.schedule_once(Kathikon.Workers.UniqueMediaWorker, args,
                 in: 3600,
                 unique: false
               )

      assert {:ok, second_id} =
               Scheduler.BuiltIn.schedule_once(Kathikon.Workers.UniqueMediaWorker, args,
                 in: 3600,
                 unique: false
               )

      assert second_id != first_id
    end
  end

  describe "option validation" do
    test ":keys raises because it was removed" do
      assert_raise ArgumentError, ~r/unique :keys was removed; use criteria: instead/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, unique: [period: 60, keys: [:args]])
      end
    end

    test ":by raises because it was removed" do
      assert_raise ArgumentError, ~r/unique :by was removed; use criteria: instead/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, by: fn _ -> 1 end]
        )
      end
    end

    test "a criteria function of the wrong arity raises" do
      assert_raise ArgumentError, ~r/unique :criteria must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, criteria: fn _a, _b -> 1 end]
        )
      end
    end

    test "a non-function criteria raises" do
      assert_raise ArgumentError, ~r/unique :criteria must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, criteria: "nope"]
        )
      end
    end

    test "an invalid period raises" do
      assert_raise ArgumentError,
                   ~r/unique :period must be a positive integer or :infinity/,
                   fn ->
                     Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, unique: [period: 0])
                   end
    end

    test "an invalid timestamp raises" do
      assert_raise ArgumentError, ~r/unique :timestamp must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, timestamp: :created_at]
        )
      end
    end

    test "an empty or unknown states value raises" do
      assert_raise ArgumentError, ~r/unique :states must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, unique: [period: 60, states: []])
      end

      assert_raise ArgumentError, ~r/unique :states must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, unique: [period: 60, states: :all])
      end
    end
  end

  test "concurrent inserts of the same args coalesce to one job" do
    args = %{"kind" => "media", "id" => 99}
    unique = [period: 60]

    ids =
      1..50
      |> Task.async_stream(
        fn _ ->
          {:ok, job} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
          job.id
        end,
        max_concurrency: 50,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, id} -> id end)

    assert length(Enum.uniq(ids)) == 1
    assert length(Storage.all()) == 1
  end

  describe "regression coverage for uniqueness edge paths" do
    test "a stale index row whose job is gone is dropped on the next insert" do
      args = %{"media_id" => 81}
      unique = [period: 60]

      assert {:ok, first} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

      # Remove the job row but leave its index row behind.
      {:atomic, :ok} = :mnesia.transaction(fn -> :mnesia.delete({:kathikon_jobs, first.id}) end)
      assert index_rows_for(first.id) == 1

      assert {:ok, second} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)
      assert second.id != first.id
      assert index_rows_for(first.id) == 0
    end

    test "insert_many with on_error: :abort returns {:error, :unique} on a conflict with on_conflict: :error" do
      args = %{"media_id" => 82}
      unique = [period: 60, on_conflict: :error]

      assert {:ok, _} = Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

      assert {:error, :unique} =
               Kathikon.insert_many([{Kathikon.Workers.SuccessWorker, args, [unique: unique]}],
                 on_error: :abort
               )

      assert length(Storage.all()) == 1
    end

    test "an invalid on_conflict raises" do
      assert_raise ArgumentError, ~r/unique :on_conflict must be :coalesce or :error/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, on_conflict: :ignore]
        )
      end
    end

    test "a worker declared with unique: false never coalesces" do
      args = %{"n" => 1}

      assert {:ok, a} = Kathikon.insert(Kathikon.Workers.UniqueDisabledWorker, args)
      assert {:ok, b} = Kathikon.insert(Kathikon.Workers.UniqueDisabledWorker, args)
      assert a.id != b.id
    end

    test "insert_many reports errors for a chunk where every spec fails" do
      criteria = fn _job -> raise "boom" end

      specs =
        for n <- 1..2 do
          {Kathikon.Workers.SuccessWorker, %{"n" => n},
           [unique: [period: 60, criteria: criteria]]}
        end

      assert {:ok,
              %{
                inserted: 0,
                coalesced: 0,
                ids: [],
                errors: [{0, {:unique_key_failed, _}}, {1, {:unique_key_failed, _}}]
              }} = Kathikon.insert_many(specs)

      assert Storage.all() == []
    end

    test "an explicit id that is already taken returns already_exists" do
      criteria = fn job -> job.args["n"] end

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"n" => 1},
                 id: "dup-id-1",
                 unique: [period: 60, criteria: criteria]
               )

      assert first.id == "dup-id-1"

      # Different criteria value, so no conflict; the id is what collides.
      assert {:error, {:already_exists, "dup-id-1"}} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"n" => 2},
                 id: "dup-id-1",
                 unique: [period: 60, criteria: criteria]
               )
    end

    test "a backend without insert_unique/2 falls back to plain insert" do
      args = %{"media_id" => 90}
      unique = [period: 60]

      Storage.with_backend(BackendWithoutUnique, fn ->
        assert {:ok, first} =
                 Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

        assert {:ok, second} =
                 Kathikon.insert(Kathikon.Workers.SuccessWorker, args, unique: unique)

        assert second.id != first.id
      end)

      assert length(Storage.all()) == 2
    end

    test "schedule_once without at or in inserts an immediately available job" do
      assert {:ok, id} =
               Scheduler.BuiltIn.schedule_once(Kathikon.Workers.SuccessWorker, %{"n" => 1}, [])

      assert {:ok, %{state: :available}} = Kathikon.status(id)
    end
  end

  # Number of rows in the :kathikon_unique index that point at `job_id`.
  defp index_rows_for(job_id) do
    {:atomic, rows} =
      :mnesia.transaction(fn -> :mnesia.index_read(:kathikon_unique, job_id, :job_id) end)

    length(rows)
  end
end

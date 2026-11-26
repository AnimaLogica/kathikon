defmodule Kathikon.UniquenessTest do
  use ExUnit.Case, async: false

  alias Kathikon.TestSupport

  setup do
    TestSupport.ensure_runtime!()
    Kathikon.pause_queue(:default)
    Kathikon.Storage.clear_jobs!()
    :ok
  end

  test "identical args coalesce within the period" do
    args = %{"kind" => "webp", "media_id" => 42}

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert {:ok, second} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert second.id == first.id
    assert length(Kathikon.Storage.all()) == 1
  end

  test "different args insert separately" do
    assert {:ok, a} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 1},
               unique: [period: 60, keys: [:args]]
             )

    assert {:ok, b} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 2},
               unique: [period: 60, keys: [:args]]
             )

    assert a.id != b.id
    assert length(Kathikon.Storage.all()) == 2
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

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args,
               unique: [period: 5, keys: [:args]]
             )

    assert {:ok, second} =
             Kathikon.insert(Kathikon.Workers.UniqueMediaWorker, args,
               unique: [period: 5, keys: [:args]]
             )

    assert second.id == first.id
  end

  test "on_conflict: :error returns {:error, :unique}" do
    args = %{"media_id" => 3}

    assert {:ok, _} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args], on_conflict: :error]
             )

    assert {:error, :unique} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args], on_conflict: :error]
             )
  end

  test "terminal jobs do not block a new insert" do
    args = %{"media_id" => 11}

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert {:ok, _} = Kathikon.cancel(first.id)

    assert {:ok, second} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert second.id != first.id
  end

  test "period expiry allows a second insert" do
    args = %{"media_id" => 13}

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    aged = %{first | inserted_at: DateTime.add(first.inserted_at, -120, :second)}
    assert {:ok, _} = Kathikon.Storage.update(aged)

    assert {:ok, second} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert second.id != first.id
  end

  test "coalesce does not emit inserted telemetry" do
    args = %{"media_id" => 15}
    parent = self()

    :telemetry.attach(
      "kathikon-unique-test",
      [:kathikon, :job, :inserted],
      fn _event, _meas, meta, _ -> send(parent, {:inserted, meta.job_id}) end,
      nil
    )

    assert {:ok, first} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    assert_receive {:inserted, id} when id == first.id

    assert {:ok, ^first} =
             Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
               unique: [period: 60, keys: [:args]]
             )

    refute_receive {:inserted, _}, 50
  after
    :telemetry.detach("kathikon-unique-test")
  end

  test "insert_many coalesces duplicate specs in a burst" do
    args = %{"kind" => "media", "id" => 1}

    specs =
      for _ <- 1..5 do
        {Kathikon.Workers.SuccessWorker, args, [unique: [period: 5, keys: [:args]]]}
      end

    assert {:ok, %{inserted: 5, ids: ids, errors: []}} = Kathikon.insert_many(specs)
    assert length(Enum.uniq(ids)) == 1
    assert length(Kathikon.Storage.all()) == 1
  end

  test "concurrent inserts of the same args coalesce to one job" do
    args = %{"kind" => "media", "id" => 99}

    ids =
      1..50
      |> Task.async_stream(
        fn _ ->
          {:ok, job} =
            Kathikon.insert(Kathikon.Workers.SuccessWorker, args,
              unique: [period: 60, keys: [:args]]
            )

          job.id
        end,
        max_concurrency: 50,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, id} -> id end)

    assert length(Enum.uniq(ids)) == 1
    assert length(Kathikon.Storage.all()) == 1
  end

  describe ":by function" do
    test "coalesces jobs that differ only in ignored args" do
      by = fn job -> job.args["media_id"] end

      assert {:ok, first} =
               Kathikon.insert(
                 Kathikon.Workers.SuccessWorker,
                 %{"media_id" => 1, "requested_at" => 100},
                 unique: [period: 60, by: by]
               )

      assert {:ok, second} =
               Kathikon.insert(
                 Kathikon.Workers.SuccessWorker,
                 %{"media_id" => 1, "requested_at" => 200},
                 unique: [period: 60, by: by]
               )

      assert second.id == first.id
      assert length(Kathikon.Storage.all()) == 1
    end

    test "different return values insert separately" do
      by = fn job -> job.args["media_id"] end

      assert {:ok, a} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 1},
                 unique: [period: 60, by: by]
               )

      assert {:ok, b} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"media_id" => 2},
                 unique: [period: 60, by: by]
               )

      assert a.id != b.id
    end

    test "gets the built job struct" do
      parent = self()

      by = fn job ->
        send(parent, {:by_called, job})
        job.queue
      end

      assert {:ok, _} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"x" => 1},
                 queue: :default,
                 priority: 3,
                 unique: [period: 60, by: by]
               )

      assert_receive {:by_called, %Kathikon.Job{queue: :default, priority: 3, args: %{"x" => 1}}}
    end

    test "{module, function} pair works" do
      unique = [period: 60, by: {Kathikon.Workers.UniqueByWorker, :unique_key}]

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

    test "the worker module is part of the fingerprint" do
      assert {:ok, a} =
               Kathikon.insert(Kathikon.Workers.UniqueByWorker, %{"media_id" => 9})

      assert {:ok, b} =
               Kathikon.insert(Kathikon.Workers.UniqueByOtherWorker, %{"media_id" => 9})

      assert a.id != b.id
    end

    test "equal maps built in different insertion order share a fingerprint" do
      by = fn job -> job.args end
      big = for i <- 1..40, into: %{}, do: {"k#{i}", i}
      reversed = big |> Map.to_list() |> Enum.reverse() |> Map.new()

      assert {:ok, first} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, big, unique: [period: 60, by: by])

      assert {:ok, second} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, reversed,
                 unique: [period: 60, by: by]
               )

      assert second.id == first.id
    end

    test "the function runs once per insert" do
      parent = self()

      by = fn job ->
        send(parent, :by_called)
        job.args
      end

      assert {:ok, _} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, by: by]
               )

      assert_receive :by_called
      refute_receive :by_called, 50
    end

    test "a raising function returns an error and writes no job" do
      by = fn _job -> raise "boom" end

      assert {:error, {:unique_key_failed, %RuntimeError{message: "boom"}}} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, by: by]
               )

      assert Kathikon.Storage.all() == []
    end

    test "a throwing function returns an error" do
      by = fn _job -> throw(:nope) end

      assert {:error, {:unique_key_failed, {:throw, :nope}}} =
               Kathikon.insert(Kathikon.Workers.SuccessWorker, %{"a" => 1},
                 unique: [period: 60, by: by]
               )
    end

    test "insert_many records the failure at the input index and inserts the rest" do
      by = fn job ->
        if job.args["bad"], do: raise("bad spec"), else: job.args["id"]
      end

      unique = [unique: [period: 60, by: by]]

      specs = [
        {Kathikon.Workers.SuccessWorker, %{"id" => 1}, unique},
        {Kathikon.Workers.SuccessWorker, %{"id" => 2, "bad" => true}, unique},
        {Kathikon.Workers.SuccessWorker, %{"id" => 3}, unique}
      ]

      assert {:ok, %{inserted: 2, errors: [{1, {:unique_key_failed, %RuntimeError{}}}]}} =
               Kathikon.insert_many(specs)

      assert length(Kathikon.Storage.all()) == 2
    end

    test "insert_many with on_error: :abort stops on a failing function" do
      by = fn _job -> raise "boom" end

      specs = [
        {Kathikon.Workers.SuccessWorker, %{"id" => 1}, [unique: [period: 60, by: by]]}
      ]

      assert {:error, {:unique_key_failed, %RuntimeError{}}} =
               Kathikon.insert_many(specs, on_error: :abort)

      assert Kathikon.Storage.all() == []
    end

    test "insert_many coalesces by function" do
      unique = [unique: [period: 60, by: fn job -> job.args["media_id"] end]]

      specs =
        for n <- 1..4 do
          {Kathikon.Workers.SuccessWorker, %{"media_id" => 1, "n" => n}, unique}
        end

      assert {:ok, %{inserted: 4, ids: ids, errors: []}} = Kathikon.insert_many(specs)
      assert length(Enum.uniq(ids)) == 1
    end
  end

  describe "option validation" do
    test ":keys together with :by raises" do
      assert_raise ArgumentError, ~r/cannot be used together/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, keys: [:args], by: fn _ -> 1 end]
        )
      end
    end

    test "a function of the wrong arity raises" do
      assert_raise ArgumentError, ~r/unique :by must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{},
          unique: [period: 60, by: fn _a, _b -> 1 end]
        )
      end
    end

    test "a non-function :by raises" do
      assert_raise ArgumentError, ~r/unique :by must be/, fn ->
        Kathikon.insert(Kathikon.Workers.SuccessWorker, %{}, unique: [period: 60, by: "nope"])
      end
    end
  end
end

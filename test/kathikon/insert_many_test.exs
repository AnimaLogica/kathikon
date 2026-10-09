defmodule Kathikon.InsertManyTest do
  use ExUnit.Case, async: false

  alias Kathikon.{Job, Storage}

  defmodule FallbackBackend do
    def insert(%Job{} = job), do: {:ok, job}
  end

  setup do
    Storage.setup()
    Storage.clear_jobs!()
    Kathikon.pause_queue(:default)
    Kathikon.pause_queue(:emails)

    on_exit(fn ->
      Kathikon.resume_queue(:default)
      Kathikon.resume_queue(:emails)
      Storage.clear_test_backend!()
    end)

    :ok
  end

  test "chunks writes and emits one inserted_many event per chunk" do
    ref = make_ref()
    parent = self()

    :telemetry.attach(
      "insert-many-chunks",
      [:kathikon, :job, :inserted_many],
      fn _event, measurements, _meta, _ ->
        send(parent, {ref, measurements.count})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach("insert-many-chunks") end)

    specs = Enum.map(1..5, fn n -> {Kathikon.Workers.SuccessWorker, %{"n" => n}} end)

    assert {:ok, %{inserted: 5, errors: []}} =
             Kathikon.insert_many(specs, chunk_size: 2)

    assert_receive {^ref, 2}
    assert_receive {^ref, 2}
    assert_receive {^ref, 1}
    refute_receive {^ref, _}
  end

  test "starts each queue once" do
    parent = self()

    tracer =
      spawn(fn ->
        loop = fn loop ->
          receive do
            {:trace, _, :call, {Kathikon.Queue, :ensure_started, [queue]}} ->
              send(parent, {:ensured, queue})
              loop.(loop)

            :done ->
              :ok
          end
        end

        loop.(loop)
      end)

    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    :erlang.trace_pattern({Kathikon.Queue, :ensure_started, 1}, true, [:local])

    on_exit(fn ->
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({Kathikon.Queue, :ensure_started, 1}, false, [:local])
      send(tracer, :done)
    end)

    specs = [
      {Kathikon.Workers.SuccessWorker, %{"n" => 1}, [queue: :default]},
      {Kathikon.Workers.SuccessWorker, %{"n" => 2}, [queue: :emails]},
      {Kathikon.Workers.SuccessWorker, %{"n" => 3}, [queue: :default]}
    ]

    assert {:ok, %{inserted: 3}} = Kathikon.insert_many(specs, chunk_size: 1)
    calls = ensure_started_calls([])

    assert Enum.count(calls, &(&1 == :default)) == 1
    assert Enum.count(calls, &(&1 == :emails)) == 1
  end

  test "duplicate ids are reported and skipped" do
    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, [])
    duplicate = %{job | id: job.id}

    assert {:ok, _} = Storage.insert(job)

    assert {:ok, %{ids: [], errors: [{0, {:already_exists, id}}]}} =
             Storage.insert_jobs([duplicate], on_error: :continue, chunk_size: 10)

    assert id == job.id
  end

  test "on_error abort rolls back the chunk" do
    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, [])
    assert {:ok, _} = Storage.insert(job)

    fresh = Job.build(Kathikon.Workers.SuccessWorker, %{"n" => 2}, [])

    assert {:error, {:already_exists, _}} =
             Storage.insert_jobs([fresh, %{fresh | id: job.id}],
               on_error: :abort,
               chunk_size: 10
             )

    assert {:error, :not_found} = Storage.fetch(fresh.id)
  end

  test "skips inserted history unless history: true" do
    assert {:ok, %{ids: [skipped]}} =
             Kathikon.insert_many([{Kathikon.Workers.SuccessWorker, %{"n" => 1}}])

    assert {:ok, []} = Kathikon.history(skipped)

    assert {:ok, %{ids: [recorded]}} =
             Kathikon.insert_many([{Kathikon.Workers.SuccessWorker, %{"n" => 2}}], history: true)

    assert {:ok, [%{event: :inserted}]} = Kathikon.history(recorded)
  end

  test "falls back to insert/1 when the backend omits insert_jobs/2" do
    Storage.set_test_backend!(FallbackBackend)

    job = Job.build(Kathikon.Workers.SuccessWorker, %{}, [])

    assert {:ok, %{ids: [id], errors: []}} = Storage.insert_jobs([job], [])
    assert id == job.id
  end

  defp ensure_started_calls(acc) do
    receive do
      {:ensured, queue} -> ensure_started_calls([queue | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end
end

defmodule Kathikon.Storage.Mnesia.IntegrationTest do
  @moduledoc false

  use ExUnit.Case, async: false

  @moduletag :integration

  alias Kathikon.{Job, Storage}
  alias Kathikon.Storage.Mnesia

  setup do
    ensure_mnesia!()
    :ok = Storage.setup()
    Storage.clear_jobs!()

    on_exit(fn ->
      ensure_mnesia!()
      Storage.setup()
      Storage.clear_jobs!()
      Kathikon.TestSupport.ensure_runtime!()
    end)

    :ok
  end

  defp ensure_mnesia! do
    case :mnesia.system_info(:is_running) do
      :yes -> :ok
      _ -> :mnesia.start()
    end
  end

  defp available_job(opts \\ []) do
    Job.build(Kathikon.Workers.SuccessWorker, %{}, opts)
    |> Map.put(:state, :available)
    |> Map.put(:available_at, DateTime.utc_now())
  end

  defp delete_table!(table) do
    case :mnesia.delete_table(table) do
      {:atomic, :ok} -> :ok
      :ok -> :ok
      {:aborted, {:no_exists, ^table}} -> :ok
      {:aborted, {:no_exists, ^table, _}} -> :ok
      other -> flunk("unexpected delete_table result: #{inspect(other)}")
    end
  end

  defp refute_table!(table) do
    if table in :mnesia.system_info(:tables) do
      flunk(
        "expected #{inspect(table)} to be absent, found: #{inspect(:mnesia.system_info(:tables))}"
      )
    end
  end

  test "setup initializes a brand-new mnesia schema" do
    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    :ok = :mnesia.delete_schema([node()])

    assert :ok = Mnesia.setup()
    assert :yes = :mnesia.system_info(:is_running)
    assert :kathikon_jobs in :mnesia.system_info(:tables)
  end

  test "setup succeeds with disc copies" do
    Application.put_env(:kathikon, :mnesia_copies, :disc)

    on_exit(fn -> Application.delete_env(:kathikon, :mnesia_copies) end)

    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    :ok = :mnesia.delete_schema([node()])

    assert :ok = Mnesia.setup()
    Kathikon.TestSupport.ensure_runtime!()
    assert :kathikon_jobs in :mnesia.system_info(:tables)
    assert node() in :mnesia.table_info(:kathikon_jobs, :disc_copies)
  end

  test "disc copies persist jobs across mnesia restarts" do
    Application.put_env(:kathikon, :mnesia_copies, :disc)

    on_exit(fn -> Application.delete_env(:kathikon, :mnesia_copies) end)

    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    assert :ok = Mnesia.reset!()
    Kathikon.TestSupport.ensure_runtime!()

    job = available_job()
    {:ok, inserted} = Storage.insert(job)

    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    :mnesia.start()
    :ok = :mnesia.wait_for_tables([:kathikon_jobs], 5_000)
    Kathikon.TestSupport.ensure_runtime!()

    assert {:ok, fetched} = Storage.fetch(inserted.id)
    assert fetched.id == inserted.id
  end

  test "claim returns error when job payload cannot be decoded" do
    now = DateTime.utc_now()

    :mnesia.transaction(fn ->
      :mnesia.write({:kathikon_jobs, "corrupt", <<"not-valid-erlang-term">>})
    end)

    assert {:error, _reason} = Storage.claim(:default, now)
  end

  test "clear_jobs! is a no-op when the jobs table does not exist" do
    Kathikon.TestSupport.stop_runtime!()

    on_exit(fn ->
      ensure_mnesia!()
      Storage.setup()
      Kathikon.TestSupport.ensure_runtime!()
    end)

    assert :kathikon_jobs in :mnesia.system_info(:tables)
    assert :ok = delete_table!(:kathikon_jobs)
    refute_table!(:kathikon_jobs)

    assert :ok = Mnesia.clear_jobs!()
    refute_table!(:kathikon_jobs)
  end

  test "reset! bootstraps storage when mnesia is stopped" do
    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    refute :mnesia.system_info(:is_running) == :yes

    assert :ok = Mnesia.reset!()
    assert :yes = :mnesia.system_info(:is_running)
    assert :kathikon_jobs in :mnesia.system_info(:tables)
  end

  test "setup recreates a dropped jobs table" do
    job = available_job()
    {:ok, _} = Storage.insert(job)
    :ok = delete_table!(:kathikon_jobs)

    assert :ok = Mnesia.setup()
    assert :kathikon_jobs in :mnesia.system_info(:tables)
    assert [] = Storage.all()
  end

  test "setup tolerates an existing mnesia schema" do
    assert {:error, {_, {:already_exists, _}}} = :mnesia.create_schema([node()])
    assert :ok = Mnesia.setup()
    assert :kathikon_jobs in :mnesia.system_info(:tables)
  end

  test "setup starts mnesia when it is not running" do
    Kathikon.TestSupport.stop_runtime!()
    :mnesia.stop()
    refute :mnesia.system_info(:is_running) == :yes

    assert :ok = Mnesia.setup()
    assert :yes = :mnesia.system_info(:is_running)
  end

  test "reset! deletes existing tables before recreating them" do
    job = available_job()
    {:ok, _} = Storage.insert(job)
    assert [_] = Storage.all()

    assert :ok = Mnesia.reset!()
    assert [] = Storage.all()
    assert :kathikon_jobs in :mnesia.system_info(:tables)
  end

  test "prunable_jobs uses cancelled_at for cancelled jobs" do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, 1, :second)

    job =
      available_job()
      |> Map.put(:state, :cancelled)
      |> Map.put(:cancelled_at, now)
      |> Map.put(:completed_at, nil)

    {:ok, _} = Storage.insert(job)
    assert [fetched] = Storage.prunable_jobs(cutoff)
    assert fetched.id == job.id
  end

  test "prunable_jobs uses inserted_at for discarded jobs without terminal timestamps" do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, 1, :second)

    job =
      available_job()
      |> Map.put(:state, :discarded)
      |> Map.put(:completed_at, nil)
      |> Map.put(:cancelled_at, nil)
      |> Map.put(:inserted_at, now)

    {:ok, _} = Storage.insert(job)
    assert [fetched] = Storage.prunable_jobs(cutoff)
    assert fetched.id == job.id
  end

  test "prunable_jobs excludes terminal jobs newer than the cutoff" do
    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -1, :second)

    job =
      available_job()
      |> Map.put(:state, :completed)
      |> Map.put(:completed_at, now)

    {:ok, _} = Storage.insert(job)
    assert [] = Storage.prunable_jobs(cutoff)
  end

  test "claim returns a retryable job when backoff has elapsed" do
    now = DateTime.utc_now()
    available_at = DateTime.add(now, -1, :second)

    job =
      available_job(queue: :default)
      |> Map.put(:state, :retryable)
      |> Map.put(:available_at, available_at)

    {:ok, _} = Storage.insert(job)

    assert {:ok, claimed} = Storage.claim(:default, now)
    assert claimed.id == job.id
    assert claimed.state == :running
  end

  test "promote_scheduled returns zero when no jobs are due" do
    now = DateTime.utc_now()
    future = DateTime.add(now, 3600, :second)

    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: :default)
      |> Map.put(:state, :scheduled)
      |> Map.put(:scheduled_at, future)

    {:ok, _} = Storage.insert(job)
    assert Storage.promote_scheduled(now) == 0
  end

  test "claim ignores jobs on other queues" do
    now = DateTime.utc_now()

    job =
      available_job(queue: :emails)
      |> Map.put(:available_at, now)

    {:ok, _} = Storage.insert(job)
    assert :not_found = Storage.claim(:default, now)
  end
end

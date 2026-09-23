defmodule Kathikon.LiveDashboard.PageTest do
  use ExUnit.Case, async: false

  import Mox

  alias Kathikon.{Dashboard, Job, Storage}
  alias Kathikon.LiveDashboard.Page

  setup :verify_on_exit!

  setup do
    Kathikon.TestSupport.ensure_runtime!()
    Storage.setup()
    Storage.clear_jobs!()
    Kathikon.resume_queue(:default)
    :ok
  end

  defp fetch(params, state \\ nil), do: Page.fetch_queues(params, :ignored, state)

  test "toggle_queue pauses and resumes one queue" do
    on_exit(fn -> Kathikon.resume_queue(:default) end)

    assert :ok = Page.toggle_queue(:default)
    assert %{paused: true} = Dashboard.queue_status(:default)

    assert :ok = Page.toggle_queue("default")
    assert %{paused: false} = Dashboard.queue_status(:default)

    assert :ok = Page.toggle_queue("not_a_queue_atom_name")
  end

  test "menu link is Kathikon" do
    assert Page.menu_link(%{}, %{}) == {:ok, "Kathikon"}
  end

  test "short id, worker label, and timestamp formatting" do
    assert Page.short_id("abcdef123456") == "123456"
    assert Page.worker_label(Kathikon.Workers.SuccessWorker) == "SuccessWorker"

    datetime = ~U[2026-06-23 19:20:00Z]
    assert Page.format_timestamp(datetime) == "2026-06-23 19:20:00"
    assert Page.format_timestamp(nil) == "—"
  end

  test "visible jobs honour the state filter and search text" do
    future = DateTime.add(DateTime.utc_now(), 3600, :second)

    {:ok, scheduled} =
      insert_state(:scheduled, Kathikon.Workers.SuccessWorker, %{"source" => "later"},
        scheduled_at: future
      )

    {:ok, completed} =
      insert_state(:completed, Kathikon.Workers.FailWorker, %{"source" => "done"})

    scheduled_rows = Page.visible_jobs(:available, "").jobs
    assert Enum.any?(scheduled_rows, &(&1.id == scheduled.id))
    refute Enum.any?(scheduled_rows, &(&1.id == completed.id))

    found = Page.visible_jobs(nil, "FailWorker").jobs
    assert Enum.any?(found, &(&1.id == completed.id))
    refute Enum.any?(found, &(&1.id == scheduled.id))
  end

  test "visible jobs paginate after 50 rows" do
    for n <- 1..51 do
      insert_state(:completed, Kathikon.Workers.SuccessWorker, %{"n" => n})
    end

    first = Page.visible_jobs(nil, "", 0)
    assert first.total >= 51
    assert length(first.jobs) == 50
    assert first.offset == 0

    second = Page.visible_jobs(nil, "", 50)
    assert length(second.jobs) == first.total - 50
    assert second.offset == 50
    refute Enum.any?(second.jobs, fn job -> Enum.any?(first.jobs, &(&1.id == job.id)) end)
  end

  defp insert_state(state, worker, args, extra \\ []) do
    job =
      worker
      |> Job.build(args, Keyword.merge([queue: :default], extra))
      |> Map.put(:state, state)

    job =
      if state == :scheduled do
        Map.put(job, :scheduled_at, Keyword.fetch!(extra, :scheduled_at))
      else
        job
      end

    Storage.insert(job)
  end

  test "a queue row includes ui_counts and the paused flag" do
    :ok = Dashboard.pause_queue(:default)
    on_exit(fn -> Kathikon.resume_queue(:default) end)

    {rows, total, _state} = fetch(%{})
    row = Enum.find(rows, &(&1.queue == :default))

    assert total >= 1
    assert row.paused == true
    assert row.ui_counts.available == row.available
    assert is_integer(row.ui_counts.executing)
    assert is_integer(row.failed)
    assert is_integer(row.total)
  end

  test "sort and limit are honoured" do
    {rows, total, _state} = fetch(%{sort_by: :queue, sort_dir: :asc, limit: 1})

    assert total >= 2
    assert length(rows) == 1
    assert hd(rows).queue == :default
  end

  test "sort_dir is honoured in both directions" do
    {asc, _, _} = fetch(%{sort_by: :queue, sort_dir: :asc})
    {desc, _, _} = fetch(%{sort_by: :queue, sort_dir: :desc})

    assert Enum.map(asc, & &1.queue) == Enum.sort(Enum.map(asc, & &1.queue))
    assert Enum.map(desc, & &1.queue) == Enum.sort(Enum.map(desc, & &1.queue), :desc)
  end

  test "string params from the query string work too" do
    {rows, _total, _state} = fetch(%{"sort_by" => :queue, "limit" => 1})
    assert hd(rows).queue == :default
  end

  test "a second fetch inside the TTL does not pick up a new queue" do
    {_rows, total, {_cached, fetched_at} = state} = fetch(%{})
    assert is_integer(fetched_at)

    queue = :"dash_new_#{System.unique_integer([:positive])}"

    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: queue)
      |> Map.put(:state, :available)

    {:ok, _} = Storage.insert(job)

    {rows, ^total, {_, ^fetched_at}} = fetch(%{sort_by: :queue}, state)
    refute Enum.any?(rows, &(&1.queue == queue))
  end

  test "a stale cache entry is refetched" do
    queue = :"dash_stale_#{System.unique_integer([:positive])}"

    job =
      Job.build(Kathikon.Workers.SuccessWorker, %{}, queue: queue)
      |> Map.put(:state, :available)

    {:ok, _} = Storage.insert(job)

    stale_at = System.monotonic_time(:millisecond) - 60_000
    {rows, total, {_, fetched_at}} = fetch(%{}, {[], stale_at})

    assert total >= 1
    assert fetched_at > stale_at
    assert Enum.any?(rows, &(&1.queue == queue))
  end

  test "queue_summary errors yield no rows" do
    parent = self()

    stub(Kathikon.Storage.Mock, :list_jobs, fn _ ->
      send(parent, :listed)
      {:error, :down}
    end)

    Kathikon.Storage.set_test_backend!(Kathikon.Storage.Mock)

    on_exit(fn -> Kathikon.Storage.clear_test_backend!() end)

    assert {[], 0, _state} = fetch(%{})
    assert_received :listed
  end

  test "mount, refresh, and events drive the page" do
    on_exit(fn -> Dashboard.resume_all() end)

    {:ok, _} = insert_state(:available, Kathikon.Workers.SuccessWorker, %{})
    retryable = insert_state(:retryable, Kathikon.Workers.FailWorker, %{})

    {:ok, socket} = Page.mount(%{}, %{}, %Phoenix.LiveView.Socket{})
    assert socket.assigns.jobs != []
    assert socket.assigns.all_paused == false

    {:noreply, paused} = Page.handle_event("toggle_pause_all", %{}, socket)
    assert paused.assigns.all_paused == true

    {:noreply, resumed} = Page.handle_event("toggle_pause_all", %{}, paused)
    assert resumed.assigns.all_paused == false

    {:noreply, _} = Page.handle_event("toggle_queue", %{"queue" => "default"}, resumed)
    assert %{paused: true} = Dashboard.queue_status(:default)
    {:noreply, _} = Page.handle_event("toggle_queue", %{"queue" => "default"}, resumed)

    {:noreply, filtered} = Page.handle_event("filter", %{"tab" => "available"}, resumed)
    assert filtered.assigns.filter == :available
    assert Enum.all?(filtered.assigns.jobs, &(&1.state in [:scheduled, :available]))

    {:noreply, cleared} = Page.handle_event("filter", %{"tab" => "available"}, filtered)
    assert cleared.assigns.filter == nil

    {:noreply, ignored} = Page.handle_event("filter", %{"tab" => "nope"}, cleared)
    assert ignored.assigns.filter == nil

    {:noreply, searched} = Page.handle_event("search", %{"query" => "FailWorker"}, cleared)
    assert searched.assigns.query == "FailWorker"
    assert Enum.any?(searched.assigns.jobs, &(&1.id == elem(retryable, 1).id))

    {:noreply, paged} = Page.handle_event("job_page", %{"offset" => "0"}, searched)
    assert paged.assigns.job_offset == 0

    {:noreply, _} = Page.handle_refresh(paged)

    :ok = Kathikon.pause_queue(:default)
    {:ok, available_job} = insert_state(:available, Kathikon.Workers.SuccessWorker, %{})
    {:noreply, killed} = Page.handle_event("kill_all", %{}, cleared)
    assert {:ok, %{state: :cancelled}} = Storage.fetch(available_job.id)

    {:ok, retryable_job} = insert_state(:retryable, Kathikon.Workers.FailWorker, %{})
    {:noreply, retried} = Page.handle_event("retry", %{"id" => retryable_job.id}, killed)
    assert {:ok, %{state: :available}} = Storage.fetch(retryable_job.id)

    html =
      retried.assigns
      |> Map.put(:page, %Phoenix.LiveDashboard.PageBuilder{allow_destructive_actions: true})
      |> Page.render()
      |> Phoenix.LiveViewTest.rendered_to_string()

    assert html =~ "Job queue summary"
    assert html =~ "Pause all"
  end

  test "dashboard markup covers pause, filters, empty jobs, and the pager" do
    import Phoenix.LiveViewTest

    queues = [
      %{
        queue: :default,
        paused: false,
        available: 1,
        executing: 0,
        retryable: 1,
        completed: 0,
        cancelled: 0,
        failed: 0,
        discarded: 0,
        total: 2
      },
      %{
        queue: :emails,
        paused: true,
        available: 0,
        executing: 0,
        retryable: 0,
        completed: 0,
        cancelled: 0,
        failed: 0,
        discarded: 0,
        total: 0
      }
    ]

    jobs = [
      %{
        id: "job-retryable-abcdef",
        state: :retryable,
        queue: :default,
        worker: Kathikon.Workers.FailWorker,
        attempts_label: "1/3",
        timestamp: ~U[2026-09-23 12:00:00Z]
      },
      %{
        id: "job-completed-abcdef",
        state: :completed,
        queue: :default,
        worker: "not-a-module",
        attempts_label: "1/1",
        timestamp: nil
      }
    ]

    base = %{
      queues: queues,
      jobs: jobs,
      filter: :available,
      query: "fail",
      all_paused: false,
      destructive: true,
      show_publisher: true,
      publisher_on: false,
      job_total: 60,
      job_offset: 0,
      job_limit: 50
    }

    running = render_component(&Page.dashboard/1, base)
    assert running =~ "Pause all"
    assert running =~ "Start publisher"
    assert running =~ "Pause"
    assert running =~ "Resume"
    assert running =~ "available"
    assert running =~ "abcdef"
    assert running =~ "Showing 1"

    paused =
      render_component(&Page.dashboard/1, %{
        base
        | all_paused: true,
          publisher_on: true,
          destructive: false,
          jobs: [],
          job_offset: 50
      })

    assert paused =~ "Resume all"
    assert paused =~ "Stop publisher"
    assert paused =~ "No jobs."
    assert paused =~ "disabled"
  end

  test "formatting and lookup helpers cover the remaining clauses" do
    assert Page.short_id(123_456) == "123456"
    assert Page.worker_label("plain") == inspect("plain")

    assert Page.visible_jobs(nil, "", "10").offset == 10
    assert Page.visible_jobs(nil, "", "nope").offset == 0
    assert Page.visible_jobs(nil, "", -1).offset == 0

    assert :ok = Page.toggle_queue(1)

    {_rows, _total, _state} = fetch(%{"sort_dir" => "desc"})
    {_rows, _total, _state} = fetch(:ignored)

    stub(Kathikon.Storage.Mock, :list_jobs_page, fn _ -> {:error, :down} end)
    Kathikon.Storage.set_test_backend!(Kathikon.Storage.Mock)
    on_exit(fn -> Kathikon.Storage.clear_test_backend!() end)

    assert %{jobs: [], total: 0} = Page.visible_jobs(:available, "x", 0)
  end

  test "clicking a job opens every field and close dismisses it" do
    {:ok, job} =
      insert_state(:completed, Kathikon.Workers.SuccessWorker, %{"note" => "copy-me"})

    detail = Page.job_detail(job.id)
    assert detail.job.id == job.id
    assert detail.text =~ job.id
    assert detail.text =~ "copy-me"

    keys = Enum.map(Page.detail_fields(detail.job), &elem(&1, 0))
    assert :args in keys
    assert :errors in keys
    assert :result_mode in keys
    assert Page.format_detail_value(nil) == "nil"
    assert Page.job_detail("missing-job") == nil

    {:ok, socket} = Page.mount(%{}, %{}, %Phoenix.LiveView.Socket{})
    {:noreply, open} = Page.handle_event("show_job", %{"id" => job.id}, socket)
    assert open.assigns.job_detail.job.id == job.id

    {:noreply, refreshed} = Page.handle_refresh(open)
    assert refreshed.assigns.job_detail.job.id == job.id

    html =
      refreshed.assigns
      |> Map.put(:page, %Phoenix.LiveDashboard.PageBuilder{allow_destructive_actions: true})
      |> Page.render()
      |> Phoenix.LiveViewTest.rendered_to_string()

    assert html =~ job.id
    assert html =~ "copy-me"
    assert html =~ "result_mode"

    {:noreply, closed} = Page.handle_event("close_job", %{}, open)
    assert closed.assigns.job_detail == nil
  end
end

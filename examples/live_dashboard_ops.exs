#!/usr/bin/env elixir
# Demo: Kathikon v0.3.0 ops control panel + LiveDashboard via Phoenix Playground.
#
# From the repo root:
#   iex examples/live_dashboard_ops.exs
#
# Livebook twin:
#   livebook server livebooks/live_dashboard.livemd
#
# Opens http://localhost:4000/ (control) and /dashboard/kathikon.

Mix.install(
  [
    {:kathikon, path: Path.expand("..", __DIR__)},
    {:phoenix_playground, "~> 0.1.9"},
    {:phoenix_live_dashboard, "~> 0.8"}
  ],
  config: [
    kathikon: [
      mnesia_copies: :ram,
      poll_interval: 150,
      queues: [default: [concurrency: 2]]
    ]
  ]
)

unless Code.ensure_loaded?(Kathikon.LiveDashboard.Page) do
  raise """
  Kathikon.LiveDashboard.Page did not compile.

  phoenix_live_dashboard must be installed before Kathikon compiles so the
  optional page module is available. Re-run this script after Mix.install.
  """
end

defmodule Demo.OkWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: :ok
end

defmodule Demo.FailWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: {:error, :demo_failure}
end

defmodule Demo.Publisher do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def running?, do: GenServer.call(__MODULE__, :running?)
  def start_publishing, do: GenServer.call(__MODULE__, :start)
  def stop_publishing, do: GenServer.call(__MODULE__, :stop)

  @impl true
  def init(opts) do
    {:ok, %{queue: Keyword.fetch!(opts, :queue), n: 0, timer: nil}}
  end

  @impl true
  def handle_call(:running?, _from, state), do: {:reply, state.timer != nil, state}

  def handle_call(:start, _from, %{timer: nil} = state) do
    {:reply, :ok, schedule(state)}
  end

  def handle_call(:start, _from, state), do: {:reply, :ok, state}

  def handle_call(:stop, _from, state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    {:reply, :ok, %{state | timer: nil}}
  end

  @impl true
  def handle_info(:tick, %{timer: nil} = state), do: {:noreply, state}

  def handle_info(:tick, state) do
    n = state.n + 1
    worker = if rem(n, 10) == 0, do: Demo.FailWorker, else: Demo.OkWorker

    _ =
      Kathikon.insert(worker, %{"n" => n, "source" => "publisher"}, queue: state.queue)

    {:noreply, schedule(%{state | n: n})}
  end

  defp schedule(state) do
    %{state | timer: Process.send_after(self(), :tick, 750)}
  end
end

defmodule Demo.ControlLive do
  @moduledoc false
  use Phoenix.LiveView

  @refresh_ms 1_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh, @refresh_ms)

    {:ok,
     socket
     |> assign(page_title: "Kathikon", filter: nil, query: "", job_offset: 0, job_detail: nil)
     |> assign_status()}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <main class="wrap">
      <p class="muted">
        Same view as <a href="/dashboard/kathikon">LiveDashboard → Kathikon</a>
      </p>
      <Kathikon.LiveDashboard.Page.dashboard
        queues={@queues}
        jobs={@jobs}
        filter={@filter}
        query={@query}
        all_paused={@all_paused}
        show_publisher={true}
        publisher_on={@publisher_on}
        job_total={@job_total}
        job_offset={@job_offset}
        job_limit={@job_limit}
        job_detail={@job_detail}
      />
    </main>
    <style type="text/css">
      body { margin: 0; background: #f4f6f8; }
      .wrap { max-width: 72rem; margin: 0 auto; padding: 1.25rem 1rem 3rem; }
      .muted { color: #5c5c5c; font-family: ui-sans-serif, system-ui, sans-serif; }
      a { color: #1a5276; }
    </style>
    """
  end

  @impl true
  def handle_event("toggle_publisher", _params, socket) do
    if Demo.Publisher.running?() do
      :ok = Demo.Publisher.stop_publishing()
    else
      :ok = Demo.Publisher.start_publishing()
    end

    {:noreply, assign_status(socket)}
  end

  def handle_event("toggle_pause_all", _params, socket) do
    if socket.assigns.all_paused do
      Kathikon.Dashboard.resume_all()
    else
      Kathikon.Dashboard.pause_all()
    end

    {:noreply, assign_status(socket)}
  end

  def handle_event("toggle_queue", %{"queue" => queue}, socket) do
    Kathikon.LiveDashboard.Page.toggle_queue(queue)
    {:noreply, assign_status(socket)}
  end

  def handle_event("kill_all", _params, socket) do
    Kathikon.Dashboard.cancel_jobs()
    {:noreply, assign_status(socket)}
  end

  def handle_event("filter", %{"tab" => tab}, socket) do
    filter =
      case tab do
        "available" -> :available
        "executing" -> :executing
        "retryable" -> :retryable
        "completed" -> :completed
        _ -> nil
      end

    filter = if socket.assigns.filter == filter, do: nil, else: filter
    {:noreply, socket |> assign(filter: filter, job_offset: 0) |> assign_status()}
  end

  def handle_event("search", %{"query" => query}, socket) do
    {:noreply, socket |> assign(query: query, job_offset: 0) |> assign_status()}
  end

  def handle_event("job_page", %{"offset" => offset}, socket) do
    {:noreply, socket |> assign(job_offset: parse_offset(offset)) |> assign_status()}
  end

  def handle_event("retry", %{"id" => id}, socket) do
    _ = Kathikon.Dashboard.retry_job(id)
    {:noreply, assign_status(socket)}
  end

  def handle_event("show_job", %{"id" => id}, socket) do
    {:noreply, assign(socket, :job_detail, Kathikon.LiveDashboard.Page.job_detail(id))}
  end

  def handle_event("close_job", _params, socket) do
    {:noreply, assign(socket, :job_detail, nil)}
  end

  @impl true
  def handle_info(:refresh, socket) do
    Process.send_after(self(), :refresh, @refresh_ms)
    {:noreply, assign_status(socket)}
  end

  defp assign_status(socket) do
    {queues, _total, _state} = Kathikon.LiveDashboard.Page.fetch_queues(%{}, nil, nil)

    page =
      Kathikon.LiveDashboard.Page.visible_jobs(
        socket.assigns.filter,
        socket.assigns.query,
        socket.assigns.job_offset
      )

    socket =
      assign(socket,
        publisher_on: publisher_on?(),
        queues: queues,
        all_paused: queues != [] and Enum.all?(queues, & &1.paused),
        jobs: page.jobs,
        job_total: page.total,
        job_offset: page.offset,
        job_limit: page.limit
      )

    case socket.assigns.job_detail do
      %{job: %{id: id}} when is_binary(id) ->
        assign(socket, :job_detail, Kathikon.LiveDashboard.Page.job_detail(id))

      _ ->
        socket
    end
  end

  defp parse_offset(offset) when is_integer(offset) and offset >= 0, do: offset

  defp parse_offset(offset) when is_binary(offset) do
    case Integer.parse(offset) do
      {n, ""} when n >= 0 -> n
      _ -> 0
    end
  end

  defp parse_offset(_), do: 0

  defp publisher_on? do
    is_pid(GenServer.whereis(Demo.Publisher)) and Demo.Publisher.running?()
  rescue
    _ -> false
  end
end

defmodule Demo.Router do
  use Phoenix.Router
  import Phoenix.LiveView.Router
  import Phoenix.LiveDashboard.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:put_root_layout, html: {PhoenixPlayground.Layout, :root})
    plug(:put_secure_browser_headers)
  end

  pipeline :dashboard do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  scope "/" do
    pipe_through(:browser)
    live("/", Demo.ControlLive)
  end

  scope "/" do
    pipe_through(:dashboard)

    live_dashboard("/dashboard",
      additional_pages: [kathikon: Kathikon.LiveDashboard.Page],
      allow_destructive_actions: true
    )
  end
end

defmodule Demo.Seed do
  @moduledoc false
  @queue :default

  def run do
    if seed_needed?() do
      future = DateTime.add(DateTime.utc_now(), 3_600, :second)

      {:ok, _} = insert_job(:completed, Demo.OkWorker, %{"source" => "completed"})

      {:ok, _} =
        insert_job(:scheduled, Demo.OkWorker, %{"source" => "scheduled"}, scheduled_at: future)

      {:ok, _} = insert_job(:dead, Demo.FailWorker, %{"source" => "dead"}, max_attempts: 1)
    end

    {:ok, rows} = Kathikon.Dashboard.queue_summary()
    row = Enum.find(rows, &(&1.queue == @queue))

    IO.puts("""

    Seeded #{@queue}:
      control:   http://localhost:4000/
      dashboard: http://localhost:4000/dashboard/kathikon
      total:     #{row && row.total}
    """)

    :ok
  end

  defp insert_job(state, worker, args, extra \\ []) do
    job =
      worker
      |> Kathikon.Job.build(args, Keyword.merge([queue: @queue], extra))
      |> Map.put(:state, state)

    job =
      if state == :scheduled do
        Map.put(job, :scheduled_at, Keyword.fetch!(extra, :scheduled_at))
      else
        job
      end

    Kathikon.Storage.insert(job)
  end

  defp seed_needed? do
    case Kathikon.Dashboard.list_jobs(queue: @queue, limit: 1) do
      {:ok, %{total: 0}} -> true
      _ -> false
    end
  end
end

if pid = GenServer.whereis(Demo.Publisher) do
  try do
    GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end
end

PhoenixPlayground.start(
  plug: Demo.Router,
  child_specs: [{Demo.Publisher, queue: :default}],
  port: 4000,
  open_browser: true,
  endpoint_options: [
    secret_key_base: "kathikon_live_dashboard_ops_demo_secret_key_base_at_least_64_bytes!"
  ]
)

Demo.Seed.run()

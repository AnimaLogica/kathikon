# Optional: compiled only when Phoenix LiveDashboard is present.
if Code.ensure_loaded?(Phoenix.LiveDashboard.PageBuilder) do
  defmodule Kathikon.LiveDashboard.Page do
    @moduledoc """
    LiveDashboard page for Kathikon queue health and job control.

    The queue table reads `Kathikon.Dashboard.queue_summary/0`. Pause, kill,
    and retry call `Kathikon.Dashboard` — they do not add a second control path.

    Destructive buttons follow LiveDashboard's `:allow_destructive_actions`
    flag. Enable them in the router:

        live_dashboard "/dashboard",
          additional_pages: [kathikon: Kathikon.LiveDashboard.Page],
          allow_destructive_actions: true

    Try without a host Phoenix app: `iex examples/live_dashboard_ops.exs` or
    `livebooks/live_dashboard.livemd`.

    Host setup: [LiveDashboard in a Phoenix app](live-dashboard.html).
    """

    use Phoenix.LiveDashboard.PageBuilder

    alias Kathikon.Dashboard

    @cache_ttl_ms 2_000
    @job_limit 50
    @filters [:available, :executing, :retryable, :completed]
    @job_fields [
      :id,
      :queue,
      :worker,
      :args,
      :state,
      :result,
      :error,
      :last_error,
      :parent_job_id,
      :batch_id,
      :rerun_of,
      :original_job_id,
      :claimant,
      :priority,
      :max_attempts,
      :attempts,
      :attempt,
      :scheduled_at,
      :available_at,
      :inserted_at,
      :claimed_at,
      :started_at,
      :completed_at,
      :failed_at,
      :discarded_at,
      :cancelled_at,
      :node,
      :errors,
      :result_mode
    ]

    @impl true
    def menu_link(_session, _capabilities) do
      {:ok, "Kathikon"}
    end

    @impl true
    def mount(_params, _session, socket) do
      {:ok,
       socket
       |> assign(filter: nil, query: "", job_offset: 0)
       |> load()}
    end

    @impl true
    def handle_refresh(socket) do
      {:noreply, load(socket)}
    end

    @impl true
    def handle_event("toggle_pause_all", _params, socket) do
      if socket.assigns.all_paused do
        Dashboard.resume_all()
      else
        Dashboard.pause_all()
      end

      {:noreply, load(socket)}
    end

    def handle_event("toggle_queue", %{"queue" => queue}, socket) do
      toggle_queue(queue)
      {:noreply, load(socket)}
    end

    def handle_event("kill_all", _params, socket) do
      _ = Dashboard.cancel_jobs()
      {:noreply, load(socket)}
    end

    def handle_event("filter", %{"tab" => tab}, socket) do
      filter = filter_tab(tab)

      filter =
        if socket.assigns.filter == filter do
          nil
        else
          filter
        end

      {:noreply, socket |> assign(filter: filter, job_offset: 0) |> load()}
    end

    def handle_event("search", %{"query" => query}, socket) do
      {:noreply, socket |> assign(query: query, job_offset: 0) |> load()}
    end

    def handle_event("job_page", %{"offset" => offset}, socket) do
      {:noreply, socket |> assign(job_offset: parse_offset(offset)) |> load()}
    end

    def handle_event("retry", %{"id" => id}, socket) do
      _ = Dashboard.retry_job(id)
      {:noreply, load(socket)}
    end

    def handle_event("show_job", %{"id" => id}, socket) do
      {:noreply, assign(socket, :job_detail, job_detail(id))}
    end

    def handle_event("close_job", _params, socket) do
      {:noreply, assign(socket, :job_detail, nil)}
    end

    @impl true
    def render(assigns) do
      assigns = assign(assigns, :destructive, assigns.page.allow_destructive_actions)

      ~H"""
      <.dashboard
        queues={@queues}
        jobs={@jobs}
        filter={@filter}
        query={@query}
        all_paused={@all_paused}
        destructive={@destructive}
        job_total={@job_total}
        job_offset={@job_offset}
        job_limit={@job_limit}
        job_detail={assigns[:job_detail]}
      />
      """
    end

    attr(:queues, :list, required: true)
    attr(:jobs, :list, required: true)
    attr(:filter, :any, default: nil)
    attr(:query, :string, default: "")
    attr(:all_paused, :boolean, default: false)
    attr(:destructive, :boolean, default: true)
    attr(:show_publisher, :boolean, default: false)
    attr(:publisher_on, :boolean, default: false)
    attr(:job_total, :integer, default: 0)
    attr(:job_offset, :integer, default: 0)
    attr(:job_limit, :integer, default: 50)
    attr(:job_detail, :any, default: nil)

    def dashboard(assigns) do
      assigns = assign(assigns, :filters, @filters)

      ~H"""
      <section class="kathikon-dash">
        <div class="kathikon-actions">
          <button
            type="button"
            phx-click="toggle_pause_all"
            class={if(@all_paused, do: "kathikon-btn resume", else: "kathikon-btn pause")}
            disabled={!@destructive}
          >
            {if @all_paused, do: "Resume all", else: "Pause all"}
          </button>
          <button
            type="button"
            phx-click="kill_all"
            class="kathikon-btn kill"
            disabled={!@destructive}
          >
            Kill all
          </button>
          <button
            :if={@show_publisher}
            type="button"
            phx-click="toggle_publisher"
            class={if(@publisher_on, do: "kathikon-btn kill", else: "kathikon-btn resume")}
          >
            {if @publisher_on, do: "Stop publisher", else: "Start publisher"}
          </button>
        </div>

        <h2>Job queue summary</h2>
        <div class="kathikon-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Queue</th>
                <th>Paused</th>
                <th class="num">Available</th>
                <th class="num">Executing</th>
                <th class="num">Retryable</th>
                <th class="num">Completed</th>
                <th class="num">Cancelled</th>
                <th class="num">Failed</th>
                <th class="num">Discarded</th>
                <th class="num">Total</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr :for={row <- @queues}>
                <td>{row.queue}</td>
                <td>{row.paused}</td>
                <td class="num">{row.available}</td>
                <td class="num">{row.executing}</td>
                <td class="num">{row.retryable}</td>
                <td class="num">{row.completed}</td>
                <td class="num">{row.cancelled}</td>
                <td class="num">{row.failed}</td>
                <td class="num">{row.discarded}</td>
                <td class="num">{row.total}</td>
                <td>
                  <button
                    type="button"
                    phx-click="toggle_queue"
                    phx-value-queue={row.queue}
                    class={if(row.paused, do: "kathikon-btn resume", else: "kathikon-btn pause")}
                    disabled={!@destructive}
                  >
                    {if row.paused, do: "Resume", else: "Pause"}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <nav class="kathikon-filters">
          <button
            :for={tab <- @filters}
            type="button"
            phx-click="filter"
            phx-value-tab={tab}
            class={if(@filter == tab, do: "on", else: nil)}
          >
            {tab}
          </button>
          <form phx-change="search" class="kathikon-search">
            <input
              type="text"
              name="query"
              value={@query}
              placeholder="Filter jobs"
              phx-debounce="200"
            />
          </form>
        </nav>

        <h2>Jobs</h2>
        <div class="kathikon-table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>State</th>
                <th>Queue</th>
                <th>Worker</th>
                <th>Attempts</th>
                <th>Timestamp</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr :for={job <- @jobs} class="kathikon-job" phx-click="show_job" phx-value-id={job.id}>
                <td class="mono" title={job.id}>{short_id(job.id)}</td>
                <td>{job.state}</td>
                <td>{job.queue}</td>
                <td>{worker_label(job.worker)}</td>
                <td>{job.attempts_label}</td>
                <td>{format_timestamp(job.timestamp)}</td>
                <td>
                  <button
                    type="button"
                    phx-click="retry"
                    phx-value-id={job.id}
                    class="kathikon-btn retry"
                    disabled={!@destructive or :retry not in Dashboard.actions_for_state(job.state)}
                  >
                    Retry
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
          <p :if={@jobs == []} class="kathikon-empty">No jobs.</p>
        </div>
        <div :if={@job_total > @job_limit} class="kathikon-pager">
          <span>
            Showing {@job_offset + 1}–{min(@job_offset + @job_limit, @job_total)} of {@job_total}
          </span>
          <button
            type="button"
            phx-click="job_page"
            phx-value-offset={max(@job_offset - @job_limit, 0)}
            disabled={@job_offset == 0}
          >
            Previous
          </button>
          <button
            type="button"
            phx-click="job_page"
            phx-value-offset={@job_offset + @job_limit}
            disabled={@job_offset + @job_limit >= @job_total}
          >
            Next
          </button>
        </div>
        <div :if={@job_detail} class="kathikon-modal" phx-window-keydown="close_job" phx-key="escape">
          <button type="button" class="kathikon-modal-backdrop" phx-click="close_job" aria-label="Close">
          </button>
          <div class="kathikon-modal-card">
            <div class="kathikon-modal-head">
              <h2>Job {@job_detail.job.id}</h2>
              <button type="button" class="kathikon-btn retry" phx-click="close_job">Close</button>
            </div>
            <table class="kathikon-detail-fields">
              <tbody>
                <tr :for={{key, value} <- detail_fields(@job_detail.job)}>
                  <th>{key}</th>
                  <td>{value}</td>
                </tr>
              </tbody>
            </table>
            <h3>History</h3>
            <p :if={@job_detail.history == []} class="kathikon-empty">No history events.</p>
            <pre :for={event <- @job_detail.history} class="kathikon-detail-event">{format_detail_value(event)}</pre>
            <h3>Copy</h3>
            <textarea class="kathikon-detail-copy" readonly rows="14" spellcheck="false">{@job_detail.text}</textarea>
          </div>
        </div>
      </section>

      <style type="text/css">
        .kathikon-dash { font-family: ui-sans-serif, system-ui, sans-serif; color: #1a1a1a; }
        .kathikon-dash h2 { font-size: 1.15rem; margin: 1.25rem 0 0.5rem; }
        .kathikon-actions { display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 0.75rem; }
        .kathikon-btn { border: 0; color: #fff; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; font-size: 0.75rem; padding: 0.45rem 0.7rem; cursor: pointer; }
        .kathikon-btn:disabled { opacity: 0.45; cursor: not-allowed; }
        .kathikon-btn.pause { background: #e67e22; }
        .kathikon-btn.resume { background: #1e8449; }
        .kathikon-btn.kill { background: #c0392b; }
        .kathikon-btn.retry { background: #1a5276; text-transform: none; letter-spacing: 0; font-weight: 600; }
        .kathikon-filters { display: flex; gap: 0.85rem; align-items: center; flex-wrap: wrap; margin: 0.85rem 0; }
        .kathikon-filters button { background: none; border: 0; color: #1a5276; cursor: pointer; padding: 0; font-size: 0.95rem; }
        .kathikon-filters button.on { font-weight: 700; text-decoration: underline; }
        .kathikon-search input { border: 1px solid #c5cdd6; padding: 0.3rem 0.5rem; min-width: 14rem; }
        .kathikon-table-wrap { overflow: auto; border: 1px solid #d5dbe3; background: #fff; }
        .kathikon-dash table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }
        .kathikon-dash th, .kathikon-dash td { padding: 0.45rem 0.7rem; border-bottom: 1px solid #e6ebf1; text-align: left; }
        .kathikon-dash th { background: #f7f9fb; font-weight: 600; }
        .kathikon-dash .num { text-align: right; }
        .kathikon-dash .mono { font-family: ui-monospace, monospace; }
        .kathikon-empty { margin: 0.5rem 0.7rem; color: #5c5c5c; }
        .kathikon-pager { display: flex; gap: 0.75rem; align-items: center; margin-top: 0.6rem; color: #3d3d3d; }
        .kathikon-pager button { border: 1px solid #c5cdd6; background: #fff; padding: 0.3rem 0.6rem; cursor: pointer; }
        .kathikon-pager button:disabled { opacity: 0.45; cursor: not-allowed; }
        .kathikon-job { cursor: pointer; }
        .kathikon-job:hover { background: #f4f8fb; }
        .kathikon-modal { position: fixed; inset: 0; z-index: 40; display: flex; align-items: flex-start; justify-content: center; padding: 2rem 1rem; }
        .kathikon-modal-backdrop { position: absolute; inset: 0; border: 0; background: rgba(20, 24, 28, 0.45); cursor: pointer; }
        .kathikon-modal-card { position: relative; z-index: 1; width: min(46rem, 100%); max-height: calc(100vh - 4rem); overflow: auto; background: #fff; border: 1px solid #d5dbe3; padding: 1rem 1.1rem 1.25rem; user-select: text; -webkit-user-select: text; }
        .kathikon-modal-head { display: flex; justify-content: space-between; gap: 1rem; align-items: center; }
        .kathikon-modal-head h2 { margin: 0; word-break: break-all; }
        .kathikon-detail-fields { margin-top: 0.75rem; }
        .kathikon-detail-fields th { width: 11rem; vertical-align: top; }
        .kathikon-detail-fields td, .kathikon-detail-event, .kathikon-detail-copy { font-family: ui-monospace, monospace; white-space: pre-wrap; word-break: break-word; user-select: text; -webkit-user-select: text; }
        .kathikon-detail-event { margin: 0 0 0.5rem; padding: 0.45rem 0.6rem; background: #f7f9fb; border: 1px solid #e6ebf1; }
        .kathikon-detail-copy { width: 100%; box-sizing: border-box; margin-top: 0.35rem; border: 1px solid #c5cdd6; padding: 0.5rem; }
      </style>
      """
    end

    @doc false
    def fetch_queues(params, _node, state) do
      {rows, state} = cached_rows(state)
      sorted = sort_rows(rows, params)

      {Enum.take(sorted, row_limit(params, length(sorted))), length(sorted), state}
    end

    @doc false
    def short_id(id) when is_binary(id), do: String.slice(id, -6, 6)
    def short_id(id), do: id |> to_string() |> short_id()

    @doc false
    def worker_label(worker) when is_atom(worker) do
      worker |> Module.split() |> List.last()
    end

    def worker_label(worker), do: inspect(worker)

    @doc false
    def format_timestamp(%DateTime{} = datetime) do
      Calendar.strftime(datetime, "%Y-%m-%d %H:%M:%S")
    end

    def format_timestamp(_), do: "—"

    @doc false
    def visible_jobs(filter, query, offset \\ 0) when is_binary(query) do
      offset = parse_offset(offset)

      opts =
        [limit: @job_limit, offset: offset, order: :newest]
        |> then(fn opts ->
          if filter, do: Keyword.put(opts, :tab, filter), else: opts
        end)

      case Dashboard.list_jobs(opts) do
        {:ok, %{jobs: jobs, total: total, offset: offset, limit: limit}} ->
          %{
            jobs: Enum.filter(jobs, &query_match?(&1, query)),
            total: total,
            offset: offset,
            limit: limit
          }

        _ ->
          %{jobs: [], total: 0, offset: offset, limit: @job_limit}
      end
    end

    defp load(socket) do
      queues = current_queues()
      page = visible_jobs(socket.assigns.filter, socket.assigns.query, socket.assigns.job_offset)

      assign(socket,
        queues: queues,
        all_paused: queues != [] and Enum.all?(queues, & &1.paused),
        jobs: page.jobs,
        job_total: page.total,
        job_offset: page.offset,
        job_limit: page.limit
      )
      |> refresh_open_detail()
    end

    defp parse_offset(offset) when is_integer(offset) and offset >= 0, do: offset

    defp parse_offset(offset) when is_binary(offset) do
      case Integer.parse(offset) do
        {n, ""} when n >= 0 -> n
        _ -> 0
      end
    end

    defp parse_offset(_), do: 0

    @doc false
    def job_detail(id) when is_binary(id) do
      case Dashboard.fetch_job(id) do
        {:ok, %{job: job, history: history}} ->
          %{
            job: job,
            history: history,
            text: detail_text(job, history)
          }

        _ ->
          nil
      end
    end

    @doc false
    def detail_fields(job) when is_map(job) do
      keys = @job_fields ++ (Map.keys(job) -- @job_fields)

      Enum.map(keys, fn key ->
        {key, format_detail_value(Map.get(job, key))}
      end)
    end

    @doc false
    def format_detail_value(nil), do: "nil"
    def format_detail_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
    def format_detail_value(value) when is_binary(value), do: value
    def format_detail_value(value) when is_atom(value), do: Atom.to_string(value)
    def format_detail_value(value) when is_integer(value), do: Integer.to_string(value)

    def format_detail_value(value),
      do: inspect(value, pretty: true, limit: :infinity, printable_limit: :infinity)

    defp detail_text(job, history) do
      """
      #{inspect(job, pretty: true, limit: :infinity, printable_limit: :infinity)}

      history:
      #{inspect(history, pretty: true, limit: :infinity, printable_limit: :infinity)}
      """
    end

    defp refresh_open_detail(socket) do
      case socket.assigns[:job_detail] do
        %{job: %{id: id}} when is_binary(id) -> assign(socket, :job_detail, job_detail(id))
        _ -> socket
      end
    end

    @doc false
    def toggle_queue(queue) do
      case queue_atom(queue) do
        nil ->
          :ok

        queue ->
          if Dashboard.queue_status(queue).paused do
            Dashboard.resume_queue(queue)
          else
            Dashboard.pause_queue(queue)
          end
      end
    end

    defp queue_atom(queue) when is_atom(queue), do: queue

    defp queue_atom(queue) when is_binary(queue) do
      String.to_existing_atom(queue)
    rescue
      ArgumentError -> nil
    end

    defp queue_atom(_), do: nil

    defp current_queues do
      {rows, _state} = fresh_rows()
      rows
    end

    defp query_match?(_job, ""), do: true

    defp query_match?(job, query) do
      needle = String.downcase(query)

      [job.id, job.state, job.queue, worker_label(job.worker)]
      |> Enum.map(&to_string/1)
      |> Enum.map(&String.downcase/1)
      |> Enum.any?(&String.contains?(&1, needle))
    end

    defp filter_tab(tab) when is_binary(tab) do
      case Enum.find(@filters, &(Atom.to_string(&1) == tab)) do
        nil -> nil
        filter -> filter
      end
    end

    defp cached_rows({rows, fetched_at}) do
      if monotonic_ms() - fetched_at < @cache_ttl_ms do
        {rows, {rows, fetched_at}}
      else
        fresh_rows()
      end
    end

    defp cached_rows(_state), do: fresh_rows()

    defp fresh_rows do
      rows = queue_rows()
      {rows, {rows, monotonic_ms()}}
    end

    defp monotonic_ms, do: System.monotonic_time(:millisecond)

    defp queue_rows do
      case Dashboard.queue_summary() do
        {:ok, rows} -> Enum.map(rows, &table_row/1)
        _ -> []
      end
    end

    defp table_row(row) do
      counts = row.ui_counts

      %{
        queue: row.queue,
        paused: row.paused,
        ui_counts: counts,
        available: counts.available,
        executing: counts.executing,
        retryable: counts.retryable,
        completed: counts.completed,
        cancelled: counts.cancelled,
        failed: counts.failed,
        discarded: counts.discarded,
        total: row.total
      }
    end

    defp sort_rows(rows, params) do
      field = param(params, :sort_by, :queue)
      dir = param(params, :sort_dir, :asc)

      Enum.sort_by(rows, &Map.get(&1, field, &1.queue), sort_sorter(dir))
    end

    defp row_limit(params, default) do
      case param(params, :limit, default) do
        n when is_integer(n) and n > 0 -> n
        _ -> default
      end
    end

    defp param(params, key, default) when is_map(params) do
      Map.get(params, key) || Map.get(params, Atom.to_string(key), default)
    end

    defp param(_params, _key, default), do: default

    defp sort_sorter(:desc), do: :desc
    defp sort_sorter("desc"), do: :desc
    defp sort_sorter(_), do: :asc
  end
end

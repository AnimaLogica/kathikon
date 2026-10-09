# Compare Kathikon.insert/3 with Kathikon.insert_many/2.
# Run: mix run examples/benchmark_bulk_enqueue.exs
#      mix run examples/benchmark_bulk_enqueue.exs -- 1000 5000
#      mix run examples/benchmark_bulk_enqueue.exs -- disc
#      mix run examples/benchmark_bulk_enqueue.exs -- disc 1000 5000

args = Enum.reject(System.argv(), &(&1 == "--"))
copies = if "disc" in args, do: :disc, else: :ram

# mix run starts the application before this script. Stop it so the copy
# type and, for disc, the schema directory are in place before setup.
if copies == :disc do
  dir = Path.join(System.tmp_dir!(), "kathikon-bench-#{System.unique_integer([:positive])}")
  File.mkdir_p!(dir)
  Application.put_env(:mnesia, :dir, String.to_charlist(dir))
end

Application.put_env(:kathikon, :mnesia_copies, copies)
_ = Application.stop(:kathikon)
_ = Application.stop(:mnesia)
{:ok, _} = Application.ensure_all_started(:kathikon)

actual =
  case copies do
    :disc -> :mnesia.table_info(:kathikon_jobs, :disc_copies)
    :ram -> :mnesia.table_info(:kathikon_jobs, :ram_copies)
  end

if node() not in actual do
  raise "expected :kathikon_jobs #{copies} copies on #{node()}, table info was #{inspect(actual)}"
end
Logger.configure(level: :error)
:ok = Kathikon.pause_queue(:default)

defmodule Example.BenchWorker do
  use Kathikon.Worker

  def perform(_job), do: :ok
end

counts =
  args
  |> Enum.filter(&String.match?(&1, ~r/^\d+$/))
  |> Enum.map(&String.to_integer/1)
  |> case do
    [] -> [1_000, 5_000]
    counts -> counts
  end

chunk_size = 500

warmup = fn ->
  Enum.each(1..20, fn n ->
    {:ok, _} = Kathikon.insert(Example.BenchWorker, %{"n" => n}, queue: :default)
  end)

  {:ok, %{inserted: 20}} =
    Kathikon.insert_many(
      Enum.map(1..20, &{Example.BenchWorker, %{"n" => &1}, [queue: :default]}),
      chunk_size: chunk_size
    )

  :ok = Kathikon.Storage.clear_jobs!()
end

time_insert = fn count ->
  :ok = Kathikon.Storage.clear_jobs!()

  {microseconds, :ok} =
    :timer.tc(fn ->
      Enum.each(1..count, fn n ->
        {:ok, _} = Kathikon.insert(Example.BenchWorker, %{"n" => n}, queue: :default)
      end)
    end)

  microseconds
end

time_many = fn count, opts ->
  :ok = Kathikon.Storage.clear_jobs!()

  specs = Enum.map(1..count, &{Example.BenchWorker, %{"n" => &1}, [queue: :default]})

  {microseconds, {:ok, %{inserted: ^count, errors: []}}} =
    :timer.tc(fn ->
      Kathikon.insert_many(specs, Keyword.merge([chunk_size: chunk_size], opts))
    end)

  microseconds
end

warmup.()

IO.puts("""
Bulk enqueue benchmark
queue :default paused, chunk_size #{chunk_size}, mnesia #{copies} copies
insert/3 writes one :inserted history row; insert_many/2 does not unless history: true
""")

cell = fn microseconds, count ->
  rate = round(count / (microseconds / 1_000_000))
  ms = :io_lib.format("~8.1f ms", [microseconds / 1_000]) |> IO.iodata_to_binary()
  "#{ms} #{String.pad_leading(Integer.to_string(rate), 6)}/s"
end

header =
  Enum.map_join(
    ["jobs", "insert/3", "insert_many", "history: true", "speedup"],
    "  ",
    &String.pad_leading(&1, 20)
  )

IO.puts(header)

Enum.each(counts, fn count ->
  single = time_insert.(count)
  many = time_many.(count, [])
  with_history = time_many.(count, history: true)
  speedup = :io_lib.format("~8.1fx", [single / many]) |> IO.iodata_to_binary()

  IO.puts(
    Enum.join(
      [
        String.pad_leading(Integer.to_string(count), 20),
        String.pad_leading(cell.(single, count), 20),
        String.pad_leading(cell.(many, count), 20),
        String.pad_leading(cell.(with_history, count), 20),
        String.pad_leading(speedup, 20)
      ],
      "  "
    )
  )
end)

:ok = Kathikon.Storage.clear_jobs!()
:ok = Kathikon.resume_queue(:default)

# Independent bulk enqueue
# Run: mix run examples/bulk_enqueue.exs

{:ok, _} = Application.ensure_all_started(:kathikon)

defmodule Example.BulkWorker do
  use Kathikon.Worker

  def perform(%{args: %{"n" => n}}) do
    IO.puts("job #{n}")
    :ok
  end
end

specs = Enum.map(1..5, fn n -> {Example.BulkWorker, %{"n" => n}, [queue: :default]} end)

{:ok, %{inserted: inserted, errors: errors}} =
  Kathikon.insert_many(specs, chunk_size: 2)

IO.inspect({inserted, errors}, label: "bulk enqueue")

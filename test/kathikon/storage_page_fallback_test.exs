defmodule Kathikon.StoragePageFallbackTest do
  use ExUnit.Case, async: false

  alias Kathikon.Storage

  defmodule Backend do
    @moduledoc false
    @behaviour Kathikon.Storage

    @optional_page [list_jobs: 1, list_jobs_page: 1]

    for {name, arity} <- Kathikon.Storage.behaviour_info(:callbacks) -- @optional_page do
      args = Macro.generate_arguments(arity, __MODULE__)
      def unquote(name)(unquote_splicing(args)), do: :ok
    end

    def list_jobs(_opts) do
      Process.get(:page_fallback_jobs, {:ok, []})
    end
  end

  setup do
    on_exit(fn -> Storage.clear_test_backend!() end)
    :ok
  end

  test "list_jobs_page filters and sorts when the backend has no page callback" do
    now = DateTime.utc_now()

    jobs = [
      %{id: "new", state: :completed, inserted_at: now, available_at: nil},
      %{
        id: "old",
        state: :completed,
        inserted_at: DateTime.add(now, -30, :second),
        available_at: nil
      },
      %{id: "open", state: :available, inserted_at: now, available_at: now},
      %{id: "blank", state: :completed, inserted_at: nil, available_at: nil}
    ]

    Storage.with_backend(Backend, fn ->
      Process.put(:page_fallback_jobs, {:ok, jobs})

      assert {:ok, %{jobs: [newest | _], total: 3}} =
               Storage.list_jobs_page(queue: :default, states: [:completed], order: :newest)

      assert newest.id == "blank"

      assert {:ok, %{jobs: [oldest | _], total: 3}} =
               Storage.list_jobs_page(states: [:completed], limit: 2, offset: 0, order: :oldest)

      assert oldest.id == "old"

      assert {:ok, %{total: 4}} = Storage.list_jobs_page([])

      Process.put(:page_fallback_jobs, {:error, :down})
      assert {:error, :down} = Storage.list_jobs_page(order: :oldest)
    end)
  end
end

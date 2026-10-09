defmodule Kathikon.Config do
  @moduledoc """
  Runtime configuration for Kathikon.

  Configure via `config :kathikon, ...` in `config/config.exs`.

  ## Example

      config :kathikon,
        queues: [default: [concurrency: 10], emails: [concurrency: 5]],
        poll_interval: 200,
        scheduler_interval: 1_000,
        prune_interval: 60_000,
        retention_period: :timer.hours(24 * 7),
        max_attempts: 20,
        insert_many_chunk_size: 500,
        mnesia_copies: :auto

  ## Reading at runtime

      Kathikon.Config.concurrency(:emails)
      Kathikon.Config.poll_interval()
      Kathikon.Config.mnesia_copies()

  See `docs/guides/configuration.md`.
  """

  @default_queues [default: [concurrency: 10]]
  @default_poll_interval 200
  @default_scheduler_interval 1_000
  @default_prune_interval 60_000
  @default_retention_period :timer.hours(24 * 7)
  @default_max_attempts 20
  @default_timezone "Etc/UTC"
  @default_insert_many_chunk_size 500

  def queues, do: get(:queues, @default_queues)

  def queue_names do
    queues() |> Keyword.keys()
  end

  def queue_config(queue) do
    Keyword.get(queues(), queue, concurrency: 10)
  end

  def concurrency(queue) do
    queue_config(queue) |> Keyword.get(:concurrency, 10)
  end

  def poll_interval, do: get(:poll_interval, @default_poll_interval)

  def scheduler_interval, do: get(:scheduler_interval, @default_scheduler_interval)

  def prune_interval, do: get(:prune_interval, @default_prune_interval)

  def retention_period, do: get(:retention_period, @default_retention_period)

  def max_attempts, do: get(:max_attempts, @default_max_attempts)

  @doc """
  Default number of jobs written in one `insert_many/2` storage transaction.

  ## Examples

      Kathikon.Config.insert_many_chunk_size()
      #=> 500
  """
  def insert_many_chunk_size do
    case get(:insert_many_chunk_size, @default_insert_many_chunk_size) do
      size when is_integer(size) and size > 0 -> size
      other -> raise ArgumentError, "invalid :insert_many_chunk_size #{inspect(other)}"
    end
  end

  @doc """
  IANA timezone for cron matching and naive `schedule_at` values.

  Defaults to `"Etc/UTC"`. Requires `config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase`.

  ## Examples

      Kathikon.Config.timezone()
      #=> "Etc/UTC"
  """
  def timezone, do: get(:timezone, @default_timezone)

  def scheduler_module, do: get(:scheduler, Kathikon.Scheduler.BuiltIn)

  def result_storage, do: get(:result, :store)

  @doc """
  Mnesia table copy type: `:ram` or `:disc`.

  Defaults to `:auto` — `ram` on `nonode@nohost` and Livebook nodes,
  `disc` on other named nodes.

  ## Examples

      Kathikon.Config.mnesia_copies()
      #=> :ram   # on nonode@nohost

      Kathikon.Config.concurrency(:emails)
      #=> 5
  """
  def mnesia_copies do
    case get(:mnesia_copies, :auto) do
      :auto -> auto_mnesia_copies()
      mode when mode in [:ram, :disc] -> mode
      other -> raise ArgumentError, invalid_mnesia_copies_message(other)
    end
  end

  defp invalid_mnesia_copies_message(mode) do
    "invalid :mnesia_copies #{inspect(mode)}, expected :auto, :ram, or :disc"
  end

  defp auto_mnesia_copies do
    if node() == :nonode@nohost or livebook_node?(), do: :ram, else: :disc
  end

  defp livebook_node? do
    node() |> Atom.to_string() |> String.contains?("livebook")
  end

  defp get(key, default) do
    Application.get_env(:kathikon, key, default)
  end
end

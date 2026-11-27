defmodule Kathikon.Telemetry do
  @moduledoc """
  Telemetry events emitted by Kathikon.

  All events are prefixed with `[:kathikon, ...]`.

  ## Examples

      :ok = Kathikon.Telemetry.attach_default_logger()

      :telemetry.attach("my-handler", [[:kathikon, :job, :completed]], fn event, measurements, metadata, _ ->
        IO.inspect({event, metadata.job_id})
      end)
  """

  require Logger

  @prefix [:kathikon]

  @doc false
  def event(suffix, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(@prefix ++ suffix, measurements, metadata)
  end

  @doc """
  Attaches a default logger handler for Kathikon telemetry events.

  ## Examples

      :ok = Kathikon.Telemetry.attach_default_logger()
  """
  @spec attach_default_logger() :: :ok | {:error, :already_exists}
  def attach_default_logger do
    events =
      for suffix <- [
            [:job, :inserted],
            [:job, :coalesced],
            [:job, :insert],
            [:job, :claimed],
            [:job, :started],
            [:job, :completed],
            [:job, :failed],
            [:job, :retried],
            [:job, :dead],
            [:job, :stop],
            [:job, :sleep],
            [:job, :retry],
            [:job, :discard],
            [:job, :cancel],
            [:job, :prune],
            [:batch, :started],
            [:batch, :completed],
            [:scheduler, :tick],
            [:scheduler, :fired],
            [:pruner, :tick],
            [:dispatcher, :poll]
          ] do
        @prefix ++ suffix
      end

    :telemetry.attach_many(
      "kathikon-default-logger",
      events,
      &__MODULE__.log_event/4,
      nil
    )
  end

  @doc false
  def log_event(event, measurements, metadata, _config) do
    event_name = Enum.join(event, ".")
    queue = Map.get(metadata, :queue, "-")
    job_id = Map.get(metadata, :job_id, "-")

    Logger.info(
      "[kathikon] #{event_name} queue=#{queue} job=#{job_id} #{inspect(measurements)} #{inspect(Map.drop(metadata, [:job]))}"
    )
  end
end

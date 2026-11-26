defmodule Kathikon.Workers.SuccessWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: :ok
end

defmodule Kathikon.Workers.UniqueMediaWorker do
  @moduledoc false
  use Kathikon.Worker, unique: [period: 60, keys: [:args]]

  @impl true
  def perform(_job), do: :ok
end

defmodule Kathikon.Workers.UniqueByWorker do
  @moduledoc false
  use Kathikon.Worker, unique: [period: 60, by: &__MODULE__.unique_key/1]

  @doc false
  def unique_key(job), do: job.args["media_id"]

  @impl true
  def perform(_job), do: :ok
end

defmodule Kathikon.Workers.UniqueByOtherWorker do
  @moduledoc false
  use Kathikon.Worker, unique: [period: 60, by: {Kathikon.Workers.UniqueByWorker, :unique_key}]

  @impl true
  def perform(_job), do: :ok
end

defmodule Kathikon.Workers.FailWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: {:error, :failed}
end

defmodule Kathikon.Workers.CountingWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(job) do
    if job.attempts + 1 >= 2 do
      :ok
    else
      {:error, :not_yet}
    end
  end
end

defmodule Kathikon.Workers.PriorityWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(job) do
    Kathikon.TestSupport.record_order(job.args["label"])
    :ok
  end
end

defmodule Kathikon.Workers.SleepWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(job) do
    {:sleep, job.args["seconds"] || 30}
  end
end

defmodule Kathikon.Workers.SleepOnceWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(job) do
    key = {__MODULE__, job.id}

    case :persistent_term.get(key, :pending) do
      :pending ->
        :persistent_term.put(key, :done)
        {:sleep, 1}

      :done ->
        :ok
    end
  end
end

defmodule Kathikon.Workers.RaiseWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: raise("boom")
end

defmodule Kathikon.Workers.ThrowWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: throw(:thrown)
end

defmodule Kathikon.Workers.ExitWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: exit(:shutdown)
end

defmodule Kathikon.Workers.ResultWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: {:ok, %{value: 42}}
end

defmodule Kathikon.Workers.DiscardWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: {:discard, :not_wanted}
end

defmodule Kathikon.Workers.ForceRetryWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: {:retry, :try_again}
end

defmodule Kathikon.Workers.MyWorker do
  @moduledoc false
  use Kathikon.Worker

  @impl true
  def perform(_job), do: :ok
end

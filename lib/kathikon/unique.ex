defmodule Kathikon.Unique do
  @moduledoc false

  alias Kathikon.Job

  # Oban's `:successful` intent: in-flight jobs and completed jobs block a
  # duplicate. Jobs that gave up (`:failed`, `:dead`) and jobs that were
  # cancelled or discarded do not, so a retry can replace them.
  @successful_states [
    :scheduled,
    :available,
    :claimed,
    :running,
    :retryable,
    :waiting_for_children,
    :completed
  ]

  @removed_options [:keys, :by]

  @type criteria :: (Job.t() -> term()) | {module(), atom()}
  @type timestamp :: :inserted_at | :scheduled_at
  @type period :: pos_integer() | :infinity

  @typedoc "Resolved uniqueness options, before the fingerprint is computed."
  @type t :: %{
          period: period(),
          criteria: criteria(),
          timestamp: timestamp(),
          states: [atom()],
          on_conflict: :coalesce | :error
        }

  @typedoc "What storage receives: no user code, just the precomputed fingerprint."
  @type prepared :: %{
          fingerprint: String.t(),
          period: period(),
          timestamp: timestamp(),
          states: [atom()],
          on_conflict: :coalesce | :error
        }

  @doc false
  @spec resolve(module(), keyword()) :: false | t()
  def resolve(worker, opts) when is_atom(worker) and is_list(opts) do
    case Keyword.fetch(opts, :unique) do
      {:ok, false} -> false
      {:ok, unique} when is_list(unique) -> normalize!(unique)
      :error -> from_worker(worker)
    end
  end

  @doc false
  @spec from_worker(module()) :: false | t()
  def from_worker(worker) when is_atom(worker) do
    _ = Code.ensure_loaded(worker)

    if function_exported?(worker, :unique_opts, 0) do
      case worker.unique_opts() do
        nil -> false
        false -> false
        unique when is_list(unique) -> normalize!(unique)
      end
    else
      false
    end
  end

  @doc false
  @spec normalize!(keyword()) :: t()
  def normalize!(opts) when is_list(opts) do
    reject_removed_options!(opts)

    period = Keyword.fetch!(opts, :period)
    timestamp = Keyword.get(opts, :timestamp, :inserted_at)
    states = normalize_states!(Keyword.get(opts, :states, :successful))
    criteria = Keyword.get(opts, :criteria, {__MODULE__, :default_criteria})
    on_conflict = Keyword.get(opts, :on_conflict, :coalesce)

    validate_period!(period)
    validate_timestamp!(timestamp)
    validate_criteria!(criteria)
    validate_on_conflict!(on_conflict)

    %{
      period: period,
      criteria: criteria,
      timestamp: timestamp,
      states: states,
      on_conflict: on_conflict
    }
  end

  @doc """
  Default criteria when `unique:` omits `:criteria`: the queue and the args.
  The worker module is always hashed on top of this.
  """
  @spec default_criteria(Job.t()) :: {atom(), map()}
  def default_criteria(%Job{queue: queue, args: args}), do: {queue, args}

  @doc """
  Computes the fingerprint for `job` and returns the map storage needs.

  Runs any user `:criteria` function here, in the caller, so it never executes
  inside a storage transaction. Exceptions, throws and exits from that function
  become `{:error, {:unique_key_failed, reason}}`.
  """
  @spec prepare(Job.t(), false | t()) ::
          {:ok, false | prepared()} | {:error, {:unique_key_failed, term()}}
  def prepare(%Job{}, false), do: {:ok, false}

  def prepare(%Job{} = job, %{} = unique) do
    case term_for(job, unique) do
      {:ok, term} ->
        {:ok,
         %{
           fingerprint: hash({job.worker, term}),
           period: unique.period,
           timestamp: unique.timestamp,
           states: unique.states,
           on_conflict: unique.on_conflict
         }}

      {:error, reason} ->
        {:error, {:unique_key_failed, reason}}
    end
  end

  @doc false
  @spec conflict?(Job.t(), prepared(), DateTime.t()) :: boolean()
  def conflict?(%Job{} = job, unique, now) do
    job.state in unique.states and
      within_period?(anchor(job, unique.timestamp), unique.period, now)
  end

  defp term_for(job, %{criteria: criteria}) do
    {:ok, call_criteria(criteria, job)}
  rescue
    exception -> {:error, exception}
  catch
    kind, value -> {:error, {kind, value}}
  end

  defp call_criteria({module, function}, job), do: apply(module, function, [job])
  defp call_criteria(fun, job) when is_function(fun, 1), do: fun.(job)

  defp hash(term) do
    :crypto.hash(:sha256, :erlang.term_to_binary(term, [:deterministic]))
    |> Base.encode16(case: :lower)
  end

  defp anchor(%Job{} = job, :inserted_at), do: job.inserted_at
  defp anchor(%Job{scheduled_at: nil} = job, :scheduled_at), do: job.inserted_at
  defp anchor(%Job{scheduled_at: at}, :scheduled_at), do: at

  defp within_period?(nil, _period, _now), do: false
  defp within_period?(_at, :infinity, _now), do: true
  defp within_period?(at, period, now), do: DateTime.diff(now, at, :second) < period

  defp reject_removed_options!(opts) do
    Enum.each(@removed_options, fn key ->
      if Keyword.has_key?(opts, key) do
        raise ArgumentError, "unique :#{key} was removed; use criteria: instead"
      end
    end)
  end

  defp normalize_states!(:successful), do: @successful_states
  defp normalize_states!(states) when is_list(states) and states != [], do: states

  defp normalize_states!(other) do
    raise ArgumentError,
          "unique :states must be :successful or a non-empty list, got: #{inspect(other)}"
  end

  defp validate_criteria!(fun) when is_function(fun, 1), do: :ok
  defp validate_criteria!({module, function}) when is_atom(module) and is_atom(function), do: :ok

  defp validate_criteria!(other) do
    raise ArgumentError,
          "unique :criteria must be a function of arity 1 or a {module, function} pair, " <>
            "got: #{inspect(other)}"
  end

  defp validate_period!(:infinity), do: :ok
  defp validate_period!(period) when is_integer(period) and period > 0, do: :ok

  defp validate_period!(other) do
    raise ArgumentError,
          "unique :period must be a positive integer or :infinity, got: #{inspect(other)}"
  end

  defp validate_timestamp!(ts) when ts in [:inserted_at, :scheduled_at], do: :ok

  defp validate_timestamp!(other) do
    raise ArgumentError,
          "unique :timestamp must be :inserted_at or :scheduled_at, got: #{inspect(other)}"
  end

  defp validate_on_conflict!(:coalesce), do: :ok
  defp validate_on_conflict!(:error), do: :ok

  defp validate_on_conflict!(other) do
    raise ArgumentError,
          "unique :on_conflict must be :coalesce or :error, got: #{inspect(other)}"
  end
end

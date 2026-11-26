defmodule Kathikon.Unique do
  @moduledoc false

  alias Kathikon.Job

  @default_states [
    :scheduled,
    :available,
    :claimed,
    :running,
    :retryable,
    :waiting_for_children
  ]

  @type by :: (Job.t() -> term()) | {module(), atom()}

  @typedoc "Resolved uniqueness options, before the fingerprint is computed."
  @type t :: %{
          period: pos_integer(),
          keys: [atom()] | nil,
          by: by() | nil,
          states: [atom()],
          on_conflict: :coalesce | :error
        }

  @typedoc "What storage receives: no user code, just the precomputed fingerprint."
  @type prepared :: %{
          fingerprint: String.t(),
          period: pos_integer(),
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
    period = Keyword.fetch!(opts, :period)
    states = Keyword.get(opts, :states, @default_states)
    on_conflict = Keyword.get(opts, :on_conflict, :coalesce)
    {keys, by} = key_source!(opts)

    validate_period!(period)
    validate_states!(states)
    validate_on_conflict!(on_conflict)

    %{period: period, keys: keys, by: by, states: states, on_conflict: on_conflict}
  end

  @doc """
  Computes the fingerprint for `job` and returns the map storage needs.

  Runs any user `:by` function here, in the caller, so it never executes inside
  a storage transaction. Exceptions, throws and exits from that function become
  `{:error, {:unique_key_failed, reason}}`.
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
    job.state in unique.states and within_period?(job.inserted_at, unique.period, now)
  end

  defp term_for(job, %{by: nil, keys: keys}) do
    {:ok, Enum.reduce(keys, %{}, &put_key(&1, &2, job))}
  end

  defp term_for(job, %{by: by}) do
    {:ok, call_by(by, job)}
  rescue
    exception -> {:error, exception}
  catch
    kind, value -> {:error, {kind, value}}
  end

  defp call_by(fun, job) when is_function(fun, 1), do: fun.(job)
  defp call_by({module, function}, job), do: apply(module, function, [job])

  defp hash(term) do
    :crypto.hash(:sha256, :erlang.term_to_binary(term, [:deterministic]))
    |> Base.encode16(case: :lower)
  end

  defp put_key(:args, acc, job), do: Map.put(acc, :args, job.args)
  defp put_key(:queue, acc, job), do: Map.put(acc, :queue, job.queue)
  defp put_key(:worker, acc, job), do: Map.put(acc, :worker, job.worker)

  defp put_key(other, _acc, _job) do
    raise ArgumentError, "unsupported unique key: #{inspect(other)}"
  end

  defp within_period?(nil, _period, _now), do: false

  defp within_period?(inserted_at, period, now) do
    DateTime.diff(now, inserted_at, :second) < period
  end

  defp key_source!(opts) do
    has_keys? = Keyword.has_key?(opts, :keys)
    has_by? = Keyword.has_key?(opts, :by)

    cond do
      has_keys? and has_by? ->
        raise ArgumentError, "unique :keys and :by cannot be used together"

      has_by? ->
        {nil, validate_by!(Keyword.fetch!(opts, :by))}

      true ->
        keys = Keyword.get(opts, :keys, [:args])
        validate_keys!(keys)
        {keys, nil}
    end
  end

  defp validate_by!(fun) when is_function(fun, 1), do: fun

  defp validate_by!({module, function} = by) when is_atom(module) and is_atom(function), do: by

  defp validate_by!(other) do
    raise ArgumentError,
          "unique :by must be a function of arity 1 or a {module, function} pair, " <>
            "got: #{inspect(other)}"
  end

  defp validate_period!(period) when is_integer(period) and period > 0, do: :ok

  defp validate_period!(other) do
    raise ArgumentError, "unique :period must be a positive integer, got: #{inspect(other)}"
  end

  defp validate_keys!(keys) when is_list(keys) and keys != [], do: :ok

  defp validate_keys!(other) do
    raise ArgumentError, "unique :keys must be a non-empty list, got: #{inspect(other)}"
  end

  defp validate_states!(states) when is_list(states) and states != [], do: :ok

  defp validate_states!(other) do
    raise ArgumentError, "unique :states must be a non-empty list, got: #{inspect(other)}"
  end

  defp validate_on_conflict!(:coalesce), do: :ok
  defp validate_on_conflict!(:error), do: :ok

  defp validate_on_conflict!(other) do
    raise ArgumentError,
          "unique :on_conflict must be :coalesce or :error, got: #{inspect(other)}"
  end
end

defmodule Kathikon.Job do
  @moduledoc """
  Represents a durable job obligation in Kathikon.

  Jobs move through explicit states enforced by `Kathikon.Job.StateMachine`:

    * `:scheduled` — waiting until `scheduled_at`
    * `:available` — ready to be claimed
    * `:claimed` — atomically claimed, not yet running
    * `:running` — currently being processed (v0.1 `:executing`)
    * `:retryable` — failed but will be retried after backoff
    * `:waiting_for_children` — batch parent waiting on child jobs
    * `:completed` — successfully finished
    * `:failed` — exhausted retries or terminal worker failure
    * `:dead` — in the dead-letter queue
    * `:cancelled` — explicitly cancelled
    * `:discarded` — permanently discarded

  See `docs/job_lifecycle.md`.
  """

  alias Kathikon.Job.StateMachine

  @enforce_keys [:id, :queue, :worker, :args, :state]
  defstruct [
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
    priority: 0,
    max_attempts: 20,
    attempts: 0,
    scheduled_at: nil,
    available_at: nil,
    inserted_at: nil,
    claimed_at: nil,
    started_at: nil,
    completed_at: nil,
    failed_at: nil,
    discarded_at: nil,
    cancelled_at: nil,
    node: nil,
    errors: [],
    result_mode: :store
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          queue: atom(),
          worker: module(),
          args: map(),
          state:
            :scheduled
            | :available
            | :claimed
            | :running
            | :executing
            | :retryable
            | :waiting_for_children
            | :completed
            | :failed
            | :dead
            | :cancelled
            | :discarded,
          result: term(),
          error: term(),
          last_error: term(),
          parent_job_id: String.t() | nil,
          batch_id: String.t() | nil,
          rerun_of: String.t() | nil,
          original_job_id: String.t() | nil,
          claimant: map() | nil,
          priority: non_neg_integer(),
          max_attempts: pos_integer(),
          attempts: non_neg_integer(),
          scheduled_at: DateTime.t() | nil,
          available_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          claimed_at: DateTime.t() | nil,
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          failed_at: DateTime.t() | nil,
          discarded_at: DateTime.t() | nil,
          cancelled_at: DateTime.t() | nil,
          node: node() | nil,
          errors: [map()],
          result_mode: :store | :discard
        }

  @doc false
  def states, do: StateMachine.states() ++ [:executing]

  @doc """
  Builds a new job from worker module, args, and options.

  Prefer `Kathikon.insert/3` for enqueueing — it persists the job and starts
  the queue dispatcher.

  ## Examples

      job = Kathikon.Job.build(MyApp.EmailWorker, %{"to" => "a@b.com"},
        queue: :emails,
        priority: 2,
        schedule_in: 60
      )

      job.state
      #=> :scheduled
  """
  @spec build(module(), map(), keyword()) :: t()
  def build(worker, args, opts) do
    now = DateTime.utc_now()
    queue = Keyword.get(opts, :queue, :default)
    priority = Keyword.get(opts, :priority, 0)
    max_attempts = Keyword.get(opts, :max_attempts, Kathikon.Config.max_attempts())
    result_mode = Keyword.get(opts, :result, :store)

    {state, scheduled_at, available_at} = schedule_fields(opts, now)

    %__MODULE__{
      id: Keyword.get(opts, :id) || generate_id(),
      queue: queue,
      worker: worker,
      args: args,
      state: state,
      priority: priority,
      max_attempts: max_attempts,
      attempts: 0,
      scheduled_at: scheduled_at,
      available_at: available_at,
      inserted_at: now,
      errors: [],
      result_mode: result_mode,
      original_job_id: Keyword.get(opts, :original_job_id),
      rerun_of: Keyword.get(opts, :rerun_of),
      parent_job_id: Keyword.get(opts, :parent_job_id),
      batch_id: Keyword.get(opts, :batch_id)
    }
  end

  @doc """
  Returns true when the job can be claimed for execution at `now`.

  ## Examples

      job = Kathikon.Job.build(MyWorker, %{})
      Kathikon.Job.claimable?(job, DateTime.utc_now())
      #=> true

      scheduled = Kathikon.Job.build(MyWorker, %{}, schedule_in: 3600)
      Kathikon.Job.claimable?(scheduled, DateTime.utc_now())
      #=> false
  """
  @spec claimable?(t(), DateTime.t()) :: boolean()
  def claimable?(%__MODULE__{state: state, available_at: available_at}, now) do
    state in [:available, :retryable] and
      available_at != nil and
      DateTime.compare(available_at, now) != :gt
  end

  @doc """
  Normalizes legacy `:executing` to `:running`.
  """
  @spec normalize(t()) :: t()
  def normalize(%__MODULE__{state: :executing} = job), do: %{job | state: :running}
  def normalize(%__MODULE__{} = job), do: job

  @doc """
  Computes exponential backoff in seconds for the given attempt number.

  ## Examples

      Kathikon.Job.backoff_seconds(1)
      #=> 5

      Kathikon.Job.backoff_seconds(3)
      #=> 45
  """
  @spec backoff_seconds(non_neg_integer()) :: non_neg_integer()
  def backoff_seconds(attempt) when attempt <= 0, do: 1

  def backoff_seconds(attempt) do
    min(trunc(:math.pow(attempt, 2) * 5), 86_400)
  end

  @doc false
  def to_record(%__MODULE__{} = job) do
    {:kathikon_jobs, job.id, :erlang.term_to_binary(normalize(job))}
  end

  @doc false
  def decode_payload(binary) when is_binary(binary) do
    binary
    |> :erlang.binary_to_term([:safe])
    |> normalize()
  end

  @doc false
  def from_record({:kathikon_jobs, id, binary}) when is_binary(binary) do
    job = decode_payload(binary)
    %{job | id: id}
  end

  @doc """
  Converts a job struct to a map for storage callbacks and reporting.

  ## Examples

      Kathikon.Job.to_map(job)
      #=> %{id: "...", state: :completed, worker: MyWorker, ...}
  """
  @spec to_map(t()) :: %{atom() => term()}
  def to_map(%__MODULE__{} = job) do
    job
    |> normalize()
    |> Map.from_struct()
    |> Map.put(:attempt, job.attempts)
  end

  @doc """
  Returns job history events from storage.

  Prefer `Kathikon.history/1` in application code.

  ## Examples

      {:ok, events} = Kathikon.Job.history(job_id)
  """
  @spec history(String.t()) :: {:ok, [map()]} | {:error, term()}
  def history(job_id) when is_binary(job_id) do
    Kathikon.Storage.list_history(job_id)
  end

  defp schedule_fields(opts, now) do
    cond do
      schedule_at = Keyword.get(opts, :schedule_at) ->
        {:ok, at_utc} = Kathikon.Timezone.normalize_schedule_at(schedule_at)

        if DateTime.compare(at_utc, now) == :gt do
          {:scheduled, at_utc, at_utc}
        else
          {:available, at_utc, now}
        end

      schedule_in = Keyword.get(opts, :schedule_in) ->
        at = DateTime.add(now, schedule_in, :second)
        {:scheduled, at, at}

      true ->
        {:available, now, now}
    end
  end

  defp generate_id do
    Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end

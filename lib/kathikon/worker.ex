defmodule Kathikon.Worker do
  @moduledoc """
  Behaviour for Kathikon job workers.

  ## Example

      defmodule MyApp.EmailWorker do
        use Kathikon.Worker

        @impl true
        def perform(%Kathikon.Job{args: %{"to" => email}}) do
          MyApp.Mailer.deliver(email)
          :ok
        end
      end

      {:ok, _job} = Kathikon.insert(MyApp.EmailWorker, %{"to" => "user@example.com"})

  ## Uniqueness

  Pass `unique:` to coalesce duplicate enqueues (Oban-compatible subset):

      defmodule MyApp.MediaProcess do
        use Kathikon.Worker, unique: [period: 60, keys: [:args]]

        @impl true
        def perform(%Kathikon.Job{}), do: :ok
      end

  Use `by:` instead of `keys:` to fingerprint whatever a function of the built
  `%Kathikon.Job{}` returns:

      use Kathikon.Worker,
        unique: [period: 60, by: &__MODULE__.unique_key/1]

      def unique_key(job), do: {job.args["kind"], job.args["media_id"]}

  In `use`, pass an external capture (`&Mod.fun/1`) or a `{module, function}`
  pair. Anonymous closures cannot be stored in a module attribute, but work as
  an insert-time `unique:` option.

  Insert-level `unique:` overrides the worker default; `unique: false` disables it.
  See `docs/guides/workers.md`.

  ## Return values

    * `:ok` — success; result stored per `result: :store | :discard`
    * `{:ok, result}` — success with explicit result
    * `{:error, reason}` — failure; retry with backoff until max attempts, then dead-letter
    * `{:discard, reason}` — permanent discard (no retry)
    * `{:retry, reason}` — failure treated as retryable even when attempts remain
    * `{:sleep, seconds}` — defer without counting as failure

  See `docs/guides/workers.md`.
  """

  @callback perform(job :: Kathikon.Job.t()) ::
              :ok
              | {:ok, term()}
              | {:error, term()}
              | {:discard, term()}
              | {:retry, term()}
              | {:sleep, pos_integer()}

  @doc """
  Declares the `Kathikon.Worker` behaviour.

  ## Options

    * `:unique` — default uniqueness for `Kathikon.insert/3` / `insert_many/2`
      (`[period: pos_integer(), keys: [:args]]`, `[period: ..., by: fun]`, or `false`)
  """
  defmacro __using__(opts \\ []) do
    unique = Keyword.get(opts, :unique)

    quote do
      @behaviour Kathikon.Worker
      @kathikon_unique unquote(unique)

      @doc false
      def unique_opts, do: @kathikon_unique

      defoverridable unique_opts: 0
    end
  end
end

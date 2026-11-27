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

  Pass `unique:` to coalesce duplicate enqueues. `criteria:` is a function of
  the built `%Kathikon.Job{}` whose return value is hashed, together with the
  worker module:

      defmodule MyApp.MediaProcess do
        use Kathikon.Worker,
          unique: [period: 60, criteria: fn job -> job.args["media_id"] end]

        @impl true
        def perform(%Kathikon.Job{}), do: :ok
      end

  `criteria:` may also be an external capture (`&Mod.fun/1`) or a
  `{module, function}` pair. Omitted, it defaults to `{queue, args}`.

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
      (`[period: ..., criteria: fun]` or `false`)
  """
  defmacro __using__(opts \\ []) do
    unique = Keyword.get(opts, :unique)

    quote do
      @behaviour Kathikon.Worker

      # The option AST is evaluated inside the function body, not stored in a
      # module attribute, so anonymous criteria functions are allowed.
      @doc false
      def unique_opts, do: unquote(unique)

      defoverridable unique_opts: 0
    end
  end
end

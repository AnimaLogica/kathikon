defmodule Kathikon.MixProject do
  use Mix.Project

  @version "0.3.0"

  def project do
    [
      app: :kathikon,
      version: @version,
      elixir: "~> 1.18",
      name: "Kathikon",
      description: "BEAM-native durable job queue and task execution platform",
      package: package(),
      docs: docs(),
      dialyzer: dialyzer(),
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        sobelow: :dev
      ],
      aliases: [verify: &verify/1],
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      mod: {Kathikon.Application, []},
      extra_applications: [:logger, :mnesia, :telemetry, :tzdata]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.2"},
      {:tzdata, "~> 1.1"},
      {:quantum, "~> 3.5", optional: true},
      {:phoenix_live_dashboard, "~> 0.8", optional: true},
      {:jason, "~> 1.4", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:mox, "~> 1.1", only: :test},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
    ]
  end

  defp dialyzer do
    [
      plt_add_apps: [:mnesia, :mix],
      flags: [:unmatched_returns, :error_handling, :underspecs]
    ]
  end

  defp package do
    [
      name: "kathikon",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      licenses: ["MIT"],
      maintainers: ["Thanos Vassilakis"],
      links: %{"GitHub" => "https://github.com/thanos/kathikon"}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: "https://github.com/thanos/kathikon/blob/main",
      extras: extras(),
      assets: %{"docs/screenshots" => "screenshots"},
      groups_for_modules: [
        API: ~r/^Kathikon$/,
        Core: ~r/^Kathikon\.(Job|Worker|Config|Storage|Telemetry|Report|Batch|Dashboard)$/,
        Ops: [Kathikon.LiveDashboard.Page],
        Runtime: ~r/^Kathikon\.(Application|Queue|Dispatcher|Scheduler|Pruner)/,
        Storage: ~r/^Kathikon\.Storage/
      ],
      groups_for_extras: [
        Introduction: ~r/(^README|CHANGELOG|docs\/documentation)/i,
        Guides:
          ~r/docs\/(guides\/|storage\.md|job_lifecycle|scheduling\.md|quantum|batches|management|dashboard_spec|reporting|architecture\.md|articles)/,
        Reference: ~r/docs\/reference\//,
        Design: ~r/(docs\/phase-1-|docs\/v0_2_0|plans\/)/,
        Livebook: ~r/livebooks\//
      ]
    ]
  end

  defp extras do
    [
      "README.md",
      "CHANGELOG.md",
      {"LICENSE", [title: "License"]},
      {"docs/documentation.md", [title: "Documentation"]},
      "docs/guides/quick-start.md",
      "docs/guides/workers.md",
      "docs/guides/queues-and-concurrency.md",
      "docs/guides/scheduling.md",
      {"docs/scheduling.md", [title: "Scheduling reference"]},
      "docs/guides/retries-and-errors.md",
      "docs/guides/cancellation.md",
      "docs/storage.md",
      "docs/guides/storage-and-embedding.md",
      "docs/job_lifecycle.md",
      "docs/batches.md",
      "docs/management_api.md",
      "docs/guides/live-dashboard.md",
      "docs/dashboard_spec.md",
      "docs/reporting.md",
      "docs/guides/telemetry-and-observability.md",
      "docs/guides/configuration.md",
      "docs/quantum_adapter.md",
      "docs/architecture.md",
      # "docs/articles/kathikon_v0_2_0_control_scheduling_batches.md",
      # "docs/v0_2_0_architecture_review.md",
      "docs/reference/modules.md",
      {"livebooks/kathikon_demo.livemd", [title: "Interactive demo"]},
      {"livebooks/live_dashboard.livemd", [title: "LiveDashboard"]}
    ]
  end

  defp verify(_) do
    steps = [
      # ["precommit", :dev],
      {"compile --warnings-as-errors", :dev},
      {"format --check-formatted", :dev},
      {"credo --strict", :dev},
      # {"sobelow --exit Low", :dev},
      {"dialyzer", :dev},
      {"test --cover", :test},
      {"docs --warnings-as-errors", :dev}
    ]

    Enum.each(steps, fn {task, env} ->
      Mix.shell().info(IO.ANSI.format([:bright, "==> mix #{task}", :reset]))

      mix_executable =
        System.find_executable("mix") ||
          Mix.raise("Could not find `mix` executable on PATH")

      {_, exit_code} =
        System.cmd(mix_executable, String.split(task),
          env: [{"MIX_ENV", to_string(env)}],
          into: IO.stream(:stdio, :line),
          stderr_to_stdout: true
        )

      if exit_code != 0 do
        Mix.raise("mix #{task} failed (exit code #{exit_code})")
      end
    end)

    Mix.shell().info(
      IO.ANSI.format([:green, :bright, "\nAll verification checks passed!", :reset])
    )
  end
end

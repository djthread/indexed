defmodule Indexed.MixProject do
  use Mix.Project

  def project do
    [
      app: :indexed,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        ci: :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test,
        dialyzer: :test
      ],
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit],
        ignore: ".dialyzer_ignore.exs"
      ],

      # Docs
      name: "Indexed",
      source_url: "https://github.com/instinctscience/indexed",
      homepage_url: "https://www.instinctscripts.vet/",
      docs: [
        main: "Indexed",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :phoenix_pubsub]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.13.3", only: :test},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      # Will revert this when a new hex version is cut with my change
      # https://github.com/duffelhq/paginator/pull/96
      # {:paginator, "~> 1.0"},
      {:paginator, github: "djthread/paginator", branch: "dialyzer-fix"},
      {:phoenix_pubsub, "~> 2.0", optional: true}
    ]
  end

  defp aliases do
    [
      ci: ["lint", "coveralls", "dialyzer"],
      lint: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo"
      ]
    ]
  end
end

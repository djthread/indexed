defmodule Indexed.MixProject do
  use Mix.Project

  @source_url "https://github.com/instinctscience/indexed"
  @version "0.0.1"

  def project do
    [
      app: :indexed,
      version: @version,
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),
      docs: docs(),
      package: package(),
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
      homepage_url: "https://github.com/instinctscience/indexed",
      docs: [
        main: "Indexed",
        extras: ["README.md"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :phoenix_pubsub]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ecto_sql, "~> 3.3", optional: true},
      {:excoveralls, "~> 0.14", only: :test},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:paginator, "~> 1.0"},
      {:phoenix_pubsub, "~> 2.0", optional: true},
      {:postgrex, "~> 0.15", only: [:test]}
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

  defp package do
    [
      description: "Manage and Paginate records in ETS.",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      licenses: ["MIT"],
      links: %{
        "Changelog" => "https://hexdocs.pm/indexed/changelog.html",
        "GitHub" => @source_url
      }
    ]
  end

  def docs do
    [
      extras: [
        "CHANGELOG.md": [],
        LICENSE: [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      homepage_url: @source_url,
      source_url: @source_url,
      formatters: ["html"]
    ]
  end
end

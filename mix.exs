defmodule KafkaEx.Mixfile do
  @moduledoc false
  use Mix.Project

  @source_url "https://github.com/kafkaex/kafka_ex"
  @version "0.12.1"

  def project do
    [
      app: :kafka_ex,
      version: "0.12.1",
      elixir: "~> 1.10.4",
      dialyzer: [
        plt_add_deps: :transitive,
        flags: [
          :error_handling,
          :race_conditions
        ]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
      description: description(),
      package: package(),
      deps: deps(),
      docs: [
        main: "readme",
        extras: [
          "README.md",
          "kayrock.md",
          "new_api.md",
          "CONTRIBUTING.md"
        ],
        source_url: @source_url,
        source_ref: @version
      ]
    ]
  end

  def application do
    [
      mod: {KafkaEx, []},
      applications: [:lager]
    ]
  end

  defp deps do
    main_deps = [
      {:lager, "3.8.0"},
      {:kayrock, "~> 0.1.12"},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.3", only: :dev, runtime: false},
      {:excoveralls, "~> 0.7", only: :test, runtime: false},
      {:snappy,
       git: "https://github.com/fdmanana/snappy-erlang-nif", only: [:dev, :test]},
      {:snappyer, "~> 1.2", only: [:dev, :test]},
      {:git_hooks, "~> 0.6.3", only: [:dev], runtime: false}
    ]

    # we need a newer version of ex_doc, but it will cause problems on older
    # versions of elixir
    if Version.match?(System.version(), ">= 1.7.0") do
      main_deps ++ [{:ex_doc, "~> 0.23", only: :dev, runtime: false}]
    else
      main_deps
    end
  end

  defp description do
    "Kafka client for Elixir/Erlang."
  end

  defp package do
    [
      name: "kafka_ex_tc",
      maintainers: ["Abejide Ayodele", "Dan Swain", "Jack Lund", "Joshua Scott"],
      files: ["lib", "config/config.exs", "mix.exs", "README.md"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end

defmodule FixrPipeline.Mixfile do
    use Mix.Project

    def project do
        [app: :fixr_pipeline,
        version: "0.0.1",
        elixir: "~> 1.4.4",
        deps: [
            {:httpoison, "~> 0.11.2"},
            {:poison, "~> 3.1.0"}
        ]
        ]
    end

    def application do
      [applications: [:httpoison]]
    end
end
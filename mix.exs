defmodule Membrane.MoQ.Mixfile do
  use Mix.Project

  @version "0.1.0"
  @github_url "https://github.com/membraneframework/membrane_moq_plugin"

  def project do
    [
      app: :membrane_moq_plugin,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),

      # hex
      description: "Membrane Plugin for Media over QUIC (MoQ) streams",
      package: package(),

      # docs
      name: "Membrane MoQ plugin",
      source_url: @github_url,
      docs: docs(),
      homepage_url: "https://membrane.stream"
    ]
  end

  def application do
    [
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:membrane_core, "~> 1.0"},
      {:rustler, "~> 0.38"},
      {:bimap, "~> 1.3"},
      {:ratio, "~> 4.0.1"},
      {:membrane_h26x_plugin, "~> 0.10.7"},
      {:membrane_h264_format, "~> 0.6.0"},
      {:membrane_h265_format, "~> 0.2.0"},
      {:membrane_aac_format, "~> 0.8.0"},
      {:membrane_opus_format, "0.3.0"},
      {:membrane_aac_plugin, "~> 0.19", only: :test},
      {:muontrap, "~> 1.8", only: :test},
      {:membrane_file_plugin, "~> 0.17", only: :test},
      {:membrane_realtimer_plugin, "~> 0.9", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:dialyxir, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp dialyzer() do
    opts = [
      flags: [:error_handling]
    ]

    if System.get_env("CI") == "true" do
      # Store PLTs in cacheable directory for CI
      [plt_local_path: "priv/plts", plt_core_path: "priv/plts"] ++ opts
    else
      opts
    end
  end

  defp package do
    [
      maintainers: ["Membrane Team"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @github_url,
        "Membrane Framework Homepage" => "https://membrane.stream"
      },
      files: ["lib", "native", "mix.exs", "README*", "LICENSE*", ".formatter.exs"]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      formatters: ["html"],
      source_ref: "v#{@version}",
      nest_modules_by_prefix: [Membrane.MoQ]
    ]
  end
end

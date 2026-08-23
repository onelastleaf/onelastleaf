defmodule ExamplePlugin.MixProject do
  use Mix.Project

  def project do
    [
      app: :__OLL_PLUGIN_MODULE__,
      version: "0.1.0",
      elixir: "~> 1.17",
      escript: [main_module: ExamplePlugin],
      deps: [{:onelastleaf_plugin_sdk, "== 0.1.0"}]
    ]
  end

  def application, do: [extra_applications: [:logger]]
end

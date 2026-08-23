defmodule ExamplePlugin do
  alias Onelastleaf.PluginSDK.{ActionResult, Plugin}

  def main(_arguments) do
    Plugin.new!("__OLL_PLUGIN_ID__", "0.1.0")
    |> Plugin.action("echo", "Return the supplied arguments", fn _context, arguments ->
      ActionResult.string(Enum.join(arguments, " "))
    end)
    |> Plugin.run!()
  end

  def echo(arguments), do: Enum.join(arguments, " ")
end

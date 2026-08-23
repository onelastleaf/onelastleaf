defmodule ExamplePluginTest do
  use ExUnit.Case
  test "echo preserves argument order" do
    assert ExamplePlugin.echo(["one", "two"]) == "one two"
  end
end

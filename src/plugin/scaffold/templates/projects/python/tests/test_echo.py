from __OLL_PLUGIN_MODULE__.echo import echo

def test_echo_preserves_order() -> None:
    assert echo(["one", "two"]) == "one two"

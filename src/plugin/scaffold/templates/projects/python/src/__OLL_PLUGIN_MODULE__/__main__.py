import asyncio

from onelastleaf_plugin_sdk import ActionResult, Plugin
from .echo import echo

plugin = Plugin("__OLL_PLUGIN_ID__", "0.1.0")

@plugin.action("echo", "Return the supplied arguments")
async def echo_action(_context, arguments: list[str]) -> ActionResult:
    return ActionResult.string(echo(arguments))

def main() -> None:
    asyncio.run(plugin.run())

if __name__ == "__main__":
    main()

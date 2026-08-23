using ExamplePlugin;
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("__OLL_PLUGIN_ID__", "0.1.0")
    .Action("echo", "Return the supplied arguments",
        (_, arguments) => Task.FromResult(ActionResult.String(Echo.Run(arguments))));
await plugin.RunAsync();

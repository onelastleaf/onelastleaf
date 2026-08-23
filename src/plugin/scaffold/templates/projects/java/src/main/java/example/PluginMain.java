package example;

import java.util.concurrent.CompletableFuture;
import org.onelastleaf.plugin.sdk.ActionResult;
import org.onelastleaf.plugin.sdk.Plugin;

public final class PluginMain {
    public static void main(String[] args) {
        Plugin.builder("__OLL_PLUGIN_ID__", "0.1.0")
            .action("echo", "Return the supplied arguments", (context, arguments) ->
                CompletableFuture.completedFuture(ActionResult.string(Echo.run(arguments))))
            .build()
            .run()
            .toCompletableFuture()
            .join();
    }
}

package example

import org.onelastleaf.plugin.sdk.kotlin.action
import org.onelastleaf.plugin.sdk.kotlin.plugin
import org.onelastleaf.plugin.sdk.kotlin.stringResult

fun main() = plugin("__OLL_PLUGIN_ID__", "0.1.0") {
    action("echo", "Return the supplied arguments") { _, arguments ->
        stringResult(echo(arguments))
    }
}.run().toCompletableFuture().join()

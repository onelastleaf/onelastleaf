package example

import org.onelastleaf.plugin.sdk.scala.Plugin

object PluginMain:
  def main(arguments: Array[String]): Unit =
    Plugin("__OLL_PLUGIN_ID__", "0.1.0")
      .action("echo", "Return the supplied arguments")((_, values) => Echo.run(values))
      .run()
      .toCompletableFuture
      .join()

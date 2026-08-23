import Foundation
import OnelastleafPluginSDK

do {
  let plugin = try Plugin(id: "__OLL_PLUGIN_ID__", version: "0.1.0")
    .action(name: "echo", description: "Return the supplied arguments") { _, arguments in
      .string(echo(arguments))
    }
  try await plugin.run()
} catch {
  FileHandle.standardError.write(Data("plugin failed: \(error)\n".utf8))
  exit(EXIT_FAILURE)
}

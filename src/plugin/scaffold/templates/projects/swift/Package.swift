// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "__OLL_PLUGIN_NAME__",
    platforms: [.macOS(.v15)],
    products: [
        .executable(name: "__OLL_PLUGIN_NAME__", targets: ["PluginMain"])
    ],
    dependencies: [
        .package(url: "https://github.com/onelastleaf/swift-plugin-sdk.git", exact: "0.1.0")
    ],
    targets: [
        .executableTarget(name: "PluginMain", dependencies: [
            .product(name: "OnelastleafPluginSDK", package: "swift-plugin-sdk")
        ]),
        .testTarget(name: "PluginTests", dependencies: ["PluginMain"])
    ]
)

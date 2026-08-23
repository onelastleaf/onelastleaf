import Testing
@testable import PluginMain

@Test func preservesOrder() { #expect(echo(["one", "two"]) == "one two") }

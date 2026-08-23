using ExamplePlugin;
using Xunit;

public sealed class EchoTests
{
    [Fact]
    public void PreservesOrder() => Assert.Equal("one two", Echo.Run(["one", "two"]));
}

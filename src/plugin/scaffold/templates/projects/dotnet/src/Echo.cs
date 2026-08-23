namespace ExamplePlugin;

public static class Echo
{
    public static string Run(IEnumerable<string> arguments) => string.Join(" ", arguments);
}

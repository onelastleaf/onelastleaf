package example;

import java.util.List;

public final class Echo {
    private Echo() {}
    public static String run(List<String> arguments) { return String.join(" ", arguments); }
}

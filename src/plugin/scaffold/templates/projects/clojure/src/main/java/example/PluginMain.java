package example;

public final class PluginMain {
    private PluginMain() {}
    public static void main(String[] args) {
        clojure.main.main(new String[] {"-m", "example.plugin"});
    }
}

package example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentVector;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class EchoTest {
    private static IFn echo;

    @BeforeAll
    static void loadPlugin() {
        Clojure.var("clojure.core", "require").invoke(Clojure.read("example.plugin"));
        echo = Clojure.var("example.plugin", "echo");
    }

    @Test
    void preservesOrder() {
        assertEquals("one two", echo.invoke(PersistentVector.create(List.of("one", "two"))));
    }
}

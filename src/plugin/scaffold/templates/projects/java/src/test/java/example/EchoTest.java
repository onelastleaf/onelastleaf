package example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import org.junit.jupiter.api.Test;

final class EchoTest {
    @Test void preservesOrder() { assertEquals("one two", Echo.run(List.of("one", "two"))); }
}

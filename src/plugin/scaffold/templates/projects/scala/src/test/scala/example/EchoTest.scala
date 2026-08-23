package example

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class EchoTest:
  @Test def preservesOrder(): Unit = assertEquals("one two", Echo.run(Seq("one", "two")))

package example

import kotlin.test.Test
import kotlin.test.assertEquals

class EchoTest {
    @Test fun preservesOrder() = assertEquals("one two", echo(listOf("one", "two")))
}

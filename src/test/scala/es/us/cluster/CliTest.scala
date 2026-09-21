package es.us.cluster

import org.junit.Test
import org.junit.Assert._

class CliTest {
  @Test def parseNamedOptions(): Unit = {
    val c = Main.parse(Array("--input", "a.csv", "--output", "results", "--header", "true",
      "--drop-columns", "0,2", "--seed", "7"))
    assertEquals(Set(0, 2), c.dropColumns); assertEquals(7L, c.seed); assertTrue(c.header)
    assertEquals("paper", c.definition)
    assertArrayEquals(Array(1.0, 3.0), Main.parseRow("id,1,label,3", 2, Set(0, 2)).toArray, 0.0)
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectMissingValue(): Unit = {
    Main.parse(Array("--input"))
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectUnknownOption(): Unit = {
    Main.parse(Array("--input", "a", "--output", "b", "--sead", "42"))
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectInvalidRange(): Unit = {
    Main.parse(Array("--input", "a", "--output", "b", "--min-k", "4", "--max-k", "2"))
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectNonFiniteValue(): Unit = {
    Main.parseRow("1,NaN", 1, Set.empty)
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectEmptyCell(): Unit = {
    Main.parseRow("1,", 1, Set.empty)
  }
  @Test def medianSortsItsInput(): Unit = {
    assertEquals(2.5, Utils.calculateMedian(List(4.0, 1.0, 3.0, 2.0)), 0.0)
  }
}

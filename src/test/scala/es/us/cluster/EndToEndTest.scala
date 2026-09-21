package es.us.cluster

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConverters._

/** Includes Hadoop-backed CSV input, checkpoints and result directory output. */
class EndToEndTest {
  @Test def allAlgorithmsWriteLabeledResults(): Unit = {
    val root = Files.createTempDirectory("clusterindices-e2e-")
    val input = root.resolve("input.csv")
    Files.write(input, "id,x,label\na,0,A\nb,0.1,A\nc,0.2,A\nd,10,B\ne,10.1,B\nf,10.2,B\n".getBytes(StandardCharsets.UTF_8))
    for (algorithm <- Seq("kmeans", "bkm", "linkage")) {
      val output = root.resolve(algorithm)
      Main.run(RunConfig(input.toString, output.toString, algorithm = algorithm,
        minK = 2, maxK = 2, partitions = 3, iterations = 20, header = true,
        dropColumns = Set(0, 2), checkpoint = root.resolve("checkpoints").toString,
        master = Some("local[2]")))
      val lines = Files.readAllLines(output.resolve("part-00000")).asScala
      assertEquals(2, lines.size)
      assertTrue(lines.head.startsWith("algorithm\tdefinition\tseed\tk\t"))
      val fields = lines(1).split("\t")
      assertEquals(13, fields.length)
      assertEquals(algorithm, fields(0)); assertEquals("paper", fields(1))
      assertEquals(0.04, fields(7).toDouble, 1e-8)
      assertTrue(Files.exists(output.resolve("_SUCCESS")))
      val metadata = new java.util.Properties()
      val stream = Files.newInputStream(output.resolve("_metadata.properties"))
      try metadata.load(stream) finally stream.close()
      assertEquals("6", metadata.getProperty("rows"))
      assertEquals("0,2", metadata.getProperty("drop.columns"))
    }
  }
  @Test def anExistingOutputIsNeverOverwritten(): Unit = {
    val root = Files.createTempDirectory("clusterindices-existing-")
    val marker = root.resolve("keep.txt")
    Files.write(marker, "keep".getBytes(StandardCharsets.UTF_8))
    try {
      Main.run(RunConfig("missing.csv", root.toString, master = Some("local[2]")))
      fail("Expected existing output rejection")
    } catch { case e: IllegalArgumentException => assertTrue(e.getMessage.contains("already exists")) }
    assertEquals("keep", new String(Files.readAllBytes(marker), StandardCharsets.UTF_8))
  }
}

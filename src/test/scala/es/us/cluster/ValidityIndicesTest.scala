package es.us.cluster

import java.nio.file.Files
import es.us.linkage.{Distance, Linkage}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.junit.{After, Before, Test}
import org.junit.Assert._

class ValidityIndicesTest {
  private var sc: SparkContext = _
  @Before def start(): Unit = {
    sc = new SparkContext(new SparkConf().setAppName("indices-test").setMaster("local[2]")
      .set("spark.ui.enabled", "false").set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1"))
    sc.setLogLevel("ERROR")
  }
  @After def stop(): Unit = if (sc != null) sc.stop()
  private def v(x: Double): Vector = Vectors.dense(x)
  private def close(expected: Double, actual: Double): Unit = assertEquals(expected, actual, 1e-9)

  @Test def publishedEquationsUseGlobalCenterAndEqualClusterWeights(): Unit = {
    val data = sc.parallelize(Seq(0 -> v(0), 0 -> v(2), 1 -> v(8), 1 -> v(10), 1 -> v(12), 1 -> v(14)), 3)
    val result = ValidityIndices.evaluate(data, Array(v(1), v(11)))
    close(0.7, result.bdSilhouette)
    close(5.0 / 3, result.bdDunn)
    close(0.3, result.daviesBouldin)
    close(22.0, result.wssse)
  }
  @Test def historicalEquationsUsePairsAndSquaredDistances(): Unit = {
    val data = sc.parallelize(Seq(0 -> v(0), 0 -> v(2), 1 -> v(8), 1 -> v(10), 1 -> v(12), 1 -> v(14)), 2)
    val result = ValidityIndices.evaluate(data, Array(v(1), v(11)), IndexDefinition.Legacy)
    close(1.0 - 22.0 / 600, result.bdSilhouette)
    close(100.0 / 9, result.bdDunn)
    close(0.06, result.daviesBouldin)
    close(22.0, result.wssse)
  }
  @Test def singleClusterAndZeroRadiusHaveDocumentedSemantics(): Unit = {
    val single = ValidityIndices.evaluate(sc.parallelize(Seq(0 -> v(0), 0 -> v(2))), Array(v(1)))
    close(-1, single.bdSilhouette); close(0, single.bdDunn)
    assertTrue(single.daviesBouldin.isNaN)
    val perfect = ValidityIndices.evaluate(sc.parallelize(Seq(0 -> v(0), 1 -> v(2))), Array(v(0), v(2)))
    close(1, perfect.bdSilhouette); assertTrue(perfect.bdDunn.isNaN)
    close(0, perfect.wssse)
  }
  @Test(expected = classOf[IllegalArgumentException]) def rejectUnusedCenters(): Unit = {
    ValidityIndices.evaluate(sc.parallelize(Seq(0 -> v(0))), Array(v(0), v(2)))
  }
  @Test def clusteringIsRepeatableAndBkmRuns(): Unit = {
    val data = sc.parallelize(Seq(0.0, 0.1, 0.2, 10.0, 10.1, 10.2).map(v), 2).cache()
    try {
      val first = ClusterIndex.evaluateKMeans(data, 2, 20)
      val second = ClusterIndex.evaluateKMeans(data, 2, 20)
      close(first.wssse, second.wssse)
      close(0.04, first.wssse)
      close(0.04, ClusterIndex.evaluateBKM(data, 2, 20).wssse)
    } finally data.unpersist()
  }
  @Test def linkageUsesStableIdsAndPreservesMemberships(): Unit = {
    sc.setCheckpointDir(Files.createTempDirectory("clusterindices-test-").toString)
    val coords = sc.parallelize(Seq(1 -> v(0), 2 -> v(2), 3 -> v(9), 4 -> v(10)), 3)
    val distances = coords.cartesian(coords).filter { case (a, b) => a._1 < b._1 }
      .map { case (a, b) => new Distance(a._1, b._1, math.abs(a._2(0) - b._2(0)).toFloat) }.repartition(1)
    for (strategy <- Seq("min", "max", "avg")) {
      val model = new Linkage(2, strategy).runAlgorithm(distances, 4)
      val membership = model.createClusters(4, 2, sc.parallelize(1 to 4))
      val labels = membership.collect().toMap
      assertEquals(labels(1), labels(2)); assertEquals(labels(3), labels(4))
      assertNotEquals(labels(1), labels(3))
      val centers = model.inicializeCenters(coords, 2, 4, 1, membership)
      assertEquals(Set(1.0, 9.5), centers.map(_(0)).toSet)
      assertEquals(2, model.giveMePoints(labels(1), 4).length)
    }
  }
  @Test def coincidentCentersGiveUndefinedDaviesBouldin(): Unit = {
    val result = ValidityIndices.evaluate(sc.parallelize(Seq(0 -> v(-1), 1 -> v(1))), Array(v(0), v(0)))
    assertTrue(result.daviesBouldin.isNaN)
  }
  @Test def suppliedMembershipsAreNotReplacedByNearestCenters(): Unit = {
    val data = sc.parallelize(Seq(0 -> v(0), 0 -> v(10), 1 -> v(4), 1 -> v(8)))
    val result = ValidityIndices.evaluate(data, Array(v(5), v(6)))
    close(58, result.wssse)
  }
  @Test def linkageStrategiesProduceExpectedMergeHeights(): Unit = {
    sc.setCheckpointDir(Files.createTempDirectory("clusterindices-heights-").toString)
    val points = Seq(0.0, 2.0, 9.0, 10.0)
    val pairs = for (i <- points.indices; j <- i + 1 until points.size)
      yield new Distance(i + 1, j + 1, math.abs(points(i) - points(j)).toFloat)
    for ((strategy, expected) <- Seq("min" -> 7.0, "max" -> 10.0, "avg" -> 8.5)) {
      val model = new Linkage(1, strategy).runAlgorithm(sc.parallelize(pairs, 2), 4)
      close(expected, model.mergeDistances(7L))
      assertEquals(List(1, 2, 3, 4), model.giveMePoints(7).sorted)
    }
  }

}

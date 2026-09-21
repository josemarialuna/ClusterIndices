package es.us.cluster

import es.us.linkage.{Distance, Linkage}
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/** Training facade. Historical tuple APIs retain the legacy metric convention. */
object ClusterIndex {
  val DefaultSeed: Long = 42L
  type LegacyResult = (Double, Double, Double, Double, Long, Long, Long, Long)

  def dataToDouble(s: String): Double = if (s.isEmpty) 0.0 else s.toDouble

  def evaluateKMeans(data: RDD[Vector], k: Int, iterations: Int,
                     seed: Long = DefaultSeed,
                     definition: IndexDefinition = IndexDefinition.Paper): IndexResult = {
    validate(data, k, iterations)
    val model = new KMeans().setK(k).setMaxIterations(iterations).setSeed(seed).run(data)
    require(model.clusterCenters.length == k, "Training produced fewer clusters than requested; inspect duplicate points")
    ValidityIndices.evaluate(data.map(p => (model.predict(p), p)), model.clusterCenters, definition)
  }

  def evaluateBKM(data: RDD[Vector], k: Int, iterations: Int,
                  seed: Long = DefaultSeed,
                  definition: IndexDefinition = IndexDefinition.Paper): IndexResult = {
    validate(data, k, iterations)
    val model = new BisectingKMeans().setK(k).setMaxIterations(iterations).setSeed(seed).run(data)
    require(model.clusterCenters.length == k,
      "Bisecting K-means produced fewer clusters than requested; reduce k or inspect duplicate points")
    ValidityIndices.evaluate(data.map(p => (model.predict(p), p)), model.clusterCenters, definition)
  }

  private def validate(data: RDD[Vector], k: Int, iterations: Int): Unit = {
    require(k >= 2, "k must be at least 2")
    require(iterations > 0, "iterations must be positive")
    require(data.take(k).length >= k, "Input must contain at least k points")
  }

  def getIndicesKMeans(data: RDD[Vector], k: Int, iterations: Int): LegacyResult =
    evaluateKMeans(data, k, iterations, DefaultSeed, IndexDefinition.Legacy).legacyTuple

  def getIndicesBKM(data: RDD[Vector], k: Int, iterations: Int): LegacyResult =
    evaluateBKM(data, k, iterations, DefaultSeed, IndexDefinition.Legacy).legacyTuple

  def getIndicesLinkage(data: RDD[Vector], coordinates: RDD[(Int, Vector)], distances: RDD[Distance],
                        numPoints: Int, clusterFilterNumber: Int, strategyDistance: String,
                        minClusters: Int, maxClusters: Int): RDD[(Int, LegacyResult)] = {
    require(clusterFilterNumber == 1, "Filtering small clusters is unsupported: it changes the evaluated population")
    require(minClusters >= 2 && maxClusters >= minClusters && maxClusters <= numPoints,
      "Expected 2 <= minClusters <= maxClusters <= numPoints")
    require(coordinates.count() == numPoints, "numPoints must match the coordinates")
    val sc = data.sparkContext
    require(sc.getCheckpointDir.nonEmpty, "Set a checkpoint directory before running Linkage")
    val model = new Linkage(minClusters, strategyDistance).runAlgorithm(distances, numPoints)
    val total = sc.parallelize(1 to numPoints)
    val results = (minClusters to maxClusters).map { k =>
      val membership = model.createClusters(numPoints, k, total).cache()
      try {
        val centers = model.inicializeCenters(coordinates, k, numPoints, 1, membership)
        val labels = membership.values.distinct().collect().sorted.zipWithIndex.toMap
        val assigned = coordinates.join(membership).map { case (_, (point, label)) => (labels(label), point) }
        (k, ValidityIndices.evaluate(assigned, centers, IndexDefinition.Legacy).legacyTuple)
      } finally membership.unpersist()
    }
    sc.parallelize(results)
  }
}

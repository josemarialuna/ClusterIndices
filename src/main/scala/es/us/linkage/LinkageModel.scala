package es.us.linkage

import es.us.cluster.Utils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/** Merge history uses original IDs 1..n and merge IDs n+1..2n-k. */
class LinkageModel(val clusters: RDD[(Long, (Int, Int))],
                   private var centers: Array[Vector],
                   val mergeDistances: Map[Long, Double] = Map.empty) extends Serializable {
  def clusterCenters: Array[Vector] = centers
  def setClusterCenters(value: Array[Vector]): Unit = { centers = value }
  def isCluster(point: Int): Boolean = clusters.lookup(point.toLong).nonEmpty
  def isCluster(point: Int, totalPoints: Int): Boolean = point > totalPoints

  def giveMePoints(point: Int): List[Int] = {
    val merges = clusters.collect().toMap
    var pending = List(point)
    val result = List.newBuilder[Int]
    while (pending.nonEmpty) {
      val head = pending.head
      pending = pending.tail
      merges.get(head.toLong) match {
        case Some((a, b)) => pending = a :: b :: pending
        case None => result += head
      }
    }
    result.result()
  }
  def giveMePoints(point: Int, numberPoints: Int): Array[(Int, Int)] =
    giveMePoints(point).map(_ -> point).toArray
  def giveMePointsRDD(cluster: Int, numberPoints: Int): RDD[(Int, Int)] =
    clusters.sparkContext.parallelize(giveMePoints(cluster).sorted.map(_ -> cluster))
  def giveMeCluster(point: Int, numberPoints: Int, clusterBase: RDD[(Int, Int)]): Int = {
    val parents = clusterBase.collect().toMap
    var current = point
    val visited = scala.collection.mutable.Set.empty[Int]
    while (parents.contains(current) && parents(current) != current) {
      require(visited.add(current), "Cycle in cluster membership")
      current = parents(current)
    }
    current
  }
  def printSchema(separator: String): Unit =
    println(clusters.sortByKey().map { case (id, (a, b)) => s"$id,$a,$b" }.collect().mkString(separator))
  def saveSchema(destination: String): Unit =
    clusters.sortByKey().map { case (id, (a, b)) => s"$id,$a,$b" }
      .coalesce(1).saveAsTextFile(destination + "Linkage-" + Utils.whatTimeIsIt())
  def saveResult(destination: String, resultPoints: RDD[(Int, Int)], numPoints: Int, numCluster: Int): Unit =
    resultPoints.sortByKey().map { case (point, cluster) => s"$point,$cluster" }.coalesce(1)
      .saveAsTextFile(destination + s"Points-$numPoints-Clusters-$numCluster")

  def createClusters(numPoints: Int, numCluster: Int, totalPoints: RDD[Int]): RDD[(Int, Int)] = {
    require(numCluster >= 1 && numCluster <= numPoints, "Invalid cluster count")
    val history = clusters.filter(_._1 <= 2L * numPoints - numCluster).collect().sortBy(_._1)
    require(history.length == numPoints - numCluster, "Merge history cannot produce the requested cut")
    val parent = Array.tabulate(2 * numPoints + 1)(identity)
    history.foreach { case (id, (a, b)) => parent(a) = id.toInt; parent(b) = id.toInt }
    def root(start: Int): Int = {
      var current = start
      while (parent(current) != current) current = parent(current)
      var node = start
      while (parent(node) != node) { val next = parent(node); parent(node) = current; node = next }
      current
    }
    val labels = Array.tabulate(numPoints + 1)(root)
    totalPoints.map { point =>
      require(point >= 1 && point <= numPoints, "Point IDs must be consecutive from 1 to numPoints")
      (point, labels(point))
    }
  }

  /** Centers are ordered by ascending cluster ID, matching createClusters memberships. */
  def inicializeCenters(coordinates: RDD[(Int, Vector)], numClusters: Int, numPoints: Int,
                        kMin: Int, resultPoints: RDD[(Int, Int)]): Array[Vector] = {
    require(kMin == 1, "Small-cluster filtering is unsupported")
    val summaries = coordinates.join(resultPoints).map { case (_, (point, label)) =>
      (label, (point.toArray, 1L))
    }.reduceByKey { (a, b) =>
      require(a._1.length == b._1.length, "Inconsistent vector dimensions")
      (a._1.indices.map(i => a._1(i) + b._1(i)).toArray, a._2 + b._2)
    }.collect().sortBy(_._1)
    require(summaries.length == numClusters && summaries.map(_._2._2).sum == numPoints,
      "Memberships must cover every input point exactly once")
    summaries.map { case (_, (sum, count)) => Vectors.dense(sum.map(_ / count)) }
  }
  def predict(point: Vector): Int = {
    require(centers.nonEmpty, "Initialize centers before predicting")
    centers.indices.minBy(i => Vectors.sqdist(point, centers(i)))
  }
  def pointCost(point: Vector): Double = Vectors.sqdist(point, centers(predict(point)))
  def computeCost(points: RDD[Vector]): Double = points.map(pointCost).sum()
}

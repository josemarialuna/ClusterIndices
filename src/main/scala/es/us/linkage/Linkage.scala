package es.us.linkage

import es.us.cluster.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * @author José María Luna and José David Martín
  * @version 1.0
  * @since v1.0 Dev
  */

class Linkage(
               private var numClusters: Int,
               private var distanceStrategy: String) extends Serializable with Logging {

  def getNumClusters: Int = numClusters

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getDistanceStrategy: String = distanceStrategy

  def setDistanceStrategy(distanceStrategy: String): this.type = {
    this.distanceStrategy = distanceStrategy
    this
  }

  //Sort by dist
  object DistOrdering extends Ordering[Distance] {
    def compare(a: Distance, b: Distance) = a.getDist compare b.getDist
  }

  /**
    * Return the Linkage model to given distance data
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @return A Linkage model to given data
    * @example runAlgorithm(distanceMatrix, 150)
    */
  def runAlgorithm(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

    require(numPoints >= 2 && numClusters >= 1 && numClusters <= numPoints, "Invalid cluster count")
    require(Set("min", "max", "avg").contains(distanceStrategy), "Linkage strategy must be min, max or avg")
    val sc = distanceMatrix.sparkContext
    require(sc.getCheckpointDir.nonEmpty, "Linkage requires a checkpoint directory")
    val partitions = math.max(1, distanceMatrix.getNumPartitions)
    var matrix = distanceMatrix.map { d =>
      require(d.getIdW1 >= 1 && d.getIdW2 <= numPoints && d.getIdW1 < d.getIdW2,
        "Distances must use unique pairs 1 <= idW1 < idW2 <= numPoints")
      require(!d.getDist.isNaN && !d.getDist.isInfinity && d.getDist >= 0, "Invalid distance")
      d
    }.persist(StorageLevel.MEMORY_AND_DISK)
    val history = scala.collection.mutable.ArrayBuffer.empty[(Long, (Int, Int))]
    val heights = scala.collection.mutable.Map.empty[Long, Double]
    try {
      val expected = numPoints.toLong * (numPoints - 1) / 2
      require(matrix.count() == expected &&
        matrix.map(d => (d.getIdW1, d.getIdW2)).distinct().count() == expected,
        "Provide exactly one distance for every unordered pair")
      for (step <- 0 until (numPoints - numClusters)) {
        val closest = matrix.takeOrdered(1)(Ordering.by[Distance, (Float, Int, Int)](
          d => (d.getDist, d.getIdW1, d.getIdW2))).head
        val a = closest.getIdW1
        val b = closest.getIdW2
        val id = numPoints + step + 1
        history += ((id.toLong, (a, b)))
        heights(id.toLong) = closest.getDist.toDouble
        if (step < numPoints - numClusters - 1) {
          def neighbors(point: Int, other: Int): RDD[(Int, Float)] =
            matrix.filter(d => (d.getIdW1 == point || d.getIdW2 == point) &&
              d.getIdW1 != other && d.getIdW2 != other).map { d =>
              (if (d.getIdW1 == point) d.getIdW2 else d.getIdW1, d.getDist)
            }
          val strategy = distanceStrategy
          val added = neighbors(a, b).join(neighbors(b, a)).map { case (neighbor, (x, y)) =>
            val distance = strategy match {
              case "min" => math.min(x, y)
              case "max" => math.max(x, y)
              // Historical avg is WPGMA: equal weight per merged cluster, not per point.
              case "avg" => x / 2 + y / 2
            }
            new Distance(neighbor, id, distance)
          }
          val retained = matrix.filter(d => d.getIdW1 != a && d.getIdW2 != a &&
            d.getIdW1 != b && d.getIdW2 != b)
          val previous = matrix
          val next = retained.union(added).coalesce(partitions).persist(StorageLevel.MEMORY_AND_DISK)
          matrix = next
          try {
            if (step % 5 == 0) next.checkpoint()
            next.count()
          } finally previous.unpersist()
        }
      }
      new LinkageModel(sc.parallelize(history.toSeq), Array.empty[Vector], heights.toMap)
    } finally matrix.unpersist()
  }

  /**
    * Return the Linkage model to given distance data and all points with its cluster number
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @return A Linkage model to given data and RDD with (Int, Int) [point and cluster number]
    * @example runAlgorithmWithResult(distanceMatrix, 150)
    */
  def runAlgorithmWithResult(distanceMatrix: RDD[Distance], numPoints: Int): (LinkageModel, RDD[(Int,Int)]) = {

    val model = runAlgorithm(distanceMatrix, numPoints)
    (model, model.createClusters(numPoints, numClusters, distanceMatrix.sparkContext.parallelize(1 to numPoints)))
  }

  /**
    * Save the result of Linkage algorithm into a external file with the dendogram format to representation with Python library
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @param numClusters The number of clusters
    * @return Nothing
    * @example runAlgorithmDendrogram(distanceMatrix, 150, 5)
    */
  def runAlgorithmDendrogram(distanceMatrix: RDD[Distance], numPoints: Int, numClusters: Int) = {

    val model = new Linkage(numClusters, distanceStrategy).runAlgorithm(distanceMatrix, numPoints)
    val sizes = scala.collection.mutable.Map.empty[Int, Long]
    val rows = model.clusters.collect().sortBy(_._1).map { case (id, (a, b)) =>
      val count = sizes.getOrElse(a, 1L) + sizes.getOrElse(b, 1L)
      sizes(id.toInt) = count
      s"${a - 1},${b - 1},${model.mergeDistances(id)},$count"
    }
    distanceMatrix.sparkContext.parallelize(rows.toSeq, 1)
      .saveAsTextFile(s"Points-$numPoints-Clusters-$numClusters-${Utils.whatTimeIsIt()}")
  }

  /**
    * Return the Linkage model to given distance data but using DataFrames
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @param numPartitions The number of partitions
    * @return A Linkage model to given data
    * @example runAlgorithmDF(distanceMatrix, 150, 16)
    */
  def runAlgorithmDF(distanceMatrix: DataFrame, numPoints: Int, numPartitions: Int): LinkageModel ={

    val rdd = distanceMatrix.select("idW1", "idW2", "dist").rdd.map { row =>
      new Distance(row.get(0).asInstanceOf[Number].intValue(),
        row.get(1).asInstanceOf[Number].intValue(), row.get(2).asInstanceOf[Number].floatValue())
    }.repartition(numPartitions)
    runAlgorithm(rdd, numPoints)
  }

  /**
    * Return a Int that represent a point of the model
    * @param oldDistance    Original Distance
    * @param clusterReference A Distance with the cluster
    * @return A Int that represent the next value for the Linkage model. Return the first o the second point that constitution the cluster
    * @example filterMatrix(oldDistnace, clusterReference)
    */
  def filterMatrix(oldDistance: Distance, clusterReference: Distance): Int = {
    var result = 0

    if (oldDistance.getIdW1 == clusterReference.getIdW1 || oldDistance.getIdW1 == clusterReference.getIdW2) {
      result = oldDistance.getIdW2
    } else if (oldDistance.getIdW2 == clusterReference.getIdW1 || oldDistance.getIdW2 == clusterReference.getIdW2) {
      result = oldDistance.getIdW1
    }

    result
  }

  /**
    * Return a Int that represent a point of the model
    * @param idW1    First coordinate
    * @param idW2 Second coordinate
    * @param pointReference1 First coordinate of the cluster
    * @param pointReference2 Second coordinate of the cluster
    * @return A Int that represent the next value for the Linkage model. Return the first o the second point that constitution the cluster
    * @example filterDF(1, 2, 10, 11)
    */
  def filterDF(idW1: Int, idW2: Int, pointReference1: Int, pointReference2: Int): Int = {
    var result = idW1

    if (idW1 == pointReference1 || idW1 == pointReference2) {
      result = idW2
    }

    result
  }
}

object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[(Int, Int), Float],
                       strategy: String): Double = {
    var res = 0.0
    var aux = res

    strategy match {
      case "min" =>
        res = Double.PositiveInfinity

        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            }
            else {
              aux = distanceMatrix(y, x)
            }
            if (aux < res)
              res = aux

          }
        }


      case "max" =>
        res = 0.0
        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            } else {
              aux = distanceMatrix(y, x)
            }
            if (aux > res)
              res = aux
          }
        }

      case "avg" =>
        val values = for (x <- c1.getCoordinates; y <- c2.getCoordinates)
          yield distanceMatrix((math.min(x, y), math.max(x, y))).toDouble
        require(values.nonEmpty, "Cannot compare empty clusters")
        res = values.sum / values.size
      case _ => throw new IllegalArgumentException("Unknown linkage strategy")
    }

    res

  }


}

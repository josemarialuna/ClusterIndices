package es.us.cluster

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/** Explicit scientific conventions. Paper uses Euclidean d in equations 6-10. */
sealed trait IndexDefinition extends Serializable
object IndexDefinition {
  case object Paper extends IndexDefinition
  case object Legacy extends IndexDefinition
  def parse(value: String): IndexDefinition = value match {
    case "paper" => Paper
    case "legacy" => Legacy
    case _ => throw new IllegalArgumentException("definition must be paper or legacy")
  }
}

case class IndexResult(bdSilhouette: Double, bdDunn: Double, daviesBouldin: Double,
                       wssse: Double, statisticsMs: Long, silhouetteMs: Long,
                       dunnMs: Long, daviesBouldinMs: Long, wssseMs: Long) {
  // Shared statistics are charged once to the first index in the historical tuple.
  def legacyTuple: (Double, Double, Double, Double, Long, Long, Long, Long) =
    (bdSilhouette, bdDunn, daviesBouldin, wssse,
      statisticsMs + silhouetteMs, dunnMs, daviesBouldinMs, wssseMs)
}

/** Evaluate supplied memberships without retraining or reassigning hierarchical clusters.
  * Only O(k) summaries and O(k*d) centers are retained on the driver.
  */
object ValidityIndices {
  private case class Summary(count: Long, distance: Double, squared: Double, maximum: Double) {
    def +(other: Summary): Summary = Summary(count + other.count,
      distance + other.distance, squared + other.squared, math.max(maximum, other.maximum))
  }
  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val result = f
    (result, (System.nanoTime() - start) / 1000000L)
  }
  private def finite(x: Double): Boolean = !x.isNaN && !x.isInfinity

  def evaluate(assignments: RDD[(Int, Vector)], centers: Array[Vector],
               definition: IndexDefinition = IndexDefinition.Paper): IndexResult = {
    require(centers.nonEmpty, "At least one center is required")
    val dimensions = centers.head.size
    require(dimensions > 0 && centers.forall(c => c.size == dimensions && c.toArray.forall(finite)),
      "Centers must have equal positive dimensions and finite coordinates")
    require(definition != IndexDefinition.Legacy || centers.length >= 2,
      "Legacy indices require at least two clusters")
    val bc = assignments.sparkContext.broadcast(centers.map(_.copy))
    val (stats, statisticsMs) = try {
      timed {
        assignments.map { case (label, point) =>
          require(label >= 0 && label < bc.value.length, "Cluster label is outside the center array")
          require(point.size == dimensions && point.toArray.forall(finite),
            "Points must have the same dimensions as centers and finite coordinates")
          val squared = Vectors.sqdist(point, bc.value(label))
          require(finite(squared), "Squared distance overflow; rescale the input")
          val distance = if (definition == IndexDefinition.Paper) math.sqrt(squared) else squared
          (label, Summary(1L, distance, squared, distance))
        }.reduceByKey(_ + _).collect().toMap
      }
    } finally bc.destroy()
    require(stats.size == centers.length, "Every center must have at least one assigned point")
    require(stats.values.forall(s => finite(s.distance) && finite(s.squared)),
      "Distance sum overflow; rescale the input")
    val k = centers.length
    val radii = Array.tabulate(k)(i => stats(i).distance / stats(i).count)
    def distance(a: Vector, b: Vector): Double = {
      val squared = Vectors.sqdist(a, b)
      require(finite(squared), "Center distance overflow; rescale the input")
      if (definition == IndexDefinition.Paper) math.sqrt(squared) else squared
    }
    val (silhouette, silhouetteMs) = timed {
      val intra = if (definition == IndexDefinition.Paper) radii.sum / k
        else stats.values.map(_.distance).sum / stats.values.map(_.count).sum
      val inter = if (definition == IndexDefinition.Paper) {
        val global = Vectors.dense(Array.tabulate(dimensions)(j => centers.map(_(j) / k).sum))
        centers.map(distance(_, global)).sum / k
      } else {
        var sum = 0.0
        for (i <- centers.indices; j <- 0 until i) sum += distance(centers(i), centers(j))
        sum / (k.toDouble * (k - 1) / 2)
      }
      val denominator = math.max(intra, inter)
      if (denominator == 0) Double.NaN else (inter - intra) / denominator
    }
    val (dunn, dunnMs) = timed {
      val separation = if (definition == IndexDefinition.Paper) {
        val global = Vectors.dense(Array.tabulate(dimensions)(j => centers.map(_(j) / k).sum))
        centers.map(distance(_, global)).min
      } else {
        var minimum = Double.PositiveInfinity
        for (i <- centers.indices; j <- 0 until i)
          minimum = math.min(minimum, distance(centers(i), centers(j)))
        minimum
      }
      val radius = stats.values.map(_.maximum).max
      if (radius == 0) Double.NaN else separation / radius
    }
    val (db, dbMs) = timed {
      if (k < 2) Double.NaN
      else {
        val maxima = Array.fill(k)(0.0)
        var coincident = false
        for (i <- centers.indices; j <- 0 until i) {
          val separation = distance(centers(i), centers(j))
          if (separation == 0) coincident = true
          else {
            val ratio = (radii(i) + radii(j)) / separation
            maxima(i) = math.max(maxima(i), ratio)
            maxima(j) = math.max(maxima(j), ratio)
          }
        }
        if (coincident) Double.NaN else maxima.sum / k
      }
    }
    val (wssse, wssseMs) = timed { stats.values.map(_.squared).sum }
    IndexResult(silhouette, dunn, db, wssse, statisticsMs, silhouetteMs, dunnMs, dbMs, wssseMs)
  }
}

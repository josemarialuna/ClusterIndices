package es.us.cluster

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.rdd.RDD

/** Utilities retained for source compatibility. Original author: José María Luna. */
object Utils {
  def whatTimeIsIt(): String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
  def whatDayIsIt(): String = LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE)
  def giveMeTime(): Long = System.currentTimeMillis()
  def dataToDouble(s: String): Double = if (s.isEmpty) 0.0 else s.toDouble

  def calculateMedian(values: List[Double]): Double = {
    require(values.nonEmpty, "Cannot calculate the median of an empty list")
    val sorted = values.sorted
    val middle = sorted.length / 2
    if (sorted.length % 2 == 0) sorted(middle - 1) / 2 + sorted(middle) / 2 else sorted(middle)
  }

  @deprecated("Use RDD[String].saveAsTextFile directly", "2.0.0")
  def printRDD(dataRDD: RDD[Unit], nameFile: String): Unit =
    dataRDD.map(_.toString).saveAsTextFile(nameFile)
}

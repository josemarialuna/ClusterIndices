package es.us.cluster

import org.apache.spark.{SparkConf, SparkContext}

/** Extract fields 1..5 from space-separated input and prepend a zero-based row ID. */
object MainIndex {
  def main(args: Array[String]): Unit = {
    require(args.length == 2 || args.length == 3, "Usage: MainIndex input output [partitions]")
    val partitions = if (args.length == 3) args(2).toInt else 16
    require(partitions > 0, "partitions must be positive")
    val conf = new SparkConf().setAppName("ClusterIndices: index input")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      sc.textFile(args(0), partitions).zipWithIndex().map { case (line, index) =>
        val fields = line.trim.split("\\s+")
        require(fields.length >= 6, s"Row ${index + 1}: expected at least six fields")
        (index.toString +: fields.slice(1, 6)).mkString("\t")
      }.coalesce(1).saveAsTextFile(args(1))
    } finally sc.stop()
  }
  def dataToDouble(s: String): Double = ClusterIndex.dataToDouble(s)
}

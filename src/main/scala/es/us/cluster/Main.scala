package es.us.cluster

import es.us.linkage.{Distance, Linkage}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

case class RunConfig(input: String, output: String, algorithm: String = "kmeans",
                     minK: Int = 2, maxK: Int = 10, iterations: Int = 100,
                     partitions: Int = 16, seed: Long = 42L, definition: String = "paper",
                     header: Boolean = false, dropColumns: Set[Int] = Set.empty,
                     linkage: String = "avg", maxLinkagePoints: Int = 2000,
                     checkpoint: String = "checkpoints", master: Option[String] = None)

/** Shared CSV reader and command line for reproducible experiments. */
object Main {
  val usage: String = """ClusterIndices
Usage: spark-submit --class es.us.cluster.Main --master local[2] clusterIndices.jar \
  --input data.csv --output results [options]
Options (each takes a value):
  --algorithm kmeans|bkm|linkage    --definition paper|legacy
  --min-k 2 --max-k 10             --iterations 100 --partitions 16 --seed 42
  --header true|false              --drop-columns 0,4 (zero-based indices)
  --linkage min|max|avg            --max-linkage-points 2000
  --checkpoint checkpoints        --master local[2] (optional override)
The output path must not already exist. Input must contain finite numeric features.
Linkage builds all point pairs; avg is WPGMA. Use --help to show this message.
"""
  def parse(args: Array[String]): RunConfig = {
    require(args.length % 2 == 0, usage)
    val pairs = args.grouped(2).map(a => a(0) -> a(1)).toSeq
    val known = Set("input", "output", "algorithm", "definition", "min-k", "max-k", "iterations",
      "partitions", "seed", "header", "drop-columns", "linkage", "max-linkage-points", "checkpoint", "master")
    require(pairs.forall { case (key, _) => key.startsWith("--") && known(key.drop(2)) }, "Unknown option. " + usage)
    require(pairs.map(_._1).distinct.size == pairs.size, "Duplicate options are not allowed")
    val values = pairs.toMap
    def get(key: String, default: String): String = values.getOrElse("--" + key, default)
    val config = RunConfig(get("input", ""), get("output", ""), get("algorithm", "kmeans"),
      get("min-k", "2").toInt, get("max-k", "10").toInt, get("iterations", "100").toInt,
      get("partitions", "16").toInt, get("seed", "42").toLong, get("definition", "paper"),
      get("header", "false") match {
        case "true" => true
        case "false" => false
        case _ => throw new IllegalArgumentException("header must be true or false")
      }, get("drop-columns", "").split(",").filter(_.nonEmpty).map(_.toInt).toSet,
      get("linkage", "avg"), get("max-linkage-points", "2000").toInt,
      get("checkpoint", "checkpoints"), values.get("--master"))
    require(config.input.nonEmpty && config.output.nonEmpty, "--input and --output are required")
    require(Set("kmeans", "bkm", "linkage")(config.algorithm), "Unknown algorithm")
    IndexDefinition.parse(config.definition)
    require(config.minK >= 2 && config.maxK >= config.minK, "Expected 2 <= min-k <= max-k")
    require(config.iterations > 0 && config.partitions > 0, "iterations and partitions must be positive")
    require(config.dropColumns.forall(_ >= 0), "Column indices must be non-negative")
    require(Set("min", "max", "avg")(config.linkage), "Invalid linkage strategy")
    require(config.maxLinkagePoints >= 2 && config.checkpoint.nonEmpty, "Invalid Linkage configuration")
    config
  }

  def parseRow(line: String, row: Long, drop: Set[Int]): Vector = {
    val cells = line.split(",", -1)
    require(drop.forall(_ < cells.length), s"Row $row: dropped column does not exist")
    val selected = cells.indices.filterNot(drop).map { i =>
      val value = try cells(i).trim.toDouble catch {
        case _: NumberFormatException => throw new IllegalArgumentException(s"Row $row, column $i: expected a number")
      }
      require(!value.isNaN && !value.isInfinity, s"Row $row, column $i: expected a finite number")
      value
    }
    require(selected.nonEmpty, s"Row $row: no feature columns remain")
    Vectors.dense(selected.toArray)
  }

  def load(sc: SparkContext, config: RunConfig): RDD[Vector] =
    sc.textFile(config.input, config.partitions).zipWithIndex()
      .filter { case (_, index) => !config.header || index != 0 }
      .map { case (line, index) => parseRow(line, index + 1, config.dropColumns) }

  def main(args: Array[String]): Unit = {
    if (args.sameElements(Array("--help"))) { println(usage); return }
    run(parse(args))
  }

  def run(config: RunConfig): Unit = {
    require(config.input.nonEmpty && config.output.nonEmpty, "Input and output are required")
    require(Set("kmeans", "bkm", "linkage")(config.algorithm), "Unknown algorithm")
    require(config.minK >= 2 && config.maxK >= config.minK, "Invalid cluster range")
    require(config.iterations > 0 && config.partitions > 0, "Iterations and partitions must be positive")
    IndexDefinition.parse(config.definition)
    val conf = new SparkConf().setAppName("ClusterIndices")
    config.master.foreach(conf.setMaster)
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      val output = new org.apache.hadoop.fs.Path(config.output)
      require(!output.getFileSystem(sc.hadoopConfiguration).exists(output), "Output path already exists")
      val data = load(sc, config).persist(StorageLevel.MEMORY_AND_DISK)
      try {
        val count = data.count()
        require(count >= config.maxK, "Input must contain at least max-k points")
        require(data.map(_.size).distinct().take(2).length == 1, "Rows have inconsistent feature counts")
        val definition = IndexDefinition.parse(config.definition)
        def row(k: Int, result: IndexResult): String =
          Seq(config.algorithm, config.definition, config.seed, k, result.bdSilhouette, result.bdDunn,
            result.daviesBouldin, result.wssse, result.statisticsMs, result.silhouetteMs,
            result.dunnMs, result.daviesBouldinMs, result.wssseMs).mkString("\t")
        val rows = if (config.algorithm == "linkage") {
          require(count <= config.maxLinkagePoints && count <= Int.MaxValue / 2,
            "Linkage exceeds max-linkage-points; its pair matrix is quadratic. Raise the limit explicitly or use kmeans/bkm")
          sc.setCheckpointDir(config.checkpoint)
          val coordinates = data.zipWithIndex().map { case (v, i) => ((i + 1).toInt, v) }.cache()
          try {
            val distances = coordinates.cartesian(coordinates).filter { case (a, b) => a._1 < b._1 }
              .map { case (a, b) => new Distance(a._1, b._1, math.sqrt(Vectors.sqdist(a._2, b._2)).toFloat) }
              .repartition(config.partitions)
            val model = new Linkage(config.minK, config.linkage).runAlgorithm(distances, count.toInt)
            (config.minK to config.maxK).map { k =>
              val membership = model.createClusters(count.toInt, k, sc.parallelize(1 to count.toInt)).cache()
              try {
                val centers = model.inicializeCenters(coordinates, k, count.toInt, 1, membership)
                val labels = membership.values.distinct().collect().sorted.zipWithIndex.toMap
                val assigned = coordinates.join(membership).map { case (_, (point, id)) => (labels(id), point) }
                row(k, ValidityIndices.evaluate(assigned, centers, definition))
              } finally membership.unpersist()
            }
          } finally coordinates.unpersist()
        } else (config.minK to config.maxK).map { k =>
          val result = if (config.algorithm == "kmeans")
            ClusterIndex.evaluateKMeans(data, k, config.iterations, config.seed, definition)
          else ClusterIndex.evaluateBKM(data, k, config.iterations, config.seed, definition)
          row(k, result)
        }
        val header = "algorithm\tdefinition\tseed\tk\tbd_silhouette\tbd_dunn\tdavies_bouldin\twssse\tstatistics_ms\tsilhouette_ms\tdunn_ms\tdavies_bouldin_ms\twssse_ms"
        sc.parallelize(header +: rows, 1).saveAsTextFile(config.output)
        val metadata = new java.util.Properties()
        Map("software.version" -> "2.0.0-SNAPSHOT", "spark.version" -> sc.version,
          "java.version" -> System.getProperty("java.version"), "input" -> config.input,
          "algorithm" -> config.algorithm, "definition" -> config.definition,
          "seed" -> config.seed.toString, "min.k" -> config.minK.toString,
          "max.k" -> config.maxK.toString, "iterations" -> config.iterations.toString,
          "requested.partitions" -> config.partitions.toString, "actual.partitions" -> data.getNumPartitions.toString,
          "rows" -> count.toString, "features" -> data.first().size.toString,
          "header" -> config.header.toString, "drop.columns" -> config.dropColumns.toSeq.sorted.mkString(","),
          "linkage.strategy" -> config.linkage, "master" -> sc.master,
          "timing.unit" -> "milliseconds", "timing.excludes" -> "training,input,linkage,centroids")
          .foreach { case (key, value) => metadata.setProperty(key, value) }
        val stream = output.getFileSystem(sc.hadoopConfiguration)
          .create(new org.apache.hadoop.fs.Path(output, "_metadata.properties"), false)
        try metadata.store(stream, "ClusterIndices experiment configuration") finally stream.close()
      } finally data.unpersist()
    } finally sc.stop()
  }
}

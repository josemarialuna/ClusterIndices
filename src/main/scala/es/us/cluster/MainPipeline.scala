package es.us.cluster

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Main object for testing clustering methods using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainPipeline {
  def main(args: Array[String]) {


    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val fileOriginal = "c:\\datasets\\trabajadores.csv"

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()
    var numClusters = 256
    var numIterations = 100
/*
    if (args.size > 1) {
      origen = args(0)
      destino = args(1)
      numClusters = args(2).toInt
      numIterations = args(3).toInt
    }
*/
    val dataset = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(origen)


    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L).setMaxIter(numIterations)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    sparkSession.stop()
  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

}


package es.us.cluster

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Main object for testing clustering methods using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainTest {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark Cluster")
      .setMaster("local[*]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    //var path = "C:\\datasets\\GeneratedMatrix\\"
    //var fileName = "matrizRelativa"
    var path = "C:\\datasets\\art2\\005\\"
    var fileName = "C5-D20-I10000.csv"

    var origen: String = path + fileName
    var destino: String = Utils.whatTimeIsIt()
    var minNumCluster = 2
    var maxNumCluster = 10
    var numIterations = 100
    var numPartitions = 16

    if (args.size > 2) {
      path = args(0).toString
      fileName = args(1).toString
      destino = args(2)
      minNumCluster = args(3).toInt
      maxNumCluster = args(4).toInt
      numIterations = args(5).toInt
      numPartitions = args(6).toInt

      origen = path + fileName
    }

    val data = sc.textFile(if (args.length > 2) args(0) else origen, numPartitions)

    // Load and parse the data
    //val dataRDDSplitted = data.map(x => x.split(","))
    //It skips the first line
    //val dataRDDSkipped = dataRDDSplitted.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val dataRDD = data
      .map(s => s.split(",")
        .map(_.toDouble))
      .keyBy(_.apply(0))
      .cache()


    val parsedData = dataRDD.map(s => Vectors.dense(s._2)).cache()


    println("*******************************")
    println("*********CLUSTER SPARK*********")
    println("*******************************")
    println("Configuration:")
    println("\tCLUSTERS: " + minNumCluster + "-" + maxNumCluster)
    println("\tInput file: " + origen)
    println("\tOutput File: " + destino)
    println("Running...\n")
    println("Loading file..")


    val resultado = for {i <- minNumCluster to maxNumCluster} yield {
      println("*** K = " + i + " ***")
      println("Executing Indices")
      val indices = ClusterIndex.getIndicesKMeans(parsedData, i, numIterations)
      println("VALUES:")
      println("\tSilhouette: " + indices._1)
      println("\tDunn: " + indices._2)
      println("\tDavies-Bouldin: " + indices._3)
      println("\tWSSSE: " + indices._4)
      println("Elapsed Time:")
      println("\tTime Silhouette: " + indices._5)
      println("\tTime Dunn: " + indices._6)
      println("\tTime Davies-Bouldin: " + indices._3)
      println("\tTime WSSSE: " + indices._4)
      println("\n")

      (i, indices)
    }


    val stringRdd = sc.parallelize(resultado)
    stringRdd.repartition(1).saveAsTextFile(destino + "Results-" + fileName + "-" + Utils.whatTimeIsIt() + ".csv")

    sc.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

}


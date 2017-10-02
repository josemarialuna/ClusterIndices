package es.us.cluster

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Main object for testing clustering methods using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainIndex {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark Cluster")
      .setMaster("local[*]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    //var path = "C:\\datasets\\GeneratedMatrix\\"
    //var fileName = "matrizRelativa"
    var path = "C:\\Users\\Josem\\CloudStation\\Edificios procesados por separado\\"
    var fileName = "EDIF_COMPLETO"

    var origen: String = path + fileName
    var destino: String = path + fileName + "_" + Utils.whatTimeIsIt()

    var numPartitions = 16

    if (args.size > 2) {
      path = args(0).toString
      fileName = args(1).toString
      destino = args(2)
      numPartitions = args(3).toInt

      origen = path + fileName
    }

    val data = sc.textFile(if (args.length > 2) args(0) else origen, numPartitions)


    println("*******************************")
    println("***********INDEX FILE**********")
    println("*******************************")
    println("Configuration:")
    println("\tInput file: " + origen)
    println("\tOutput File: " + destino)
    println("Running...\n")
    println("Loading file..")

    val dataRDD = data
      .map(s => s.split(" "))
      .map(x => (x.apply(1), x.apply(2), x.apply(3), x.apply(4), x.apply(5)))
      .zipWithIndex()
      .map(_.swap)


    dataRDD.repartition(1)
      .mapValues(_.toString().replace(",", "\t").replace("(", "").replace(")", ""))
      .map(x => x._1.toInt + "\t" + x._2)
      .saveAsTextFile(destino)


    sc.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

}


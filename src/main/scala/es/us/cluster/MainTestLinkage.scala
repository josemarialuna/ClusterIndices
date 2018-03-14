package es.us.cluster

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.storage.StorageLevel

/**
  * Main object for testing clustering methods using Linkage in SPARK MLLIB
  *
  * @author José David Martín
  * @version 1.0
  * @since v1.0 Dev
  */
object MainTestLinkage {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Spark Cluster")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    var path = ""
//    var fileName = "C5-D20-I1000.csv"
    var fileName = "DataBase500p"
//    var fileName = "B:\\Datasets\\irisData.txt"

    var origen: String = path + fileName
    var destino: String = path + fileName

    var numPoints = 500
    var clusterFilterNumber = 1
    val strategyDistance = "avg"

    var typDataSet = 1
    var idIndex = "_c0"
    var classIndex = "_c0"
    var distanceMethod = "Euclidean"

    var minNumCluster = 2
    var maxNumCluster = 10
    var numPartitions = 16

    if (args.size > 2) {
      origen = args(0).toString
      destino = args(1)
      minNumCluster = args(2).toInt
      maxNumCluster = args(3).toInt
      numPartitions = args(4).toInt

    }

    //Load data from csv
    val dataDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(origen)

    //Filtering DataFrame
    val dataDFFiltered = typDataSet match{
      //It is not necessary to remove any column
      case 0 =>
        dataDF.map(_.toSeq.asInstanceOf[Seq[Double]])
      //It is necessary to delete the class column
      case 1 =>
        dataDF.drop(classIndex).map(_.toSeq.asInstanceOf[Seq[Double]])
      //It is necessary to eliminate both the identifier and the class
      case 2 =>
        dataDF.drop(idIndex,classIndex).map(_.toSeq.asInstanceOf[Seq[Double]])
    }

    val parsedData = dataDFFiltered.rdd.map(s => Vectors.dense(s.toArray))

    //We automatically generate an index for each row
    val dataAux = dataDFFiltered.withColumn("index", monotonically_increasing_id()+1)

    //Save dataAux for futures uses
    val coordinates = dataAux.map(row => (row.getLong(1), row.getSeq[Double](0).toList))
      .rdd
      .sortByKey()
      .map(_.toString().replace("(", "").replace("))", ")").replace("List", "(").replace(",(", ";("))

    val coordinatesRDD = coordinates
      .map(s => s.split(";"))
      .map(row => (row(0).toInt, Vectors.dense(row(1).replace("(", "").replace(")", "").split(",").map(_.toDouble))))

    //Rename the columns and generate a new DataFrame copy of the previous to be able to do the subsequent filtered out in the join
    val newColumnsNames = Seq("valueAux", "indexAux")
    val dataAuxRenamed = dataAux.toDF(newColumnsNames: _*)

    val distances = dataAux.crossJoin(dataAuxRenamed)
      .filter(r => r.getLong(1) < r.getLong(3))
      .map{r =>
        //Depending on the method we choose to perform the distance, the value of the same will change
        val dist = distanceMethod match {

          case "Euclidean" =>
            distEuclidean(r.getSeq[Double](0), r.getSeq[Double](2))
        }

        //We return the result saving: (point 1, point 2, the distance that separates both)
        (r.getLong(1), r.getLong(3), dist)
      }

    val distancesRDD = distances.rdd.map(_.toString().replace("(", "").replace(")", ""))
      .map(s => s.split(',').map(_.toFloat))
      .map { case x =>
        new Distance(x(0).toInt, x(1).toInt, x(2))
      }.filter(x => x.getIdW1 < x.getIdW2).repartition(numPartitions)

    println("*******************************")
    println("*********CLUSTER SPARK*********")
    println("*******************************")
    println("Configuration:")
    println("\tCLUSTERS: " + minNumCluster + "-" + maxNumCluster)
    println("\tInput file: " + origen)
    println("\tOutput File: " + destino)
    println("Running...\n")
    println("Loading file..")


    val result = ClusterIndex.getIndicesLinkage(parsedData, coordinatesRDD, distancesRDD, numPoints, clusterFilterNumber,
      strategyDistance, minNumCluster, maxNumCluster).sortByKey()

    val resultado = result.map { case k =>
      (k._1, k._2)
    }

    val stringRdd = resultado

    stringRdd.repartition(1)
      .mapValues(_.toString().replace(",", "\t").replace("(", "").replace(")", ""))
      .map(x => x._1.toInt + "\t" + x._2)
      .saveAsTextFile(destino + "-Results-" + Utils.whatTimeIsIt())

    spark.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

  //Method for calculating de Euclidean distante between two points
  def distEuclidean(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    var kv = 0
    val sz = v1.size
    while (kv < sz) {
      val score = v1.apply(kv) - v2.apply(kv)
      squaredDistance += Math.abs(score*score)
      kv += 1
    }
    math.sqrt(squaredDistance)
  }
}

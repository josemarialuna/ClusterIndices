package es.us.cluster

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

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
    var fileName = "C5-D20-I1000.csv"
//    var fileName = "DataBase500pC7"
//    var fileName = "B:\\Datasets\\irisData.txt"

    var origen: String = path + fileName
    var destino: String = path + fileName

    /* Set up the number of points to the data, the minimum number of points per centroid
    and the strategy distance to run linkage algorithm */
    var numPoints = 5000
    var clusterFilterNumber = 1
    var strategyDistance = "avg"

    /* Set up the type of the data, its id column, its class column ("_cX" format, since 0 until length - 1)
    and the method to calculate the distance between points */
    var typDataSet = 0
    var idIndex = "_c0"
    var classIndex = "_c4"
    var distanceMethod = "Euclidean"

    //Set up the minimum and maximum number of cluster and the number of partitions
    var minNumCluster = 2
    var maxNumCluster = 10
    var numPartitions = 16

    if (args.size > 2) {
      origen = args(0).toString
      destino = args(1).toString
      numPoints = args(2).toInt
      clusterFilterNumber = args(3).toInt
      strategyDistance = args(4).toString
      typDataSet = args(5).toInt
      idIndex = args(6).toString
      classIndex = args(7).toString
      distanceMethod = args(8).toString
      minNumCluster = args(9).toInt
      maxNumCluster = args(10).toInt
      numPartitions = args(11).toInt
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

    //Save the coordinates of all points in a RDD[Vector]
    val parsedData = dataDFFiltered.rdd.map(s => Vectors.dense(s.toArray))

    //Generate automatically an index for each row
    val dataAux = dataDFFiltered.withColumn("index", monotonically_increasing_id()+1)

    //Save dataAux for futures uses
    val coordinates = dataAux.map(row => (row.getLong(1), row.getSeq[Double](0).toList))
      .rdd
      .sortByKey()
      .map(_.toString().replace("(", "").replace("))", ")").replace("List", "(").replace(",(", ";("))

    //Save the id and the coordinates of all points in a RDD[(Int,Vector)]
    val coordinatesRDD = coordinates
      .map(s => s.split(";"))
      .map(row => (row(0).toInt, Vectors.dense(row(1).replace("(", "").replace(")", "").split(",").map(_.toDouble))))

    //Rename the columns and generate a new DataFrame copy of the previous to be able to do the subsequent filtered out in the join
    val newColumnsNames = Seq("valueAux", "indexAux")
    val dataAuxRenamed = dataAux.toDF(newColumnsNames: _*)

    //Calculate the distance between all points
    val distances = dataAux.crossJoin(dataAuxRenamed)
      .filter(r => r.getLong(1) < r.getLong(3))
      .map{r =>
        //Depending on the method chosen one to perform the distance, the value of the same will change
        val dist = distanceMethod match {

          case "Euclidean" =>
            distEuclidean(r.getSeq[Double](0), r.getSeq[Double](2))
        }

        //Return the result saving: (point 1, point 2, the distance between both)
        (r.getLong(1), r.getLong(3), dist)
      }

    //Save the distances between all points in a RDD[Distance]
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


    //Run the Linkage algorithm with the parameters choose
    val result = ClusterIndex.getIndicesLinkage(parsedData, coordinatesRDD, distancesRDD, numPoints, clusterFilterNumber,
      strategyDistance, minNumCluster, maxNumCluster).sortByKey()

    //Save the result in a RDD[String]
    val stringRDD = result.map { case k =>
      (k._1, k._2)
    }

    //Save the result in a external file
    stringRDD.coalesce(1)
      .mapValues(_.toString().replace(",", "\t").replace("(", "").replace(")", ""))
      .map(x => x._1.toInt + "\t" + x._2)
      .saveAsTextFile(destino + "-Results-" + Utils.whatTimeIsIt())

    spark.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

  //Calculate de Euclidean distance between two points
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

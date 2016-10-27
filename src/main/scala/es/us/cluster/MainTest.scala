package es.us.cluster

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
/*
    val conf = new SparkConf()
      .setAppName("Spark Cluster")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val fileOriginal = "c:\\datasets\\randomsetN4.csv"

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()
    var numClusters = 256
    var numIterations = 100

    if (args.size > 1) {
      origen = args(0)
      destino = args(1)
      numClusters = args(2).toInt
      numIterations = args(3).toInt
    }


    val datos = sc.textFile(if (args.length > 2) args(0) else origen)

    // Load and parse the data
    val dataRDDSplitted = datos.map(x => x.split(";"))
    //It skips the first line
    val dataRDDSkipped = dataRDDSplitted.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    //val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble, x(15).toDouble, x(16).toDouble, x(17).toDouble, x(18).toDouble, x(19).toDouble, x(20).toDouble, x(21).toDouble, x(22).toDouble, x(23).toDouble, x(24).toDouble)))
    //val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble, x(15).toDouble, x(16).toDouble, x(17).toDouble, x(18).toDouble, x(19).toDouble, x(20).toDouble, x(21).toDouble, x(22).toDouble, x(23).toDouble, x(24).toDouble,x(25).toDouble,x(26).toDouble,x(27).toDouble,x(28).toDouble,x(29).toDouble,x(30).toDouble, x(31).toDouble, x(32).toDouble, x(33).toDouble, x(34).toDouble, x(35).toDouble, x(36).toDouble, x(37).toDouble, x(38).toDouble, x(39).toDouble, x(40).toDouble,x(41).toDouble, x(42).toDouble, x(43).toDouble, x(44).toDouble, x(45).toDouble, x(46).toDouble, x(47).toDouble, x(48).toDouble, x(49).toDouble, x(50).toDouble,x(51).toDouble, x(52).toDouble, x(53).toDouble, x(54).toDouble, x(55).toDouble, x(56).toDouble, x(57).toDouble, x(58).toDouble, x(59).toDouble, x(60).toDouble,x(61).toDouble, x(62).toDouble, x(63).toDouble, x(64).toDouble, x(65).toDouble, x(66).toDouble, x(67).toDouble, x(68).toDouble))).cache()
    //val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble))).cache()
    val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble))).cache()
    //val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble))).cache()
    //val parsedData = dataRDDSkipped.map(x => Vectors.dense(Array(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble))).cache()


    /** *****KMEANS ********/
    val modelName = ClusterIndex.kMeansCreateModel(sc, parsedData, 4, 100)
    val clustersKM = KMeansModel.load(sc, modelName)

    sc.parallelize(clustersKM.clusterCenters).coalesce(1, shuffle = true).saveAsTextFile(modelName + "-KMcentroids")

    /** *****END KMEANS ********/



    /** *****BISECTINGKMEANS ********/
    val clustersBKM = new BisectingKMeans().setK(4).setMaxIterations(100).run(parsedData)

    sc.parallelize(clustersBKM.clusterCenters).coalesce(1, shuffle = true).saveAsTextFile(modelName + "-BKMcentroids")

    /** *****END BISECTINGKMEANS ********/
    //It returns idCliente, cluster
    val dataClustered = parsedData.map(x => (clustersBKM.predict(x), 1)).reduceByKey((a, b) => a + b)

    dataClustered.coalesce(1).saveAsTextFile(modelName + ".csv")

    sc.stop()
    */
  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

}


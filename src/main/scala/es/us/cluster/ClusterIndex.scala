package es.us.cluster

import java.io.{File, PrintWriter}

import akka.io.Udp.SO.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

/**
  * This object contains two methods that calculates the optimal number for
  * clustering using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object ClusterIndex {

  /**
    * @usecase Create a file with Silhoutte index results applying K-means
    * @param parsedData     RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param maxNumClusters Set the number of cluster you want to analyze
    * @param numIterations  Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example silhoutte(sc, parsedData, 20, 100)
    */
  def silhoutteKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = "k;BDSilhouette;elapsedTime" + "\n"
    val sc = parsedData.sparkContext

    //evaluates number of clusters
    for (i <- 2 to maxNumClusters) {
      val startTimeK = System.currentTimeMillis

      val clusters = KMeans.train(parsedData, i, numIterations)
      val intraMean = clusters.computeCost(parsedData) / parsedData.count()

      //Global Center
      val centroides = sc.parallelize(clusters.clusterCenters)
      val clusterCentroides = KMeans.train(centroides, 1, numIterations)
      val interMean = clusterCentroides.computeCost(centroides) / centroides.count()

      //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
      val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
      s += i + ";" + silhoutte + ";"

      val stopTimeK = System.currentTimeMillis
      val elapsedTime = (stopTimeK - startTimeK)
      s += elapsedTime
      s += "\n"

    }


    //Save number of cluster distances results
    val pw = new PrintWriter(new File("SilhoutteKM-" + Utils.whatTimeIsIt() + "-" + numIterations + "it-" + ".csv"))
    try pw.write(s) finally pw.close()

  }


  def silhoutteKMeansInter(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""

    //evaluates number of clusters
    for (i <- 2 to maxNumClusters) {

      val clusters = KMeans.train(parsedData, i, numIterations)
      val intraMean = clusters.computeCost(parsedData) / parsedData.count()

      //Global Center
      val centroides = sc.parallelize(clusters.clusterCenters).cache()
      val clusterCentroides = KMeans.train(centroides, 1, numIterations)
      //val interMean = clusterCentroides.computeCost(centroides) / centroides.count()
      var interMean = 0.0
      var aux = sc.accumulator(0.0)
      var j = 0
      for (j <- 0 to clusters.clusterCenters.length - 1) {
        val bdCenter = sc.broadcast(clusters.clusterCenters(j))
        aux.value = centroides.map(a => Vectors.sqdist(bdCenter.value, a)).max()
        if (aux.value > interMean)
          interMean = aux.value
      }


      //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
      val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
      s += i + ";" + interMean + " ; " + intraMean + " ; " + silhoutte + "\n"
    }


    //Save number of cluster distances results
    val pw = new PrintWriter(new File("SilhoutteKM-" + Utils.whatTimeIsIt() + "-" + numIterations + "it-" + ".csv"))
    try pw.write(s) finally pw.close()

  }

  /**
    * @usecase Create a file with an approach Dunn index results applying K-means
    * @param parsedData     RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param maxNumClusters Set the number of cluster you want to analyze
    * @param numIterations  Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example dunn(sc, parsedData, 20, 100)
    */
  def dunnKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = "k;BDDunn;elapsedtime\n"
    val sc = parsedData.sparkContext
    //evaluates number of clusters
    //Results: 6 clusters is the first minimum
    for (i <- 2 to maxNumClusters) {
      val startTimeK = System.currentTimeMillis

      val clusters = KMeans.train(parsedData, i, numIterations)

      //Global Centroid
      val centroides = sc.parallelize(clusters.clusterCenters)
      val clusterCentroides = KMeans.train(parsedData, 1, numIterations)

      //Min distance from centroids to global centroid
      val minA = centroides.map { x =>
        Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
      }.min()

      //Max distance from points to its centroid
      val maxB = parsedData.map { x =>
        Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      }.max

      //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
      val dunn = minA / maxB
      s += i + ";" + dunn + ";"

      val stopTimeK = System.currentTimeMillis
      val elapsedTime = (stopTimeK - startTimeK)
      s += elapsedTime
      s += "\n"
    }


    //Save number of cluster distances results
    val pw = new PrintWriter(new File("DunnKM-" + Utils.whatTimeIsIt() + "-" + numIterations + "it-" + ".csv"))
    try pw.write(s) finally pw.close()
  }


  def checkEfficacyKMeans(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""


    val clusters = KMeans.train(parsedData, maxNumClusters, numIterations)

    val checkRDD = parsedData.map(x => (x, clusters.predict(x)))

    checkRDD.coalesce(1).saveAsTextFile("checkEfficacy-" + Utils.whatTimeIsIt())

  }

  /**
    * @usecase Create a file with Silhoutte index results applying K-means
    * @param sc             SparkContext defined
    * @param parsedData     RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param maxNumClusters Set the number of cluster you want to analyze
    * @param numIterations  Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example silhoutte(sc, parsedData, 20, 100)
    */
  def silhoutteBKM(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""

    //evaluates number of clusters
    for (i <- 2 to maxNumClusters) {

      val clusters = new BisectingKMeans().setK(i).setMaxIterations(numIterations).run(parsedData)
      val intraMean = clusters.computeCost(parsedData) / parsedData.count()

      //Global Center
      val centroides = sc.parallelize(clusters.clusterCenters)
      val clusterCentroides = KMeans.train(centroides, 1, numIterations)
      val interMean = clusterCentroides.computeCost(centroides) / centroides.count()

      //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
      val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
      s += i + ";" + interMean + " ; " + intraMean + " ; " + silhoutte + "\n"
    }


    //Save number of cluster distances results
    val pw = new PrintWriter(new File("SilhoutteBKM-" + Utils.whatTimeIsIt() + "-" + numIterations + "it-" + ".csv"))
    try pw.write(s) finally pw.close()

  }

  /**
    * @usecase Create a file with an approach Dunn index results applying K-means
    * @param sc             SparkContext defined
    * @param parsedData     RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param maxNumClusters Set the number of cluster you want to analyze
    * @param numIterations  Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example dunn(sc, parsedData, 20, 100)
    */
  def dunnBKM(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""
    //evaluates number of clusters
    //Results: 6 clusters is the first minimum
    for (i <- 2 to maxNumClusters) {

      val clusters = new BisectingKMeans().setK(i).setMaxIterations(numIterations).run(parsedData)

      //Global Centroid
      val centroides = sc.parallelize(clusters.clusterCenters)
      val clusterCentroides = KMeans.train(parsedData, 1, numIterations)

      //Min distance from centroids to global centroid
      val minA = centroides.map { x =>
        Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
      }.min()

      //Max distance from points to its centroid
      val maxB = parsedData.map { x =>
        Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      }.max

      //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
      val dunn = minA / maxB
      s += i + ";" + minA + " ; " + maxB + " ; " + dunn + "\n"
    }


    //Save number of cluster distances results
    val pw = new PrintWriter(new File("DunnBKM-" + Utils.whatTimeIsIt() + "-" + numIterations + "it-" + ".csv"))
    try pw.write(s) finally pw.close()
  }

  /**
    * @usecase Apply Bisecting K-Means to a given RDD
    * @param sc            SparkContext defined
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of cluster to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example bisectingKMCreateModel(sc, parsedData, 20, 100)
    */
  def bisectingKMCreateModel(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): Unit = {

    val clusters = new BisectingKMeans().setK(numClusters).setMaxIterations(numIterations).run(parsedData)
    // Save and load model
    val modelName = Utils.whatTimeIsIt + "-" + numClusters + "Clusters-" + numIterations + "It-Model"
    //clusters.save(sc, modelName)

  }

  /**
    * @usecase Create K-Means model to a given RDD
    * @param sc            SparkContext defined
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of cluster to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return prints results in a csv file. It prints 4 columns:
    *         First is the number of cluster.
    *         Second and third are auxiliar results.
    *         Fourth column is the result.
    * @example kMeansCreateModel(sc, parsedData, 20, 100)
    */
  def kMeansCreateModel(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): String = {

    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // Save and load model
    val modelName = Utils.whatTimeIsIt + "-" + numClusters + "Clusters-" + numIterations + "It-Model"
    clusters.save(sc, modelName)

    sc.parallelize(clusters.clusterCenters).coalesce(1, shuffle = true).saveAsTextFile(modelName + "-centroids")

    return modelName
  }


  def checkEfficacyBKM(sc: SparkContext, parsedData: RDD[org.apache.spark.mllib.linalg.Vector], maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""


    val clusters = new BisectingKMeans().setK(maxNumClusters).setMaxIterations(numIterations).run(parsedData)

    val checkRDD = parsedData.map(x => (x, clusters.predict(x)))

    checkRDD.coalesce(1).saveAsTextFile("checkEfficacy-" + Utils.whatTimeIsIt())

  }


  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }


  /**
    * @usecase Return Silhouette indice for a given k
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of clusters to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return A tuple composed by the silhouette value and the elapsed time
    * @example getSilhoutteKMeans(parsedData, 6, 100)
    */
  def getSilhoutteKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): (Double, Float) = {
    var i = 1
    var s = ""
    val sc = parsedData.sparkContext

    val startTimeK = System.currentTimeMillis

    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val intraMean = clusters.computeCost(parsedData) / parsedData.count()

    //Global Center
    val centroides = sc.parallelize(clusters.clusterCenters)
    val clusterCentroides = KMeans.train(centroides, 1, numIterations)
    val interMean = clusterCentroides.computeCost(centroides) / centroides.count()

    //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
    val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
    s += i + ";" + silhoutte + ";"

    val stopTimeK = System.currentTimeMillis
    val elapsedTime = (stopTimeK - startTimeK)

    (silhoutte, elapsedTime)

  }

  /**
    * @usecase Return Dunn indice for a given k
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of clusters to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return A tuple composed by the dunn value and the elapsed time
    * @example getSilhoutteKMeans(parsedData, 6, 100)
    */
  def getDunnKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): (Double, Float) = {
    val sc = parsedData.sparkContext

    val startTimeK = System.currentTimeMillis

    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    //Global Centroid
    val centroides = sc.parallelize(clusters.clusterCenters)
    val clusterCentroides = KMeans.train(parsedData, 1, numIterations)

    //Min distance from centroids to global centroid
    val minA = centroides.map { x =>
      Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
    }.min()

    //Max distance from points to its centroid
    val maxB = parsedData.map { x =>
      Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
    }.max

    //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
    val dunn = minA / maxB

    val stopTimeK = System.currentTimeMillis
    val elapsedTime = (stopTimeK - startTimeK)

    (dunn, elapsedTime)
  }

  /**
    * @usecase Return Silhouette indice for a given k
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of clusters to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return A tuple composed by the silhouette value and the elapsed time
    * @example getSilhoutteKMeans(parsedData, 6, 100)
    */
  def getIndicesKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): (Double, Double, Double, Double, Long, Long, Long, Long) = {
    var i = 1
    var s = ""
    val sc = parsedData.sparkContext

    val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||", Utils.giveMeTime())

    //Global Center
    val centroides = sc.parallelize(clusters.clusterCenters)
    val centroidesCartesian = centroides.cartesian(centroides).filter(x => x._1 != x._2).cache()

    var startTimeK = System.currentTimeMillis

    val intraMean = clusters.computeCost(parsedData) / parsedData.count()
    val interMeanAux = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).reduce(_ + _)
    val interMean = interMeanAux / centroidesCartesian.count()
    /*val clusterCentroides = KMeans.train(centroides, 1, numIterations)
    val interMean = clusterCentroides.computeCost(centroides) / centroides.count()
*/
    //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
    val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
    s += i + ";" + silhoutte + ";"

    var stopTimeK = System.currentTimeMillis
    val elapsedTimeSil = (stopTimeK - startTimeK)


    //DUNN
    startTimeK = System.currentTimeMillis

    //Min distance between centroids
    val minA = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).min()

    /*
    //Min distance from centroids to global centroid
    val minA = centroides.map { x =>
      Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
    }.min()
*/
    //Max distance from points to its centroid
    val maxB = parsedData.map { x =>
      Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
    }.max

    //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
    val dunn = minA / maxB

    stopTimeK = System.currentTimeMillis
    val elapsedTime = (stopTimeK - startTimeK)

    //DAVIES-BOULDIN
    startTimeK = System.currentTimeMillis

    val avgCentroid = parsedData.map { x =>
      //Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      (clusters.predict(x), x)
    }.map(x => (x._1, (Vectors.sqdist(x._2, clusters.clusterCenters(x._1)))))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .collectAsMap()

    val bcAvgCentroid = sc.broadcast(avgCentroid)

    val centroidesWithId = centroides.zipWithIndex()
      .map(_.swap).cache()

    val cartesianCentroides = centroidesWithId.cartesian(centroidesWithId).filter(x => x._1._1 != x._2._1)

    val davis = cartesianCentroides.map { case (x, y) => (x._1.toInt, (bcAvgCentroid.value(x._1.toInt) + bcAvgCentroid.value(y._1.toInt)) / Vectors.sqdist(x._2, y._2)) }
      .groupByKey()
      .map(_._2.max)
      .reduce(_ + _)

    val bouldin = davis / numClusters

    stopTimeK = System.currentTimeMillis
    val elapsedTimeDavies = (stopTimeK - startTimeK)

    //WSSSE
    startTimeK = System.currentTimeMillis
    val wssse = clusters.computeCost(parsedData)

    stopTimeK = System.currentTimeMillis
    val elapsedTimeW = (stopTimeK - startTimeK)

    (silhoutte, dunn, bouldin, wssse, elapsedTimeSil, elapsedTime, elapsedTimeDavies, elapsedTimeW)
  }

  def getFlatIndicesKMeans(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): (Double, Double, Double, Long, Double, Double, Long, Double, Int, Long) = {
    var i = 1
    var s = ""
    val sc = parsedData.sparkContext

    //val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||", Utils.giveMeTime())

    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    //Global Center
    val centroides = sc.parallelize(clusters.clusterCenters)
    val centroidesCartesian = centroides.cartesian(centroides).filter(x => x._1 != x._2).cache()

    var startTimeK = System.currentTimeMillis

    val intraMean = clusters.computeCost(parsedData) / parsedData.count()
    val interMeanAux = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).reduce(_ + _)
    val interMean = interMeanAux / centroidesCartesian.count()
    /*val clusterCentroides = KMeans.train(centroides, 1, numIterations)
    val interMean = clusterCentroides.computeCost(centroides) / centroides.count()
*/
    //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
    val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
    s += i + ";" + silhoutte + ";"

    var stopTimeK = System.currentTimeMillis
    val elapsedTimeSil = (stopTimeK - startTimeK)


    //DUNN
    startTimeK = System.currentTimeMillis

    //Min distance between centroids
    val minA = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).min()

    /*
    //Min distance from centroids to global centroid
    val minA = centroides.map { x =>
      Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
    }.min()
*/
    //Max distance from points to its centroid
    val maxB = parsedData.map { x =>
      Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
    }.max

    //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
    val dunn = minA / maxB

    stopTimeK = System.currentTimeMillis
    val elapsedTime = (stopTimeK - startTimeK)

    //DAVIES-BOULDIN
    startTimeK = System.currentTimeMillis

    val avgCentroid = parsedData.map { x =>
      //Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      (clusters.predict(x), x)
    }.map(x => (x._1, (Vectors.sqdist(x._2, clusters.clusterCenters(x._1)))))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .collectAsMap()

    val bcAvgCentroid = sc.broadcast(avgCentroid)

    val centroidesWithId = centroides.zipWithIndex()
      .map(_.swap).cache()

    val cartesianCentroides = centroidesWithId.cartesian(centroidesWithId).filter(x => x._1._1 != x._2._1)

    val davis = cartesianCentroides.map { case (x, y) => (x._1.toInt, (bcAvgCentroid.value(x._1.toInt) + bcAvgCentroid.value(y._1.toInt)) / Vectors.sqdist(x._2, y._2)) }
      .groupByKey()
      .map(_._2.max)
      .reduce(_ + _)

    val bouldin = davis / numClusters

    stopTimeK = System.currentTimeMillis
    val elapsedTimeDavies = (stopTimeK - startTimeK)

    (intraMean, interMeanAux, interMean, elapsedTimeSil, minA, maxB, elapsedTime, davis, numClusters, elapsedTimeDavies)
  }

}

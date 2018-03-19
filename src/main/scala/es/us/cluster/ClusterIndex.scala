package es.us.cluster

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * This object contains three methods that calculates the optimal number for
  * clustering using Kmeans, Bisecting KMeans or Linkage in SPARK MLLIB
  *
  * @author José María Luna and José David Martín
  * @version 1.0
  * @since v1.0 Dev
  */

object ClusterIndex extends Logging {


  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }


  /**
    * Return Silhouette, Dunn, Davies-Bouldin and WSSSE validity clustering indices and its time after using KMeans from Mllib
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of clusters to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return A tuple composed by the Silhouette, Dunn, Davies-Bouldin and WSSSE and its corresponding time
    * @example getIndicesKMeans(parsedData, 2, 500)
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

  /**
    * Return Silhouette, Dunn, Davies-Bouldin and WSSSE validity clustering indices and its time after using Bisecting KMeans from Mllib
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param numClusters   Set the number of clusters to apply
    * @param numIterations Set the number of iterations that Kmeans does
    * @return A tuple composed by the Silhouette, Dunn, Davies-Bouldin and WSSSE and its corresponding time
    * @example getIndicesBKM(parsedData, 2, 500)
    */
  def getIndicesBKM(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], numClusters: Int, numIterations: Int): (Double, Double, Double, Double, Long, Long, Long, Long) = {
    var i = 1
    var s = ""
    val sc = parsedData.sparkContext

    //val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||", Utils.giveMeTime())
    val clusters = new BisectingKMeans()
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .setSeed(Utils.giveMeTime())
      .run(parsedData)

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

  /**
    * Return Silhouette, Dunn, Davies-Bouldin and WSSSE validity clustering indices and its time after using Linkage
    * @param parsedData    RDD with parsed data ready to cluster. Its values are set in Vector from mllib
    * @param coordinates RDD with the values of each point and its id. The format is (Int, Vector)
    * @param distances RDD with the distances between all points. Its values are Distances class instances
    * @param numPoints The total number of points to the data
    * @param clusterFilterNumber A Int number to filter the minimum number of points in each centroids
    * @param strategyDistance Strategy to run Linkage algorithm in String (min, max or avg)
    * @param minClusters Set the minimum number of clusters
    * @param maxClusters Set the maximum number of clusters
    * @return A RDD composed by each iteration and the Silhouette, Dunn, Davies-Bouldin and WSSSE and its corresponding time
    * @example getIndicesLinkage(parsedData, coordinates, distances, 150, 1, "avg", 2, 10)
    */
  def getIndicesLinkage(parsedData: RDD[org.apache.spark.mllib.linalg.Vector], coordinates: RDD[(Int,Vector)], distances: RDD[Distance],
                        numPoints: Int, clusterFilterNumber: Int, strategyDistance: String, minClusters: Int, maxClusters: Int):
                        RDD[(Int, (Double, Double, Double, Double, Long, Long, Long, Long))] = {
    var i = 1
    var s = ""
    val sc = parsedData.sparkContext

    //Create a Map to save the result model
    var modelResult = scala.collection.mutable.Map[Int, (Double, Double, Double, Double, Long, Long, Long, Long)]()

    //Set up the checkpoint directory
    sc.setCheckpointDir("checkpoints")

    //Initialize the number of clusters with the minimum number of clusters
    var numberClusters = minClusters

    println("Number of points: " + numPoints)

    //Create a Linkage object with the number of clusters and the strategy distance choose (min,max,avg)
    val linkage = new Linkage(numberClusters, strategyDistance)
    println("New Linkage with strategy: " + strategyDistance)

    //Run the Linkage algorithm and create the clusters variable
    var clusters = linkage.runAlgorithm(distances, numPoints)

    //Initialize an RDD from 1 to the number of points in our database
    val totalPoints = sc.parallelize(1 to numPoints).cache()

    for (k <- minClusters to maxClusters) {

      val start = System.nanoTime

      //For each iteration the number of clusters is equal to the K iteration
      val numClusters = k

      //In function of the number of clusters the centroids will chance
      val resultPoints = clusters.createClusters(numPoints, numClusters, totalPoints)
      val centroids = clusters.inicializeCenters(coordinates, numClusters, numPoints, clusterFilterNumber, resultPoints)
      clusters.setClusterCenters(centroids)

      //Global Center
      val centroides = sc.parallelize(clusters.clusterCenters)
      val centroidesCartesian = centroides.cartesian(centroides).filter(x => x._1 != x._2).cache()

      var startTimeK = System.currentTimeMillis

      val intraMean = clusters.computeCost(parsedData) / parsedData.count()
      val interMeanAux = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).reduce(_ + _)
      val interMean = interMeanAux / centroidesCartesian.count()

      //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
      val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
      s += i + ";" + silhoutte + ";"

      var stopTimeK = System.currentTimeMillis
      val elapsedTimeSil = (stopTimeK - startTimeK)

      //DUNN
      startTimeK = System.currentTimeMillis

      //Min distance between centroids
      val minA = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).min()

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
        .groupByKey
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

      println("*** K = " + k + " ***")
      println("Executing Indices")
      println("VALUES:")
      println("\tSilhouette: " + silhoutte)
      println("\tDunn: " + dunn)
      println("\tDavies-Bouldin: " + bouldin)
      println("\tWSSSE: " + wssse)
      println("Elapsed Time:")
      println("\tTime Silhouette: " + elapsedTimeSil)
      println("\tTime Dunn: " + elapsedTime)
      println("\tTime Davies-Bouldin: " + elapsedTimeDavies)
      println("\tTime WSSSE: " + elapsedTimeW)
      println("\n")

      //Show the duration to run the algorithm with eack K
      val duration = (System.nanoTime - start) / 1e9d
      logInfo(s"Time for iteration $k: " + duration)

      //Save in the model the results of each iteration
      modelResult += k -> (silhoutte, dunn, bouldin, wssse, elapsedTimeSil, elapsedTime, elapsedTimeDavies, elapsedTimeW)

    }

    sc.parallelize(modelResult.toSeq)
  }

}

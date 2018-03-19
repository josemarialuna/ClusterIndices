package es.us.linkage

import es.us.cluster.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author José María Luna and José David Martín
  * @version 1.0
  * @since v1.0 Dev
  */

class LinkageModel(_clusters: RDD[(Long, (Int, Int))], var _clusterCenters: Array[Vector]) extends Serializable with Logging {

  def clusters = _clusters

  def clusterCenters = _clusterCenters

  def setClusterCenters(centroids: Array[Vector]) = {
    _clusterCenters = centroids
  }

  def isCluster(point: Int): Boolean = {
    clusters.countByKey().contains(point.toLong)
  }

  def isCluster(point: Int, totalPoints: Int): Boolean = {
    point > totalPoints
  }

  /**
    * Return all points of a cluster
    * @param point    A cluster point
    * @return A List composed by the points of the cluster
    * @example giveMePoints(151)
    */
  def giveMePoints(point: Int): List[Int] = {
    var res = List[Int]()
    val aux = clusters.lookup(point.toLong).head // valor de una Key(point)
    if (isCluster(aux._1)) {
      res = res ::: giveMePoints(aux._1)
      if (isCluster(aux._2)) {
        res = res ::: giveMePoints(aux._2)
      } else {
        res = res ::: List(aux._2)
      }
    } else {
      if (isCluster(aux._2)) {
        res = res ::: giveMePoints(aux._2)
        res = res ::: List(aux._1)
      } else {
        res = res ::: List(aux._1, aux._2)
      }
    }

    res
  }

  /**
    * Return all points of a cluster
    * @param point    A cluster point
    * @param numberPoints The number of points to the dataset
    * @return A array composed by the points of the cluster
    * @example giveMePoints(151, 150)
    */
  def giveMePoints(point: Int, numberPoints: Int): Array[(Int,Int)] = {
    var rest = new Array[(Int,Int)](numberPoints*2)
    val aux = clusters.lookup(point.toLong).head
    val cont = clusters.sparkContext.longAccumulator("Accumulator Points")
    cont.add(0)
    if (isCluster(aux._1)) {
      rest :+ giveMePoints(aux._1,numberPoints)
      if (isCluster(aux._2)) {
        rest :+ giveMePoints(aux._2,numberPoints)
      } else {
        rest(cont.value.toInt) = (aux._2,point)
      }
    } else {
      if (isCluster(aux._2)) {
        rest :+ giveMePoints(aux._2,numberPoints)
        rest(cont.value.toInt) = (aux._1,point)
      } else {
        rest(cont.value.toInt) = (aux._1,point)
        cont.add(1)
        rest(cont.value.toInt) = (aux._2,point)
        cont.add(1)
      }
    }

    rest
  }

  /**
    * Return all points of a cluster
    * @param cluster    A cluster point
    * @param numberPoints The number of points to the dataset
    * @return A RDD composed by the points of the cluster
    * @example giveMePointsRDD(151, 150)
    */
  def giveMePointsRDD(cluster: Int, numberPoints: Int): RDD[(Int,Int)] = {

    val aux = clusters.lookup(cluster.toLong).head
    var rest = clusters.sparkContext.emptyRDD[(Int,Int)]

    if(isCluster(aux._1,numberPoints)){
      rest = rest.union(giveMePointsRDD(aux._1,numberPoints))
      if(isCluster(aux._2,numberPoints)){
        rest = rest.union(giveMePointsRDD(aux._2,numberPoints))
      }else {
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._2,cluster))))
      }
    } else {
      if(isCluster(aux._2,numberPoints)){
        rest = rest.union(giveMePointsRDD(aux._2,numberPoints))
      }else {
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._1,cluster))))
        rest = rest.union(clusters.sparkContext.parallelize(Seq((aux._2,cluster))))
      }
    }

    rest.sortByKey().filter(value => value._1 > 0).map(x => (x._1,cluster))

  }

  /**
    * Return a cluster given a point
    * @param point    A point of the dataset
    * @param numberPoints The number of points to the dataset
    * @param clusterBase A RDD with all points of the dataset and its cluster
    * @return A cluster
    * @example giveMeCluster(151, 150, clusterBase)
    */
  def giveMeCluster(point: Int, numberPoints: Int, clusterBase: RDD[(Int, Int)]): Int = {
    var rest = point
    if (clusterBase.count() != 0) {
      var pointResult = clusterBase.filter(x => x._1 >= point).map {
        case (x, y) =>
          var auxPoint = point
          if (!isCluster(point, numberPoints)) {
            if (x == point) {
              auxPoint = y
            }
          } else if (x == point) {
            auxPoint = y
          }
          auxPoint
      }.distinct().max()

      if (isCluster(pointResult, numberPoints) && pointResult != point) {
        pointResult = giveMeCluster(pointResult, numberPoints, clusterBase.filter(x => x._1 >= pointResult))
      }

      rest = pointResult
    }

    rest
  }

  def printSchema(separator: String): Unit = {
    println(clusters
      .sortBy(_._1)
      .map(x => s"${
        x._1
      },${
        x._2._1
      },${
        x._2._2
      }")
      .collect()
      .mkString(separator))
  }

  /**
    * Save the model schema in a external file
    * @param destination Path to save the file
    * @return Nothing
    * @example saveSchema("Test")
    */
  def saveSchema(destination: String) = {
    clusters
      .sortBy(_._1)
      .map(x => s"${
        x._1
      },${
        x._2._1
      },${
        x._2._2
      }")
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destination + "Linkage-" + Utils.whatTimeIsIt())
  }

  /**
    * Save in a external file all points and its cluster number
    * @param destination    Path to save the file
    * @param resultPoints A RDD with all points and its cluster number
    * @param numPoints The number of points on the dataset
    * @param numCluster The number of the clusters
    * @return Nothing
    * @example saveResult("Test", resultPoints, 150, 3)
    */
  def saveResult(destination: String, resultPoints: RDD[(Int,Int)], numPoints: Int, numCluster: Int) = {
    resultPoints
      .sortByKey()
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(destination + "Points-" + numPoints + "-Clusters-" + numCluster)
  }

  /**
    * Create a RDD with all points and its cluster number
    * @param numPoints    The number of points on the dataset
    * @param numCluster   The number of the clusters
    * @param totalPoints A RDD with all points on the dataset
    * @return A RDD with point and cluster in each row
    * @example createClusters(150, 3, totalPoints)
    */
  def createClusters(numPoints: Int, numCluster: Int, totalPoints: RDD[Int]): RDD[(Int, Int)] = {

    //We filter the total of clusters establishing a lower and upper limit depending on the number of points and the level at which we want to stop
    val minCluster = numPoints + 1
    val topCluster = numPoints + numPoints

    val clustersFiltered = clusters.filterByRange(minCluster, topCluster - numCluster).sortByKey().cache()

    //We generate an auxiliary RDD to start each cluster at each point
    var auxPoints = totalPoints.map(value => (value,value))
    var a = 0

    //We go through each row of the filtered cluster file
    for (iter <- clustersFiltered.collect()){

      //We save the elements of each row in auxiliary variables to be able to filter later
      val point1 = iter._2._1
      val point2 = iter._2._2
      val cluster = iter._1.toInt

      //We go through the auxiliary RDD and check if in this iteration it is necessary to change the cluster to which each point belongs
      auxPoints = auxPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, cluster)
        }
        auxValue
      }

      a = a + 1

      //Every two hundred iterations we make a checkpoint so that the memory does not overflow
      if(a % 200 == 0){
        auxPoints.checkpoint()
        auxPoints.count()
      }
    }

    //Return the result of clustering
    auxPoints
  }

  /**
    * Create a Array with the centroids of the model in Vector format
    * @param coordinates RDD with the values of each point and its id. The format is (Int, Vector)
    * @param numClusters The number of clusters
    * @param numPoints    The number of points on the dataset
    * @param kMin Filter to the minimum number of points to each centroid
    * @param resultPoints RDD with all points and its cluster number
    * @return A Array with the centroids at the model
    * @example inicializeCenters(coordinates, 3, 150, 1, resultPoints)
    */
  def inicializeCenters(coordinates: RDD[(Int, Vector)], numClusters: Int, numPoints: Int, kMin: Int, resultPoints: RDD[(Int, Int)]): Array[Vector] = {
    val start = System.nanoTime

    val sc = resultPoints.sparkContext

    //Create one Spark Session to use DataFrames into the algorithm
    val spark = SparkSession.builder()
      .appName("Spark Cluster")
      .master("local[*]")
      .getOrCreate()

    //Import a auxiliary library to convert RDD into DataFrame and vice versa
    import spark.implicits._

    var rest = Array[Vector]()
    var a = 0

    //Through each different value within the resulting clusters of the model
    for (iter <- resultPoints.toDF().map(row => row.getInt(1)).distinct().collect()){

      var auxVectors = sc.emptyRDD[Vector]

      //Save the points of each cluster in a auxiliary variable
      val points = resultPoints.filter(id => id._2 == iter).map(value => value._1).toDF().cache()

      //Filter the number minimum of points in each centroid
      if (points.count() >= kMin){

        for(point <- points.collect()){
          a = a + 1

          //Every four hundred iterations we make a checkpoint so that the memory does not overflow
          if(a % 400 == 0){
            auxVectors.checkpoint()
            auxVectors.count()
          }

          auxVectors = auxVectors.union(coordinates.filter(id => id._1 == point.getInt(0)).map(value => value._2))
        }

        points.unpersist()

      }

      //Save the mean coordinates to the all points for each centroid
      val summary: MultivariateStatisticalSummary = Statistics.colStats(auxVectors)
      rest = rest :+ summary.mean

    }

    //Show the duration to create the centroids
    val duration = (System.nanoTime - start) / 1e9d
    logInfo("Time for create centroids: " + duration)

    rest

  }

  /**
    *  Return the Linkage cost (sum of squared distances of points to their nearest center) for this model on the given data
    * @param points   RDD with the coordenates all points in the dataset
    * @return Double cost for this model on the given data
    * @example computeCost(points)
    */
  def computeCost(points: RDD[Vector]): Double = {

    //For each point calculate the cost to its centroid
     points.map(point => pointCost(point)).sum()

  }

  /**
    *  Return the cost (sum of squared distances of points to their nearest center) for this model on the given point
    * @param vector   Coordinates to the point in a Vector format
    * @return Double cost for this model on the given point
    * @example pointCost(vector)
    */
  def pointCost(vector: Vector): Double = {

    var costVector = 0.0

    for (center <- clusterCenters){
      //Only calculated the cost if the point it's near to the centroid iteration
      if (predict(vector) == clusterCenters.indexOf(center)) {
       costVector = Vectors.sqdist(vector, center)
      }
    }

    costVector
  }

  /**
    *  Returns the cluster index that a given point belongs to
    * @param point   Coordinates to the point in a Vector format
    * @return Int the cluster index that a given point belongs to
    * @example predict(point)
    */
  def predict(point: Vector): Int = {

    var dist = -1.0
    var distAux = 0.0
    var centerPoint = point

    for (center <- clusterCenters) {

      //Calculate the distance between the point and the centroid iteration
      distAux = Vectors.sqdist(point, center)

      //If the distAux it's less than dist, the new dist is a old distAux and the centroid more near it's this iteration
      if (dist == -1 || dist > distAux){
        dist = distAux
        centerPoint = center
      }

    }

    clusterCenters.indexOf(centerPoint)

  }

}

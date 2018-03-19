package es.us.linkage

import es.us.cluster.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.min
import org.apache.spark.storage.StorageLevel

/**
  * @author José María Luna and José David Martín
  * @version 1.0
  * @since v1.0 Dev
  */

class Linkage(
               private var numClusters: Int,
               private var distanceStrategy: String) extends Serializable with Logging {

  def getNumClusters: Int = numClusters

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getDistanceStrategy: String = distanceStrategy

  def setDistanceStrategy(distanceStrategy: String): this.type = {
    this.distanceStrategy = distanceStrategy
    this
  }

  //Sort by dist
  object DistOrdering extends Ordering[Distance] {
    def compare(a: Distance, b: Distance) = a.getDist compare b.getDist
  }

  /**
    * Return the Linkage model to given distance data
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @return A Linkage model to given data
    * @example runAlgorithm(distanceMatrix, 150)
    */
  def runAlgorithm(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

    val start = System.nanoTime

    //Save in a variable the matrix of distances, the number of partitions and the sparkContext for future uses
    var matrix = distanceMatrix
    val partitionNumber = distanceMatrix.getNumPartitions
    val sc = distanceMatrix.sparkContext

    //Initialize the counter taking into account the maximum value of existing points
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)

    //Create a Map to save the cluster and points obtained in each iteration
    val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()

    for (a <- 0 until (numPoints - numClusters)) {

      val clustersRes = matrix.min()(DistOrdering)

      //Save in variables the cluster and points we find in this iteration and we add them to the model
      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      cont.add(1)
      val newIndex = cont.value.toLong

      //The new cluster is saved in the result model
      linkageModel += newIndex -> (point1, point2)

      //If it isn´t the last cluster
      if (a < (numPoints - numClusters - 1)) {

        //The point found is deleted
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Search all the distances that contain the first coordinate of the new cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Search all the distances that contain the second coordinate of the new cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminate the whole points
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        //A new RDD is generated with the points filtered by the remaining points in order to calculate the distance to each one in the next step
        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //A new point is created following the strategy
        matrix = distanceStrategy match {

          case "min" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "max" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "avg" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            //Add the points with the new index
            matrixSub.union(newPoints)
        }
      }

      //The distance matrix is ​​persisted to improve the performance of the algorithm
      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)

      //Every 5 iterations a checkpoint of the distance matrix is ​​done to improve the performance of the algorithm
      if (a % 5 == 0) {
        matrix.checkpoint()
      }

    }

    matrix.unpersist()

    //Show the duration to run the algorithm
    val duration = (System.nanoTime - start) / 1e9d
    logInfo("Time for linkage clustering: " + duration)

    //Return a new LinkageModel based into the model
    (new LinkageModel(sc.parallelize(linkageModel.toSeq), sc.emptyRDD[Vector].collect()))

  }

  /**
    * Return the Linkage model to given distance data and all points with its cluster number
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @return A Linkage model to given data and RDD with (Int, Int) [point and cluster number]
    * @example runAlgorithmWithResult(distanceMatrix, 150)
    */
  def runAlgorithmWithResult(distanceMatrix: RDD[Distance], numPoints: Int): (LinkageModel, RDD[(Int,Int)]) = {

    //Save in a variable the matrix of distances, the number of partitions and the sparkContext for future uses
    var matrix = distanceMatrix
    val partitionNumber = distanceMatrix.getNumPartitions
    val sc = distanceMatrix.sparkContext

    //Initialize the counter taking into account the maximum value of existing points
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)

    //Create a Map to save the cluster and points obtained in each iteration
    val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()

    //Create an initialized RDD with all the points having as a cluster itself
    val auxPoints = sc.parallelize(1 to numPoints)
    var totalPoints = auxPoints.map(value => (value,value)).cache()

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val clustersRes = matrix.min()(DistOrdering)

      println(s"New minimum: $clustersRes")

      //Save in variables the cluster and points we find in this iteration and we add them to the model
      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      cont.add(1)
      val newIndex = cont.value.toLong

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      //The new cluster is saved in the result model
      linkageModel += newIndex -> (point1, point2)

      //Update the RDD that shows in which cluster each point is in each iteration
      totalPoints = totalPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, newIndex.toInt)
        }
        auxValue
      }

      //If it isn´t the last cluster
      if (a < (numPoints - numClusters - 1)) {

        //The point found is deleted
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Search all the distances that contain the first coordinate of the new cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Search all the distances that contain the second coordinate of the new cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminate the whole points
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        //A new RDD is generated with the points filtered by the remaining points in order to calculate the distance to each one in the next step
        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //A new point is created following the strategy
        matrix = distanceStrategy match {

          case "min" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "max" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "avg" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            //Add the points with the new index
            matrixSub.union(newPoints)
        }
      }

      //The distance matrix is ​​persisted to improve the performance of the algorithm
      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)

      //Every 5 iterations a checkpoint of the distance matrix is ​​done to improve the performance of the algorithm
      if (a % 5 == 0) {
        matrix.checkpoint()
      }

      //Every 200 iterations the RDD of points and cluster is saved in a file and then that same file is read to improve the performance of the algorithm
      if (a % 200 == 0){
        totalPoints.map(_.toString().replace("(", "").replace(")", "")).coalesce(1).saveAsTextFile("B:\\Datasets\\saves" + "Clustering-" + a)
        totalPoints = sc.textFile("B:\\Datasets\\saves" + "Clustering-" + a).map(s => s.split(',').map(_.toInt))
          .map { case x =>
            (x(0), x(1))
          }
      }

      //Show the duration of each iteration
      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }

    //Return a new LinkageModel based into the model and the RDD with point and cluster final
    (new LinkageModel(sc.parallelize(linkageModel.toSeq), sc.emptyRDD[Vector].collect()), totalPoints)

  }

  /**
    * Save the result of Linkage algorithm into a external file with the dendogram format to representation with Python library
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @param numClusters The number of clusters
    * @return Nothing
    * @example runAlgorithmDendrogram(distanceMatrix, 150, 5)
    */
  def runAlgorithmDendrogram(distanceMatrix: RDD[Distance], numPoints: Int, numClusters: Int) = {

    //Save in a variable the matrix of distances, the number of partitions and the sparkContext for future uses
    var matrix = distanceMatrix
    val partitionNumber = distanceMatrix.getNumPartitions
    val sc = distanceMatrix.sparkContext

    //Initialize the counter taking into account the maximum value of existing points
    val cont = sc.longAccumulator("My Accumulator")
    cont.add(numPoints)

    //Create an initialized RDD with all the points having as a cluster itself
    val auxPoints = sc.parallelize(1 to numPoints)
    var totalPoints = auxPoints.map(value => (value,value)).cache()

    //Create a empty RDD to save the model for print a Dendogram
    var model = sc.emptyRDD[(Double,Double,Double,Double)]

    for (a <- 0 until (numPoints - numClusters)) {
      val start = System.nanoTime

      println("Finding minimum:")
      val clustersRes = matrix.min()(DistOrdering)

      println(s"New minimum: $clustersRes")

      //Save in variables the cluster and points we find in this iteration and we add them to the model
      val point1 = clustersRes.getIdW1
      val point2 = clustersRes.getIdW2
      val dist = clustersRes.getDist
      cont.add(1)
      val newIndex = cont.value.toInt

      println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

      //Update the RDD that shows in which cluster each point is in each iteration
      totalPoints = totalPoints.map {value =>
        var auxValue = value
        if(value._2 == point1 || value._2 == point2){
          auxValue = (value._1, newIndex.toInt)
        }
        auxValue
      }.cache()

      //Count the number of points from the new cluster
      val contPoints = totalPoints.filter(id => id._2 == newIndex).count()

      //Update the model with the schema
      model = model.union(sc.parallelize(Seq((point1.toDouble,point2.toDouble,dist.toDouble,
        contPoints.toDouble))))

      //If it isn´t the last cluster
      if (a < (numPoints - numClusters - 1)) {

        //The point found is deleted
        matrix = matrix.filter(x => !(x.getIdW1 == point1 && x.getIdW2 == point2))

        //Search all the distances that contain the first coordinate of the new cluster
        val rddPoints1 = matrix.filter(x => x.getIdW1 == point1 || x.getIdW2 == point1).collect()

        //Search all the distances that contain the second coordinate of the new cluster
        val rddPoints2 = matrix.filter(x => x.getIdW1 == point2 || x.getIdW2 == point2).collect()

        //Eliminate the whole points
        val matrixSub = matrix.filter(x => !(x.getIdW1 == point1 || x.getIdW2 == point1))
          .filter(x => !(x.getIdW1 == point2 || x.getIdW2 == point2))

        //A new RDD is generated with the points filtered by the remaining points in order to calculate the distance to each one in the next step
        val rddCartesianPoints = sc.parallelize(rddPoints1).cartesian(sc.parallelize(rddPoints2))
        val rddFilteredPoints = rddCartesianPoints.filter(x => (x._1.getIdW2 == x._2.getIdW2) ||
          (x._1.getIdW1 == x._2.getIdW1) ||
          (x._1.getIdW1 == x._2.getIdW2 ||
            (x._2.getIdW1 == x._1.getIdW2)))

        //A new point is created following the strategy
        matrix = distanceStrategy match {

          case "min" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.min(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "max" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              math.max(x._1.getDist, x._2.getDist)))

            //Add the points with the new index
            matrixSub.union(newPoints)

          case "avg" =>
            //The new distance is calculated with respect to all points and the chosen strategy
            val newPoints = rddFilteredPoints.map(x => new Distance(newIndex.toInt, filterMatrix(x._1, clustersRes),
              ((x._1.getDist + x._2.getDist) / 2)))

            //Add the points with the new index
            matrixSub.union(newPoints)
        }
      }

      //The distance matrix is ​​persisted to improve the performance of the algorithm
      matrix = matrix.coalesce(partitionNumber / 2).persist(StorageLevel.MEMORY_ONLY_2)
      totalPoints = totalPoints.coalesce(partitionNumber / 2).persist(StorageLevel.DISK_ONLY_2)
      model = model.coalesce(partitionNumber / 2).persist(StorageLevel.DISK_ONLY_2)

      //Every 5 iterations a checkpoint of the distance matrix is ​​done to improve the performance of the algorithm
      if (a % 5 == 0) {
        matrix.checkpoint()
      }

      if (a % 200 == 0) {
        model.checkpoint()
        totalPoints.checkpoint()
      }

      //Show the duration of each iteration
      val duration = (System.nanoTime - start) / 1e9d
      println(s"TIME: $duration")
    }

    //Save in a external file the result of the model
    model
      .map(value => (value._1 - 1, value._2 - 1, value._3, value._4))
      .map(_.toString().replace("(", "").replace(")", ""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile("Points-" + numPoints + "-Clusters-" + numClusters + Utils.whatTimeIsIt())

  }

  /**
    * Return the Linkage model to given distance data but using DataFrames
    * @param distanceMatrix    RDD to the distances between all points to given data
    * @param numPoints The number of points into given data
    * @param numPartitions The number of partitions
    * @return A Linkage model to given data
    * @example runAlgorithmDF(distanceMatrix, 150, 16)
    */
  def runAlgorithmDF(distanceMatrix: DataFrame, numPoints: Int, numPartitions: Int): LinkageModel ={

      //Save in a variable the matrix of distances and the sparkSession for future uses
      var matrix = distanceMatrix
      val spark = distanceMatrix.sparkSession
      import spark.implicits._

      //Initialize the counter taking into account the maximum value of existing points
      val cont = spark.sparkContext.longAccumulator("My Accumulator DF")
      cont.add(numPoints)

      //Create a Map to save the cluster and points obtained in each iteration
      val linkageModel = scala.collection.mutable.Map[Long, (Int, Int)]()

      for (a <- 0 until (numPoints - numClusters)) {
        val start = System.nanoTime

        println("Finding minimum:")

        val minDistRes = matrix.select(min("dist")).first().getFloat(0)
        val clusterRes = matrix.where($"dist" === minDistRes)

        println(s"New minimum:")
        clusterRes.show(1)

        //Save in variables the cluster and points we find in this iteration and we add them to the model
        val point1 = clusterRes.first().getInt(0)
        val point2 = clusterRes.first().getInt(1)
        cont.add(1)
        val newIndex = cont.value.toLong

        println("New Cluster: " + newIndex + ":" + point1 + "-" + point2)

        //The result is saved in the model
        linkageModel += newIndex -> (point1, point2)

        //If it is not the last cluster
        if (a < (numPoints - numClusters - 1)) {

          //The found point of the original matrix is ​​removed
          matrix = matrix.where("!(idW1 == " + point1 +" and idW2 ==" + point2 + " )").cache()

          //Search all the distances that contain the first coordinate of the new cluster
          val dfPoints1 = matrix.where("idW1 == " + point1 + " or idW2 == " + point1)

          //Rename the columns to make a subsequent filtering
          val newColumnsNames = Seq("distPoints1", "idW1Points1", "idW2Points1")
          val dfPoints1Renamed = dfPoints1.toDF(newColumnsNames: _*)

          //Search all the distances that contain the second coordinate of the new cluster
          val dfPoints2 = matrix.where("idW1 == " + point2 + " or idW2 == " + point2)

          //A new DataFrame is generated with the points filtered by the remaining points in order to calculate the distance to each one in the next step
          val dfCartesianPoints = dfPoints1Renamed.crossJoin(dfPoints2)
          val dfFilteredPoints = dfCartesianPoints.filter("(idW1Points1 == idW1) or (idW1Points1 == idW2) " +
            "or (idW2Points1 == idW1) or (idW2Points1 == idW2)")

          //Eliminate the complete points
          val matrixSub = matrix.where("!(idW1 == " + point1 + " or idW2 == " + point1 + ")")
                          .where("!(idW1 == " + point2 + " or idW2 == " + point2 + ")")

          //A new point is created following the strategy
          matrix = distanceStrategy match {
            case "min" =>
              val newPoints = dfFilteredPoints.map(r =>
                (newIndex.toInt, filterDF(r.getInt(0),r.getInt(1), point1, point2), math.min(r.getFloat(2),r.getFloat(5))))

              //Add the points with the new index
              val rows = newPoints.toDF().select("_1","_2","_3")
              matrixSub.union(rows)
          }
          matrix.cache()
        }

        //Every 5 iterations a checkpoint of the distance matrix is ​​done to improve the performance of the algorithm
        if (a % 5 == 0) {
          matrix.checkpoint()
        }

        //Show the duration of each iteration
        val duration = (System.nanoTime - start) / 1e9d
        println(s"TIME: $duration")
      }

      //Return a new LinkageModel based into the model
    (new LinkageModel(spark.sparkContext.parallelize(linkageModel.toSeq), spark.sparkContext.emptyRDD[Vector].collect()))
    }

  /**
    * Return a Int that represent a point of the model
    * @param oldDistance    Original Distance
    * @param clusterReference A Distance with the cluster
    * @return A Int that represent the next value for the Linkage model. Return the first o the second point that constitution the cluster
    * @example filterMatrix(oldDistnace, clusterReference)
    */
  def filterMatrix(oldDistance: Distance, clusterReference: Distance): Int = {
    var result = 0

    if (oldDistance.getIdW1 == clusterReference.getIdW1 || oldDistance.getIdW1 == clusterReference.getIdW2) {
      result = oldDistance.getIdW2
    } else if (oldDistance.getIdW2 == clusterReference.getIdW1 || oldDistance.getIdW2 == clusterReference.getIdW2) {
      result = oldDistance.getIdW1
    }

    result
  }

  /**
    * Return a Int that represent a point of the model
    * @param idW1    First coordinate
    * @param idW2 Second coordinate
    * @param pointReference1 First coordinate of the cluster
    * @param pointReference2 Second coordinate of the cluster
    * @return A Int that represent the next value for the Linkage model. Return the first o the second point that constitution the cluster
    * @example filterDF(1, 2, 10, 11)
    */
  def filterDF(idW1: Int, idW2: Int, pointReference1: Int, pointReference2: Int): Int = {
    var result = idW1

    if (idW1 == pointReference1 || idW1 == pointReference2) {
      result = idW2
    }

    result
  }
}

object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[(Int, Int), Float],
                       strategy: String): Double = {
    var res = 0.0
    var aux = res

    strategy match {
      case "min" =>
        res = 100.0 //QUESTION: No se podría poner otro valor ?

        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            }
            else {
              aux = distanceMatrix(y, x)
            }
            if (aux < res)
              res = aux

          }
        }


      case "max" =>
        res = 0.0
        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            } else {
              aux = distanceMatrix(y, x)
            }
            if (aux > res)
              res = aux
          }
        }

      case "avg" =>


    }

    res

  }


  //Calculate the distance between two vectors
  //DEPRECATED
  private def calculateDistance(
                                 v1: Vector,
                                 v2: Vector,
                                 strategy: String): Double = {
    var totalDist = 0.0
    for (z <- 1 to v1.size) {
      var minAux = 0.0
      try {
        val line = v1.apply(z)
        val linePlus = v2.apply(z)
        //El mínimo se suma a totalDist
        if (line < linePlus) {
          minAux = line
        } else {
          minAux = linePlus
        }
      } catch {
        case e: Exception => null
      } finally {
        totalDist += minAux
      }

    }
    totalDist

  }
}
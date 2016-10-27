package es.us.cluster


/**
  * This object contains two methods that calculates the optimal number for
  * clustering using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object ClusterIndex {
/*
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
  def silhoutteKMeans(sc: SparkContext, parsedData: DataFrame, maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""

    //evaluates number of clusters
    for (i <- 2 to maxNumClusters) {

      // Trains a k-means model.
      val kmeans = new KMeans()
        .setK(i)
        .setSeed(1L)
        .setMaxIter(numIterations)
      val model = kmeans.fit(parsedData)

      // Evaluate clustering by computing Within Set Sum of Squared Errors.
      val intraMean = model.computeCost(parsedData) / parsedData.count()

      //Global Center
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val centroides = sc.parallelize(model.clusterCenters).map(_.toArray).map(x => (x.apply(0), x.apply(1))).toDF()

      val clusterCentroides = new KMeans()
        .setK(1)
        .setSeed(1L)
        .setMaxIter(numIterations)
        .fit(centroides)
      val interMean = clusterCentroides.computeCost(centroides) / centroides.count()

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
  def dunnKMeans(sc: SparkContext, parsedData: DataFrame, maxNumClusters: Int, numIterations: Int): Unit = {
    var i = 1
    var s = ""
    //evaluates number of clusters
    //Results: 6 clusters is the first minimum
    for (i <- 2 to maxNumClusters) {

      // Trains a k-means model.
      val clusters = new KMeans()
        .setK(i)
        .setSeed(1L)
        .setMaxIter(numIterations)
        .fit(parsedData)

      //Global Centroid
      val centroides = sc.parallelize(clusters.clusterCenters)
      val clusterCentroides = new KMeans()
        .setK(1)
        .setSeed(1L)
        .setMaxIter(numIterations)
        .fit(parsedData)

      //Min distance from centroids to global centroid
      val minA = centroides.map { x =>
        Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
      }.min()

      //Max distance from points to its centroid
      val maxB = parsedData.map { x =>
        Vectors.sqdist(x, clusters.clusterCenters(clusters.pred.predict(x)))
      }.max

      //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
      val dunn = minA / maxB
      s += i + ";" + minA + " ; " + maxB + " ; " + dunn + "\n"
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

*/
}

# Validity Indices for Clustering Techniques in Big Data

This package contains the code for executing four clustering validity indices in Spark. The package includes BD-Silhouette BD-Dunn that were proposed in [1]. Davies-Bouldin and WSSSE indices are also calculated in the same method. The cluster indices can be executed using K-means and Bisecting K-Means from Spark MLlib, and Linkage method.

Please, cite as: Luna-Romera, J.M., García-Gutiérrez, J., Martínez-Ballesteros, M. et al. Prog Artif Intell (2017). https://doi.org/10.1007/s13748-017-0135-3 (https://link.springer.com/article/10.1007%2Fs13748-017-0135-3)

## Getting Started
The package includes the following Scala files in two packages:
* *es.us.spark.mllib.clustering.indices*: this package contains the main classes to launch the clustering validity indices.
  * ClusterIndex: Scala Object that contains the methods that return the values of the indices.
  * MainTestKMeans: Scala main class ready to test the validity indices using [KMeans method from Mllib](https://spark.apache.org/docs/1.6.2/mllib-clustering.html#k-means).
  * MainTestBKM: Scala main class ready to test the validity indices using [Bisecting KMeans method from Mllib](https://spark.apache.org/docs/1.6.2/mllib-clustering.html#bisecting-k-means).
  * MainTestLinkage: Scala main class ready to test the validity indices using Linkage Hierarchical Clustering.
* *es.us.spark.linkage*: Package that contains the Linkage Method.  
* Utils: Scala object that includes some helpful methods.
* C5-D20-I1000.csv: Example dataset that contains 5 clusters with 1000 points each and 20 features (columns).

### Prerequisites

The package is ready to be used. You only have to download it and import it into your workspace. The main files include an example dataset that could be used with the main classes.

## Running the tests

The testing classes have been configured for being executed in a laptop. There are critical variables that must be configured before the execution:
* path: The complete path where the dataset is located.
* fileName: The name of the dataset, including the extension.
* minNumCluster: The initial number of clusters to test.
* maxNumCluster: The last number of clusters to test.
* numIterations: The number of iterations of the clustering method.
* numPartitions: The number of partitions for the dataset. 3 x number of cores (Recommended).


```
val conf = new SparkConf()
      .setAppName("Spark Cluster")
      .setMaster("local[*]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    var path = ""
    var fileName = "C5-D20-I1000.csv"

    var origen: String = path + fileName
    var destino: String = path + fileName
    var minNumCluster = 2
    var maxNumCluster = 10
    var numIterations = 100
    var numPartitions = 16
```


## Results

By default, results are saved in the same a folder than the dataset. Results are in a folder named "DatasetName-Results-DATE" that contains a part-00000 file. The result follow the next scheme: k, Silhouette value, Dunn value, Davies-Bouldin value, WSSSE value, and the elapsed time of each index in miliseconds. 

In the case of our example dataset, a resultset could be:
```
2	0.6471984147958033	1.4964051343742035	0.6763338700755929	2836.5005986600886	196	157	1320	37
3	0.8173370699126433	2.399960823246771	0.4211436895566971	1755.6134378311212	122	49	1151	19
4	0.9011755677244645	2.247008539431008	0.2411105359423219	1001.6859042415279	88	61	1509	96
5	0.9004432069362583	0.012259186190588283	4.77638577359007	1000.1940093333519	141	51	1130	26
6	0.9754617811192272	0.06555472765131234	4.0135282309791585	246.33957814603738	69	28	1611	17
7	0.9756903365854314	0.06648407621169158	6.719173651809519	244.2847443226342	46	27	1185	25
8	0.9762670314987698	0.060601075379292504	6.6212375576322655	242.99914863497614	44	57	1020	14
9	0.9765291299881667	0.06134099486219169	8.650483438885638	241.06630939753563	59	30	1217	17
10	0.9765599524466926	0.06717560818211275	7.8725321621080315	239.42370066181508	80	42	1428	18
```

This data could be copy-pasted directly into an spreadsheet to be visualized.

For the example dataset:
![Results in an Excel Spreadsheet](https://github.com/josemarialuna/ClusterIndices/blob/master/result_data.PNG)

![Graphs results for example dataset](https://github.com/josemarialuna/ClusterIndices/blob/master/result_graph.PNG)

## Contributors

* José María Luna-Romera - (main contributor and maintainer).
* Jorge García-Gutiérrez
* María Martínez-Ballesteros
* José C. Riquelme Santos

## References

[1] Luna-Romera, J.M., García-Gutiérrez, J., Martínez-Ballesteros, M. et al. Prog Artif Intell (2017). https://doi.org/10.1007/s13748-017-0135-3


package es.us.linkage

/**
  * Created by Josem on 15/11/2016.
  */
class Cluster(private var coordinates: List[Int]) extends Serializable {

  def getCoordinates: List[Int] = coordinates

  def setCoordinates(coordinates: List[Int]): this.type = {
    this.coordinates = coordinates
    this
  }

  def mixCluster(otherCluster: Cluster): Cluster = {
    return new Cluster(this.getCoordinates ::: otherCluster.getCoordinates)
  }

  override def toString = s"Cluster($getCoordinates)"
}

object Cluster {

  //Create initial clusters from a given number
  def createInitClusters(numPoints: Int): Seq[Cluster] = {
    var res = Seq.empty[Cluster]

    for (i <- 0 until numPoints) {
      res = res :+ new Cluster(List(i))
    }
    res
  }
}

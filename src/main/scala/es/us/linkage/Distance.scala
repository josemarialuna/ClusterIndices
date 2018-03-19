package es.us.linkage

/**
  * Created by Josem on 13/12/2016.
  */
class Distance(private var idW1: Int, private var idW2: Int, private var dist: Float) extends Serializable {

  def getIdW1: Int = idW1

  def setIdW1(idW1: Int): this.type = {
    this.idW1 = idW1
    this
  }

  def getIdW2: Int = idW2

  def setIdW2(idW2: Int): this.type = {
    this.idW2 = idW2
    this
  }

  def getDist: Float = dist

  def setDist(dist: Float): this.type = {
    this.dist = dist
    this
  }

  override def toString = s"Distance($getIdW1-$getIdW2=>$getDist)"
//override def toString = s"$getIdW1,$getIdW2,$getDist"

  override def equals(that: Any) = {
    that match {
      case f: Distance => f.getIdW1 == idW1 && f.getIdW2 == idW2 && f.getDist == dist
      case _ => false
    }
  }

  override def hashCode: Int = {
    (idW1 * idW2 * dist).toInt
  }

}

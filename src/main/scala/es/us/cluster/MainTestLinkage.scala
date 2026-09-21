package es.us.cluster

/** Linkage launcher; named options replace the unsafe twelve-argument interface. */
object MainTestLinkage {
  def main(args: Array[String]): Unit = {
    if (args.sameElements(Array("--help"))) Main.main(args)
    else {
      require(args.nonEmpty && args(0).startsWith("--"),
        "Use named options: --input data.csv --output results --algorithm is supplied by this launcher")
      Main.main(Array("--algorithm", "linkage") ++ args)
    }
  }
  def distEuclidean(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.size == v2.size, "Vector dimensions do not match")
    math.sqrt(v1.zip(v2).map { case (a, b) => (a - b) * (a - b) }.sum)
  }
}

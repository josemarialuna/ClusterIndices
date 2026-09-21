package es.us.cluster

/** Compatibility launcher: zero arguments or six historical positional arguments. */
object MainTestBKM {
  def main(args: Array[String]): Unit = {
    if (args.headOption.exists(_.startsWith("--"))) {
      if (args.sameElements(Array("--help"))) Main.main(args)
      else Main.main(Array("--algorithm", "bkm") ++ args)
    } else {
      require(args.isEmpty || args.length == 6,
        "Expected: input output-prefix min-k max-k iterations partitions")
      val input = if (args.isEmpty) "C5-D20-I1000.csv" else args(0)
      val prefix = if (args.isEmpty) input else args(1)
      Main.run(RunConfig(input, prefix + "-Results-" + Utils.whatTimeIsIt(), algorithm = "bkm",
        minK = if (args.isEmpty) 2 else args(2).toInt, maxK = if (args.isEmpty) 10 else args(3).toInt,
        iterations = if (args.isEmpty) 500 else args(4).toInt,
        partitions = if (args.isEmpty) 16 else args(5).toInt, definition = "legacy"))
    }
  }
}

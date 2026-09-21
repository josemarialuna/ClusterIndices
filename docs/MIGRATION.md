# Migration to 2.0

This is a source/build modernization, with explicit behavioral changes.

- Java 17, Scala 2.12 and Spark 3.5 replace the original Java 6 target, Scala 2.11,
  and Spark 2.2 configuration. Recompile client applications; the JAR is not binary compatible.
- `KMeansEmpleo` was missing. Training now uses official Spark `KMeans` with seed 42 by
  default. Do not expect identical historical clusters or timings.
- `Main` is the recommended entry point. It defaults to `--definition paper`.
- `ClusterIndex.getIndicesKMeans/getIndicesBKM` keep the eight-element tuple and legacy
  formulas. Typed `evaluateKMeans/evaluateBKM` default to the paper convention.
- `MainTestKMeans/MainTestBKM` accept the original six positional arguments or no arguments.
  Those paths use legacy formulas; named options use the new defaults.
- Output is now a header-bearing TSV with algorithm, convention, seed and separately
  measured shared statistics. Update scripts expecting the old nine-column headerless file.
- The old Linkage twelve-position CLI is replaced by named options. Row counts are derived
  from data; use `--drop-columns` for ID/class removal. This avoids unsafe casting and
  manually supplied counts. Small-cluster filtering is rejected instead of silently
  changing the population. Use `--max-linkage-points` to opt into larger pair matrices.
- `getIndicesLinkage` requires a caller-configured Spark checkpoint directory. It uses
  actual hierarchical memberships. It no longer evaluates nearest-center reassignment.
- Linkage `avg` remains the historical unweighted merge update (WPGMA); it is not UPGMA.
- `MainIndex` now takes `input output [partitions]`; machine-specific paths were removed.
- `Utils.whatTimeIsIt` includes seconds and milliseconds. `calculateMedian` sorts input
  and rejects empty input. `printRDD` uses Spark directory output instead of an unsafe
  executor-side writer; it remains a legacy `RDD[Unit]` helper.
- IntelliJ metadata is removed. Import the Maven project instead.

The original README example figures remain as historical illustrations and must not be
presented as newly measured results from this version.

# ClusterIndices

Clustering validity indices for Apache Spark: **BD-Silhouette**, **BD-Dunn**,
**Davies-Bouldin**, and **WSSSE**, with K-means, Bisecting K-means, and hierarchical linkage.

Based on [Luna-Romera et al., *An approach to validity indices for clustering techniques in Big Data*](https://doi.org/10.1007/s13748-017-0135-3),
*Progress in Artificial Intelligence* 7, 81-94 (2018).

> The published equations and the original repository implement different conventions.
> Version 2 makes this distinction explicit: `paper` follows equations 6-10 using
> Euclidean distance; `legacy` retains the repository's squared-distance, pairwise-center
> formulas. See [Scientific definitions](docs/SCIENTIFIC_DEFINITIONS.md) before comparing results.

## Requirements and build

- JDK **17** (`JAVA_HOME` must point to that JDK).
- Spark **3.5.8**, built for Scala **2.12**, for `spark-submit`.
- Maven **3.6.3+**, or the included Maven wrapper.

```sh
./mvnw verify
# Windows PowerShell:
.\mvnw.cmd verify
```

The build compiles Scala, runs numerical and local Spark tests, and creates
`target/clusterIndices-2.0.0-SNAPSHOT.jar`. Spark and Scala are provided by the
Spark distribution; they are deliberately not bundled into the JAR.
Tests start Spark on `local[2]`; Linux is the reference CI environment.
On Windows, Hadoop filesystem operations may require a compatible native Hadoop setup;
WSL is an alternative. Do not use arbitrary third-party Hadoop executables.

## Quick start

The included `C5-D20-I1000.csv` has 5,000 rows and 20 numeric features, without a header.
The dataset description specifies five generated clusters; class labels are not included.

```sh
spark-submit --class es.us.cluster.Main --master 'local[2]' \
  target/clusterIndices-2.0.0-SNAPSHOT.jar \
  --input C5-D20-I1000.csv --output results/kmeans-paper \
  --algorithm kmeans --min-k 2 --max-k 10 --seed 42 --definition paper
```

Use `--algorithm bkm` for Bisecting K-means. The output directory must not exist;
existing results are never overwritten. `--help` lists the options without starting Spark.
Spark deployment options, such as `--master`, normally precede the JAR. The application
also accepts an explicit `--master` override after the JAR; otherwise it respects
Spark's configuration and falls back to `local[*]` only when no master is configured.

### Input contract

Input is a simple comma-separated numeric matrix. Quoted fields, embedded commas,
missing values, NaN, infinity, empty rows, and inconsistent dimensions are unsupported.
Errors identify the row/column where possible. All retained columns are features;
there is **no automatic scaling**, imputation, or label detection.

For a single input file containing a header, an ID column, and a class column:

```sh
# Append these application options to the command above:
--header true --drop-columns 0,4
```

Column positions are zero-based. `--header true` skips the first line of the whole input,
not one header per file in a directory. Remove per-file headers before loading a directory.

### Hierarchical linkage

```sh
spark-submit --class es.us.cluster.Main --master 'local[2]' \
  target/clusterIndices-2.0.0-SNAPSHOT.jar \
  --input small-data.csv --output results/linkage \
  --algorithm linkage --linkage avg --min-k 2 --max-k 8 \
  --checkpoint checkpoints --max-linkage-points 2000
```

Linkage creates all `n*(n-1)/2` point distances. It is intended for small datasets;
Spark does not remove its quadratic storage cost. The default 2,000-point guard can
be raised deliberately with `--max-linkage-points`. On a distributed cluster,
`--checkpoint` must be a shared filesystem URI accessible to executors.

Strategies: `min` (single linkage), `max` (complete linkage), and `avg` (**WPGMA**,
equal weights for the two merged clusters, not size-weighted UPGMA).
Linkage distances retain the original `Float` representation. The merge history is
built once, then cut for each requested k. Evaluation uses those actual memberships.

## Results

The output directory contains `part-00000` (TSV with a header), Spark's success marker,
and `_metadata.properties` recording input, versions, dimensions and experiment settings.
Columns are:

```text
algorithm definition seed k bd_silhouette bd_dunn davies_bouldin wssse
statistics_ms silhouette_ms dunn_ms davies_bouldin_ms wssse_ms
```

The first four identify the experiment. Four scores follow, then shared aggregation
and individual formula timings in milliseconds. Training, parsing, linkage construction,
and centroid construction are excluded. Shared statistics are computed once; the
formula-only times can be zero. These timings are not comparable to the original
implementation's repeated Spark jobs. The seed applies to K-means/BKM; linkage is deterministic
for a fixed ordered input. Floating-point reductions can vary slightly with partitioning.

Undefined metrics are written as `NaN`, not a misleading zero (see the scientific definitions).
The program reports scores across k; it does **not** automatically choose an optimum or
promise reproduction of the paper's experimental tables.

## Library API

```scala
import es.us.cluster.{ClusterIndex, IndexDefinition, ValidityIndices}

// data: RDD[org.apache.spark.mllib.linalg.Vector]
val result = ClusterIndex.evaluateKMeans(data, k = 5, iterations = 100,
  seed = 42L, definition = IndexDefinition.Paper)
println(result.bdSilhouette)

// Evaluate an existing partition without retraining:
// assignments: RDD[(Int, Vector)], labels index the centers array (0..k-1)
val scores = ValidityIndices.evaluate(assignments, centers, IndexDefinition.Paper)
```

Every center must have at least one assigned point. The direct evaluator accepts one
cluster; training experiments require k >= 2. Cache input reused across experiments
and release it when finished. The evaluator does not cache or unpersist caller-owned RDDs.

## Project guide

- [Architecture](docs/ARCHITECTURE.md)
- [Scientific definitions and numerical conventions](docs/SCIENTIFIC_DEFINITIONS.md)
- [Migration from the original repository](docs/MIGRATION.md)
- [Contributing and testing](CONTRIBUTING.md)
- [Changes](CHANGELOG.md)

The `MainTestKMeans` and `MainTestBKM` launchers retain zero/six-argument entry points
and legacy formulas, but write the new self-describing TSV schema. New code should use `Main`.

## Citation and license

Use the publication DOI above when citing the scientific method. Machine-readable citation
metadata is in [CITATION.cff](CITATION.cff). The source is licensed under [Apache-2.0](LICENSE).
Original contributors: José María Luna-Romera, Jorge García-Gutiérrez,
Maria Martínez-Ballesteros, and José C. Riquelme Santos; linkage code also credits
José David Martín. Publication authorship is preserved separately from software changes.

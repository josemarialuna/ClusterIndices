# Architecture

```text
CSV -> Main.load -> RDD[Vector] -> KMeans / BisectingKMeans
                                  -> labeled points + centers
                              -> Linkage pair distances -> merge history
                                  -> memberships + centers
                                         |
                                  ValidityIndices.evaluate
                                         |
                                    IndexResult -> TSV
```

`Main` owns configuration, CSV validation, Spark lifecycle, input persistence and output.
It respects the deployment master and releases cached input in `finally` blocks.
`ClusterIndex` trains MLlib models and exposes the original tuple adapters.
`ValidityIndices` evaluates existing assignments in one distributed aggregation:
count, sum of distances, sum of squared distances, and maximum radius per cluster.
It broadcasts centers and destroys that broadcast after the aggregation action completes.
Only per-cluster summaries return to the driver. Formula evaluation costs O(k^2*d)
for Davies-Bouldin (also historical pair separation), with no n-by-n point distances.
Supplied-label aggregation costs O(n*d); nearest-center assignment during training
model evaluation adds O(n*k*d).

`Linkage` uses RDD joins to update neighbor distances after each merge. Equal-distance
ties are resolved by endpoint IDs. It materializes each replacement matrix before
unpersisting the previous one and checkpoints periodically to truncate lineage.
`LinkageModel` stores the merge history; cuts use a parent array with path compression
on the driver, and centers use a distributed join/reduction. Point IDs are consecutive
and generated with `zipWithIndex`, independent of input partition numbering.

Linkage still has quadratic pair storage, sequential merges, and driver-resident O(n)
merge metadata. It is not the scalable part of the research contribution. Historical
helper entry points delegate to the same engine; the DataFrame adapter expects columns
`idW1`, `idW2`, and `dist`. A dendrogram export stopped at k > 1 is a partial hierarchy.

No database, HTTP service, or UI is involved. Spark filesystem APIs handle input,
results, and checkpoints. Experiment results are generated only when all requested
k values have succeeded; existing output directories are rejected.

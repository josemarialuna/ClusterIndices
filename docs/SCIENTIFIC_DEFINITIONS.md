# Scientific definitions

Reference: Luna-Romera et al. (2018), DOI [10.1007/s13748-017-0135-3](https://doi.org/10.1007/s13748-017-0135-3),
section 3.2, equations 6-10 (printed pages 84-85).

## Paper convention (`paper`)

Let `A_i` be a nonempty cluster, `C_i` its centroid, and `k` the number of clusters.
`d(a,b)` is instantiated here as Euclidean distance. The paper introduces a generic
`d`; choosing Euclidean distance is an explicit implementation convention, not a
claim that every distance choice reproduces the published experiments.

- `C_0 = (1/k) sum_i C_i`: the unweighted mean of cluster centers (the explicit
  definition immediately following equation 6), not the mean of all observations.
- `r_i = (1/|A_i|) sum_(x in A_i) d(x,C_i)` (equation 7).
- `inter = (1/k) sum_i d(C_i,C_0)` (equation 6).
- `intra = (1/k) sum_i r_i` (equation 8, interpreted as a sum over clusters).
- `BD-Silhouette = (inter-intra)/max(inter,intra)` (equation 9).
- `BD-Dunn = min_i d(C_i,C_0) / max_(i,x in A_i) d(x,C_i)` (equation 10).

This is not the classical sample-wise Silhouette or pairwise-diameter Dunn.
A center coinciding with `C_0` legitimately gives BD-Dunn zero even when other
centers are separated. These definitions should not be changed merely to make a
score look more intuitive.

Davies-Bouldin is a companion score, not a new equation introduced by this paper:
`DB = (1/k) sum_i max_(j != i) (r_i+r_j)/d(C_i,C_j)`.
WSSSE always sums **squared** Euclidean distances to the supplied cluster centers.
The evaluator honors supplied labels; it does not replace hierarchical memberships
with nearest-center assignments.

## Historical convention (`legacy`)

The repository at commit `4c9b8bb1df763007d503f6c2712aa280620bd982` differs:

| Quantity | Historical computation |
| --- | --- |
| Distance | Squared Euclidean |
| Inter | Mean distance over distinct pairs of centers |
| Intra | WSSSE divided by number of observations |
| BD-Dunn numerator | Minimum distance between distinct center indices |
| BD-Dunn denominator | Maximum squared distance of a point to its assigned center |
| Davies-Bouldin | Mean squared within-cluster dispersions / squared center separation |

For nondegenerate partitions these formulas are retained. `legacy` is not binary,
seed, timing, or full experimental reproducibility: the missing `KMeansEmpleo` has
been replaced by standard Spark KMeans, seeds are explicit, and Linkage now honors
its actual partition. Duplicate-center and empty-cluster behavior was previously
inconsistent or could fail. See the defined policy below.

## Undefined values and validation

- Empty input, unused centers, invalid labels, mismatched dimensions, and nonfinite
  coordinates are rejected. Distance overflow is rejected with a rescaling message.
- BD-Silhouette is `NaN` when both intra and inter are zero.
- BD-Dunn is `NaN` when the maximum radius is zero (the paper says the ratio cannot
  be calculated in this case); it is not silently promoted to infinity.
- Davies-Bouldin is `NaN` for fewer than two clusters or coincident centers.
- A nondegenerate single cluster has paper BD-Silhouette -1 and BD-Dunn 0.
- The historical convention requires at least two clusters.

## Hand-calculated regression fixture

Clusters `{0,2}` and `{8,10,12,14}` have centers 1 and 11. Their unequal sizes expose
accidental point-weighting. The unweighted global center is 6.

| Score | Paper | Legacy |
| --- | --- | --- |
| BD-Silhouette | 0.7 | 1 - 22/600 |
| BD-Dunn | 5/3 | 100/9 |
| Davies-Bouldin | 0.3 | 0.06 |
| WSSSE | 22 | 22 |

Tests also cover zero radius, one cluster, unused and coincident centers, seeded
training, and hierarchical membership. These are software correctness checks,
not a replication of the full research experiment.

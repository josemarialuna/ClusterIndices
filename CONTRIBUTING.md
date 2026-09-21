# Contributing

Use JDK 17 and run `./mvnw verify` (Windows: `.\mvnw.cmd verify`) before submitting changes.
Tests require local loopback networking for Spark. Linux is the reference CI platform.
Do not commit generated results, checkpoints, IDE settings, datasets from private sources,
or a copy of the publication PDF.

Keep scientific definitions explicit. Any formula change must update
`docs/SCIENTIFIC_DEFINITIONS.md`, add an independently calculated numerical fixture,
and describe compatibility implications. Keep training and evaluation separate.
Use a fixed seed for stochastic tests and tolerances for floating-point comparisons.

For bugs, include Java/Spark versions, the command, the definition mode, a small synthetic
input, and expected versus actual behavior. For performance claims, include n, dimensions,
k, partitioning, hardware, cache state, and which phases were timed.

Pull requests should explain the behavior change and validation. Do not publish unverified
benchmark claims or describe software tests as reproduction of the research results.

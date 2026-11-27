Data generated using Databricks 18 LTS Runtime, backed by AWS S3.
At least two columns are required for Hilbert clustering.

`spark.databricks.delta.optimize.maxFileSize` is tuned down from its default so that each batch's
compacted output already exceeds the target, which keeps a later whole-table `OPTIMIZE` from merging
files across years (Liquid Clustering tables reject a predicate-scoped `OPTIMIZE ... WHERE`, so this
is the only lever available to keep batches physically separate).

```sql
CREATE TABLE test_liquid_clustering_multi_column
(data string, year int, month int)
USING delta
CLUSTER BY (year, month)
LOCATION ?;

SET spark.databricks.delta.optimize.maxFileSize = 1800;

INSERT INTO test_liquid_clustering_multi_column
  SELECT concat('row ', 2021, '-', cast(id % 12 + 1 as string)) AS data, 2021 AS year, cast(id % 12 + 1 as int) AS month
  FROM range(100);

OPTIMIZE test_liquid_clustering_multi_column;

INSERT INTO test_liquid_clustering_multi_column
  SELECT concat('row ', 2022, '-', cast(id % 12 + 1 as string)) AS data, 2022 AS year, cast(id % 12 + 1 as int) AS month
  FROM range(100);

INSERT INTO test_liquid_clustering_multi_column
  SELECT concat('row ', 2023, '-', cast(id % 12 + 1 as string)) AS data, 2023 AS year, cast(id % 12 + 1 as int) AS month
  FROM range(100);
```

Result: 3 files, each holding exactly one year's 100 rows (`year` ranges are disjoint:
`[2021,2021]`, `[2022,2022]`, `[2023,2023]`). The `OPTIMIZE` call above only ran while a single file
existed, so it committed as a no-op; the two later batches were never re-optimized, meaning this
fixture demonstrates read compatibility with a Liquid Clustering table shaped by write-time clustering
plus one no-op `OPTIMIZE`, not `OPTIMIZE` re-clustering multiple pre-existing files.

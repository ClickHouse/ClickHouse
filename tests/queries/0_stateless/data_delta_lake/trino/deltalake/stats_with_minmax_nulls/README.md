Data generated using OSS Delta Lake 2.4.0 on Spark 3.4.0:

```
bin/spark-sql --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.jsonGenerator.ignoreNullFields=false" \
  --conf "spark.sql.shuffle.partitions=1"
```

```sql
CREATE TABLE delta.`/tmp/stats-with-minmax-nulls`
USING DELTA
TBLPROPERTIES('delta.checkpointInterval' = 2) AS
SELECT /*+ REPARTITION(1) */ col1 as id, col2 as id2 FROM VALUES (0, 1),(1,2),(3, 4);

INSERT INTO delta.`/tmp/stats-with-minmax-nulls` SELECT null, null;

-- creates checkpoint
-- Checkpoint file contains stats with min and max values as null
-- .stats = {"numRecords":1,"minValues":{"id":null,"id2":null},"maxValues":{"id":null,"id2":null},"nullCount":{"id":1,"id2":1}}
INSERT INTO delta.`/tmp/stats-with-minmax-nulls` SELECT 3, 7;

INSERT INTO delta.`/tmp/stats-with-minmax-nulls` SELECT null, null;
```

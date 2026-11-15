Data generated using EMR-7.8.0 & Delta Lake 3.3.0

This test resource is used to verify whether the reading from Delta Lake tables with 
multi-part v2 checkpoint files works as expected.

Trino
```
CREATE TABLE test_v2_checkpoint_multi_sidecar
(a INT, b INT)
USING delta 
LOCATION ? 
TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '4');
```

From https://docs.delta.io/latest/optimizations-oss.html

> In Delta Lake, by default each checkpoint is written as a single Parquet file. To to use this feature, 
> set the SQL configuration ``spark.databricks.delta.checkpoint.partSize=<n>``, where n is the limit of 
> number of actions (such as `AddFile`) at which Delta Lake on Apache Spark will start parallelizing the 
> checkpoint and attempt to write a maximum of this many actions per checkpoint file.

Spark
```
SET spark.databricks.delta.checkpoint.partSize=4;
INSERT INTO test_v2_checkpoint_multi_sidecar VALUES (1, 2);
INSERT INTO test_v2_checkpoint_multi_sidecar VALUES (3, 4);
INSERT INTO test_v2_checkpoint_multi_sidecar VALUES (5, 6);
INSERT INTO test_v2_checkpoint_multi_sidecar VALUES (7, 8);
```

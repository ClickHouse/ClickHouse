Data generated using Delta Lake 3.3.0:

```sql
SET spark.databricks.delta.checkpointV2.topLevelFileFormat = parquet;

CREATE TABLE test_v2_checkpoint_parquet_using_temporal
(a INT, b INT)
USING delta 
LOCATION ?
TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '2');

INSERT INTO test_v2_checkpoint_parquet_using_temporal VALUES (1, 2);
INSERT INTO test_v2_checkpoint_parquet_using_temporal VALUES (3, 4);
INSERT INTO test_v2_checkpoint_parquet_using_temporal VALUES (5, 6);
```

Then remove version 0 and 1 metadata files:`
_delta_log/00000000000000000000.json`,`_delta_log/00000000000000000000.crc`,
`_delta_log/00000000000000000001.json`, `_delta_log/00000000000000000001.json`.

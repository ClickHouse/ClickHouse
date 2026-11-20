Data generated using Databricks 13.3:

```sql
SET spark.databricks.delta.checkpointV2.topLevelFileFormat = parquet;

CREATE TABLE test_v2_checkpoint_parquet
(a INT, b INT)
USING delta 
LOCATION ? 
TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '1');

INSERT INTO test_v2_checkpoint_parquet VALUES (1, 2);
```

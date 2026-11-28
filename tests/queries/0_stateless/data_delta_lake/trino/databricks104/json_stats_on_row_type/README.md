Data generated using Databricks 10.4:

```sql
CREATE TABLE default.json_stats_on_row_type
 (struct_col struct<x bigint>, nested_struct_col struct<y struct<nested string>>)
USING DELTA 
LOCATION 's3://bucket/table' 
TBLPROPERTIES (
 delta.checkpointInterval = 2,
 delta.checkpoint.writeStatsAsJson = false,
 delta.checkpoint.writeStatsAsStruct = true
);

INSERT INTO default.json_stats_on_row_type SELECT named_struct('x', 1), named_struct('y', named_struct('nested', 'test'));
INSERT INTO default.json_stats_on_row_type SELECT named_struct('x', NULL), named_struct('y', named_struct('nested', NULL));

ALTER TABLE default.json_stats_on_row_type SET TBLPROPERTIES (
 'delta.checkpoint.writeStatsAsJson' = true,
 'delta.checkpoint.writeStatsAsStruct' = false
);
```

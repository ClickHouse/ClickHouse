Data generated using Databricks 14.3 with Unity.
`delta.universalFormat.enabledFormats` requires Unity catalog.

```sql
CREATE TABLE main.default.test_uniform_iceberg_v2
(a integer, b string)
USING DELTA 
TBLPROPERTIES (
 'delta.enableIcebergCompatV2' = 'true',
 'delta.universalFormat.enabledFormats' = 'iceberg'
);

INSERT INTO main.default.test_uniform_iceberg_v2 VALUES (1, 'test data');
```

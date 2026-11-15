Data generated using Databricks 13.3 with Unity.
`delta.universalFormat.enabledFormats` requires Unity catalog.

```sql
CREATE TABLE main.default.test_uniform_iceberg_v1
(a integer, b string)
USING DELTA 
TBLPROPERTIES (
 'delta.enableIcebergCompatV1' = 'true',
 'delta.universalFormat.enabledFormats' = 'iceberg'
);

INSERT INTO main.default.test_uniform_iceberg_v1 VALUES (1, 'test data');
```

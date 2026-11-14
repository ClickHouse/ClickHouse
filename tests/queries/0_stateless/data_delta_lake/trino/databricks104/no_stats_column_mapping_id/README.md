Data generated using:

```sql
CREATE TABLE default.no_stats_column_mapping_id
USING delta
location 's3://starburstdata-test/no_stats_column_mapping_id' 
TBLPROPERTIES (delta.dataSkippingNumIndexedCols =0, 'delta.columnMapping.mode' = 'id')
AS
SELECT 42 AS c_int, 'foo' AS c_str;

INSERT INTO no_stats_column_mapping_id VALUES (1, 'a'),(2, 'b'),(null, null);
```

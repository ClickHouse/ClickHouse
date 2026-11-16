Data generated using:

```sql
CREATE TABLE default.no_column_stats_mixed_case (c_Int int, c_Str string)
    USING delta
location 's3://starburstdata-test/no_column_stats_mixed_case'
TBLPROPERTIES (delta.dataSkippingNumIndexedCols =0); -- collects only table stats (row count), but no column stats
INSERT INTO no_column_stats_mixed_case VALUES (11, 'a'),(2, 'b'),(null, null);
OPTIMIZE no_column_stats_mixed_case; -- As databricks creates 2 parquet files per insert this ensures that we have all data in one file
```

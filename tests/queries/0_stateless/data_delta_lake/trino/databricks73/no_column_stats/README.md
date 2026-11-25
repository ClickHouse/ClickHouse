Data generated using:

```sql
CREATE TABLE no_column_stats
USING delta
LOCATION 's3://starburst-alex/delta/no_column_stats'
TBLPROPERTIES (delta.dataSkippingNumIndexedCols=0)   -- collects only table stats (row count), but no column stats
AS
SELECT 42 AS c_int, 'foo' AS c_str
```

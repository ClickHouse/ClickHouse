# paimon_nullable_composites {#paimon-nullable-composites}

A Paimon table with nullable `ARRAY` and `MAP` columns (the Paimon default
nullability), including a row where both composite values are `NULL`. Used by
`04757_paimon_nullable_composite_types.sql` to verify that nullable composite
columns are readable (https://github.com/ClickHouse/ClickHouse/issues/113337)
and that `NULL` composites are read as empty values.

Generated with `org.apache.paimon:paimon-spark-3.5:1.1.1` on Spark 3.5.5:

```sql
CREATE TABLE paimon.default.t (id INT NOT NULL, arr ARRAY<INT>, m MAP<STRING, INT>)
  TBLPROPERTIES ('file.format'='parquet');
INSERT INTO paimon.default.t VALUES (1, array(1, 2), map('k', 1)), (2, NULL, NULL);
```

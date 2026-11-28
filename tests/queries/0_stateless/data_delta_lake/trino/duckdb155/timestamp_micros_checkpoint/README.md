Data file written by DuckDB 1.5.5, which stores `TIMESTAMP WITH TIME ZONE` as parquet `INT64 (TIMESTAMP(MICROS,true))`:

```sql
COPY (SELECT 1::INTEGER AS id, TIMESTAMPTZ '2020-08-26 01:02:03.456789+00' AS ts) TO 'part-00000.parquet' (FORMAT parquet);
```

The transaction log and the version 0 checkpoint were written by hand, the checkpoint with DuckDB from a table whose `add.stats_parsed` struct holds the minimum and maximum of `ts` as `TIMESTAMPTZ`, so they are stored as `INT64 (TIMESTAMP(MICROS,true))` like Delta Lake for Spark writes them. Both bounds are `2020-08-26 01:02:03.456789 UTC`, and the `add` entry carries no JSON `stats`.

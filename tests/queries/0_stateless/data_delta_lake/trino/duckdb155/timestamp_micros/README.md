Data file written by DuckDB 1.5.5, which stores `TIMESTAMP WITH TIME ZONE` as parquet `INT64 (TIMESTAMP(MICROS,true))`:

```sql
COPY (
    SELECT * FROM (VALUES
        (1, TIMESTAMPTZ '2020-08-26 01:02:03.123456+00'),
        (2, TIMESTAMPTZ '2020-08-26 01:02:03.456789+00')) AS t(id, ts)
    ORDER BY id)
TO 'part-00000.parquet' (FORMAT parquet);
```

The transaction log was written by hand, with deletion vectors enabled and statistics truncated to milliseconds the way Delta Lake for Spark writes them. The recorded maximum `2020-08-26T01:02:03.456Z` is therefore below the `01:02:03.456789` value in the file.

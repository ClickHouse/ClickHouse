Data file written by DuckDB 1.5.5, with the `timestamp` field nested in a struct column:

```sql
COPY (SELECT 1::INTEGER AS id, {'ts': TIMESTAMPTZ '2020-08-26 01:02:03.456789+00'} AS r) TO 'part-00000.parquet' (FORMAT parquet);
```

The transaction log was written by hand in the shape Delta Lake for Spark writes: the `add` entry carries JSON statistics with the nested bound `"minValues":{"id":1,"r":{"ts":"2020-08-26T01:02:03.456Z"}}`, and the table sets `delta.checkpointInterval` to 2, so the next two commits make the connector build a checkpoint from those nested statistics.

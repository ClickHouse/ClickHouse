Data generated using Trino 481, which mapped the Delta `timestamp` type to
`timestamp(3) with time zone` and wrote it as parquet `INT64 (TIMESTAMP(MILLIS,false))`:

```sql
CREATE TABLE timestamp_tz_millis (id INTEGER, ts TIMESTAMP WITH TIME ZONE);

INSERT INTO timestamp_tz_millis VALUES
(1, TIMESTAMP '2024-01-15 10:30:00.123 UTC'),
(2, TIMESTAMP '2024-06-20 16:45:30.456 UTC'),
(3, NULL);
```

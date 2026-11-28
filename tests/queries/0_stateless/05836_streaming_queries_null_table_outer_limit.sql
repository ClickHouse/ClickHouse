-- A streaming query from a `Null` table waits for inserts. When an outer `LIMIT` is satisfied by another branch
-- of the query, the waiting source must finish instead of waiting for the next insert.

SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS null_outer_limit;
CREATE TABLE null_outer_limit (x UInt64) ENGINE = Null;

SELECT * FROM
(
    SELECT x FROM null_outer_limit STREAM
    UNION ALL
    SELECT toUInt64(7) + sleep(0.5) AS x
)
LIMIT 1
SETTINGS max_threads = 2, max_execution_time = 300;

DROP TABLE null_outer_limit;

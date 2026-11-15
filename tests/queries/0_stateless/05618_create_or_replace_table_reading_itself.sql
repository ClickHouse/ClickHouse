-- Random settings limits: enable_shared_storage_snapshot_in_query=(1, None)

SET database_atomic_wait_for_drop_and_detach_synchronously = 1;
-- Without the fix each replace waits for its own snapshot of the old table: fail instead of hanging the job.
SET max_execution_time = 60;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(10);

CREATE OR REPLACE TABLE t ENGINE = MergeTree ORDER BY x AS SELECT * FROM t;
SELECT count() FROM t;

REPLACE TABLE t ENGINE = MergeTree ORDER BY x AS SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x < 5);
SELECT count() FROM t;

CREATE OR REPLACE TABLE t ENGINE = MergeTree ORDER BY x EMPTY AS SELECT * FROM t;
SELECT count() FROM t;

-- The old object is a materialized view: its inner table is read and then dropped.
-- Without the fix this wait cannot be cancelled: keep the cases above first, so an unfixed build fails before it.
DROP TABLE IF EXISTS mv;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM t;
INSERT INTO t SELECT number FROM numbers(3);
CREATE OR REPLACE TABLE mv ENGINE = MergeTree ORDER BY x AS SELECT * FROM mv;
SELECT count() FROM mv;

DROP TABLE mv;
DROP TABLE t;

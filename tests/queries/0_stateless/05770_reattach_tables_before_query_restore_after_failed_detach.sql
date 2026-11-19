-- Tags: no-replicated-database
-- Tag no-replicated-database: tables of `Replicated` databases are not reattached.

-- `DETACH TABLE ... SYNC` removes the table from the database first and then waits until it is no longer in use.
-- If it fails after the table is removed (e.g. the wait is cancelled), the table must be attached back.
-- The failpoint throws right after the internal `DETACH` has detached the table.

DROP TABLE IF EXISTS t_reattach;
CREATE TABLE t_reattach (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_reattach VALUES (42);

SYSTEM ENABLE FAILPOINT reattach_tables_throw_after_detach;
SELECT 1 SETTINGS reattach_tables_before_query_execution_probability = 1;
SYSTEM DISABLE FAILPOINT reattach_tables_throw_after_detach;

EXISTS TABLE t_reattach;
SELECT * FROM t_reattach;

DROP TABLE t_reattach;

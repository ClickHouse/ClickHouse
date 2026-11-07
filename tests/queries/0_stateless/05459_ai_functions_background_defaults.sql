-- AI functions are rejected in `DEFAULT` and `MATERIALIZED` expressions computed in the background:
-- of a column with a TTL, and of a column added or modified by `ALTER`.

CREATE TABLE t1 (x String, d DateTime, y String DEFAULT aiGenerate(x) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY x; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t1 (x String, d DateTime, y String MATERIALIZED aiGenerate(x) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY x; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t2 (x String, y String DEFAULT aiGenerate(x)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t2 ADD COLUMN z String DEFAULT aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 ADD COLUMN z String MATERIALIZED aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 MODIFY COLUMN x String DEFAULT aiGenerate(y); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 ADD COLUMN z String ALIAS aiGenerate(x);
ALTER TABLE t2 MATERIALIZE COLUMN y; -- { serverError NOT_IMPLEMENTED }
SELECT name, default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't2' ORDER BY position;

DROP TABLE t2;

-- Column TTL added by `ALTER`.
CREATE TABLE t3 (x String, d DateTime, y String, w String DEFAULT aiGenerate(x)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t3 MODIFY COLUMN y TTL d + toIntervalDay(length(aiGenerate(x))); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t3 MODIFY COLUMN w TTL d; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t3;

-- A lightweight `DELETE` executed as a mutation is rejected. Executed as a lightweight update, it runs in the query
-- and fails on the missing credentials.
CREATE TABLE t4 (x String) ENGINE = MergeTree ORDER BY x SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DELETE FROM t4 WHERE aiFilter(x, 'c') SETTINGS lightweight_delete_mode = 'alter_update'; -- { serverError NOT_IMPLEMENTED }
DELETE FROM t4 WHERE aiFilter(x, 'c') SETTINGS lightweight_delete_mode = 'lightweight_update_force', enable_lightweight_update = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE t4;

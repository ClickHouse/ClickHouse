-- AI functions are rejected in `DEFAULT` and `MATERIALIZED` expressions computed in the background:
-- of a column with a TTL, and of a column added or modified by `ALTER`.

CREATE TABLE t1 (x String, d DateTime, y String DEFAULT aiGenerate(x) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY x; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t1 (x String, d DateTime, y String MATERIALIZED aiGenerate(x) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY x; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t2 (x String, y String DEFAULT aiGenerate(x)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t2 ADD COLUMN z String DEFAULT aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 ADD COLUMN z String MATERIALIZED aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 MODIFY COLUMN x String DEFAULT aiGenerate(y); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t2 ADD COLUMN z String ALIAS aiGenerate(x);
SELECT name, default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't2' ORDER BY position;

DROP TABLE t2;

-- The RETURNING query of an INSERT reads the inserted table through `merge` and a `Merge` table
-- while the query shares one storage snapshot per table between all its reads.

SET enable_shared_storage_snapshot_in_query = 1;

DROP TABLE IF EXISTS t_returning_merge_m;
DROP TABLE IF EXISTS t_returning_merge;

CREATE TABLE t_returning_merge (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_returning_merge_m (id UInt64, s String) ENGINE = Merge(currentDatabase(), '^t_returning_merge$');
INSERT INTO t_returning_merge VALUES (1, 'a');

INSERT INTO t_returning_merge RETURNING (SELECT (SELECT count() FROM merge(currentDatabase(), '^t_returning_merge$') WHERE s = 'a')) VALUES (2, 'a');
INSERT INTO t_returning_merge RETURNING (SELECT count(), (SELECT count() FROM t_returning_merge_m WHERE s = 'a') FROM t_returning_merge) VALUES (3, 'a');
INSERT INTO t_returning_merge RETURNING (SELECT (SELECT count() FROM merge(currentDatabase(), '^t_returning_merge$') WHERE id = 1 SETTINGS force_primary_key = 1)) VALUES (4, 'a');
SELECT count() FROM t_returning_merge;

DROP TABLE t_returning_merge_m;
DROP TABLE t_returning_merge;

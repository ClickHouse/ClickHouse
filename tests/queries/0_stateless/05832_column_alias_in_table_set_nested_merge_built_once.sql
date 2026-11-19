-- An `ALIAS p IN table` of the children of a `Merge` read is computed by the child plans, and its set is built once
-- for the whole read. This holds for the children of nested `Merge` reads as well: every child sees the same set.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_alias_set_nested_outer;
DROP TABLE IF EXISTS t_alias_set_nested_merge_1;
DROP TABLE IF EXISTS t_alias_set_nested_merge_2;
DROP TABLE IF EXISTS t_alias_set_nested_1;
DROP TABLE IF EXISTS t_alias_set_nested_2;
DROP TABLE IF EXISTS keys_alias_set_nested;

CREATE TABLE keys_alias_set_nested (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys_alias_set_nested SELECT number * 2 FROM numbers(50);

CREATE TABLE t_alias_set_nested_1 (p UInt64, f UInt8 ALIAS p IN keys_alias_set_nested) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_alias_set_nested_2 (p UInt64, f UInt8 ALIAS p IN keys_alias_set_nested) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_alias_set_nested_1 SELECT number FROM numbers(100);
INSERT INTO t_alias_set_nested_2 SELECT number FROM numbers(100);

CREATE TABLE t_alias_set_nested_merge_1 AS t_alias_set_nested_1 ENGINE = Merge(currentDatabase(), '^t_alias_set_nested_1$');
CREATE TABLE t_alias_set_nested_merge_2 AS t_alias_set_nested_2 ENGINE = Merge(currentDatabase(), '^t_alias_set_nested_2$');
CREATE TABLE t_alias_set_nested_outer AS t_alias_set_nested_1 ENGINE = Merge(currentDatabase(), '^t_alias_set_nested_merge_');

SELECT sum(f) FROM t_alias_set_nested_outer SETTINGS log_comment = 'alias_set_nested_alias';
SELECT sum(p IN keys_alias_set_nested) FROM t_alias_set_nested_outer SETTINGS log_comment = 'alias_set_nested_direct';
SELECT sum(f) FROM merge(currentDatabase(), '^t_alias_set_nested_[12]$') SETTINGS log_comment = 'alias_set_nested_flat';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'alias_set_nested_%'
ORDER BY log_comment;

DROP TABLE t_alias_set_nested_outer;
DROP TABLE t_alias_set_nested_merge_1;
DROP TABLE t_alias_set_nested_merge_2;
DROP TABLE t_alias_set_nested_1;
DROP TABLE t_alias_set_nested_2;
DROP TABLE keys_alias_set_nested;

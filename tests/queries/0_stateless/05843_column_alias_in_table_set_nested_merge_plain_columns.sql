-- The set of an `ALIAS p IN table` is built once for a read through nested `Merge` tables at the default settings too,
-- when the intermediate `Merge` tables declare the column as a plain one. A `Merge` read assigns some settings for its
-- children explicitly, such as `enable_parallel_replicas = 0`, and it must not stop a nested read, which has the same
-- value by default, from sharing the set.

DROP TABLE IF EXISTS t_alias_set_plain_merge_1;
DROP TABLE IF EXISTS t_alias_set_plain_merge_2;
DROP TABLE IF EXISTS t_alias_set_plain_1;
DROP TABLE IF EXISTS t_alias_set_plain_2;
DROP TABLE IF EXISTS keys_alias_set_plain;

CREATE TABLE keys_alias_set_plain (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys_alias_set_plain SELECT number * 2 FROM numbers(50);

CREATE TABLE t_alias_set_plain_1 (p UInt64, f UInt8 ALIAS p IN keys_alias_set_plain) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_alias_set_plain_2 (p UInt64, f UInt8 ALIAS p IN keys_alias_set_plain) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_alias_set_plain_1 SELECT number FROM numbers(100);
INSERT INTO t_alias_set_plain_2 SELECT number FROM numbers(100);

CREATE TABLE t_alias_set_plain_merge_1 (p UInt64, f UInt8) ENGINE = Merge(currentDatabase(), '^t_alias_set_plain_1$');
CREATE TABLE t_alias_set_plain_merge_2 (p UInt64, f UInt8) ENGINE = Merge(currentDatabase(), '^t_alias_set_plain_2$');

SELECT sum(f) FROM merge(currentDatabase(), '^t_alias_set_plain_merge_') SETTINGS log_comment = 'alias_set_plain_alias';
SELECT sum(p IN keys_alias_set_plain) FROM merge(currentDatabase(), '^t_alias_set_plain_merge_') SETTINGS log_comment = 'alias_set_plain_direct';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'alias_set_plain_%'
ORDER BY log_comment;

DROP TABLE t_alias_set_plain_merge_1;
DROP TABLE t_alias_set_plain_merge_2;
DROP TABLE t_alias_set_plain_1;
DROP TABLE t_alias_set_plain_2;
DROP TABLE keys_alias_set_plain;

-- A `DEFAULT p IN table` of a column missing in a part is evaluated by the reader for every block it reads.
-- The set of the default has to be built once for the whole reading query, not once per block or per reader.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_default_set_once;
DROP TABLE IF EXISTS t_default_set_once_rhs;

CREATE TABLE t_default_set_once_rhs (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_once_rhs SELECT number * 2 FROM numbers(10000);

CREATE TABLE t_default_set_once (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_once SELECT number FROM numbers(100000);

ALTER TABLE t_default_set_once ADD COLUMN f UInt8 DEFAULT p IN t_default_set_once_rhs;

SELECT sum(f) FROM t_default_set_once SETTINGS max_threads = 1, max_block_size = 1000, log_comment = 'set_built_once_single_thread';
SELECT sum(f) FROM t_default_set_once SETTINGS max_threads = 4, max_block_size = 1000, log_comment = 'set_built_once_multiple_threads';
SELECT sum(p IN t_default_set_once_rhs) FROM t_default_set_once SETTINGS max_threads = 1, max_block_size = 1000, log_comment = 'set_built_once_direct';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'set_built_once_%'
ORDER BY log_comment;

DROP TABLE t_default_set_once;
DROP TABLE t_default_set_once_rhs;

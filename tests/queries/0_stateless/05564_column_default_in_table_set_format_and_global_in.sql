-- The set of a column default `p IN table` is built once per reading query also when the defaults are evaluated
-- for the rows a format omits, and when several readers share a `GLOBAL IN` set of a default.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_default_set_format_rhs;
DROP TABLE IF EXISTS t_default_set_global_in;

CREATE TABLE t_default_set_format_rhs (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_format_rhs SELECT number * 2 FROM numbers(100);

-- The records contain only `p`: `f` is evaluated by the format for every block of one row.
INSERT INTO FUNCTION file(currentDatabase() || '_05564.jsonl', 'JSONEachRow', 'p UInt64')
SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

SELECT sum(f), sum(p)
FROM file(currentDatabase() || '_05564.jsonl', 'JSONEachRow', 'p UInt64, f UInt8 DEFAULT p IN t_default_set_format_rhs')
SETTINGS max_block_size = 1, max_threads = 1, input_format_parallel_parsing = 0, log_comment = 'default_set_05564_format';

-- A `GLOBAL IN` default of a column missing in the part, read by several threads.
CREATE TABLE t_default_set_global_in (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_global_in SELECT number FROM numbers(100000);
ALTER TABLE t_default_set_global_in ADD COLUMN f UInt8 DEFAULT p GLOBAL IN t_default_set_format_rhs;

SELECT sum(f) FROM t_default_set_global_in
SETTINGS max_threads = 4, max_block_size = 1000, log_comment = 'default_set_05564_global_in';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'default_set_05564_%'
ORDER BY log_comment;

DROP TABLE t_default_set_global_in;
DROP TABLE t_default_set_format_rhs;

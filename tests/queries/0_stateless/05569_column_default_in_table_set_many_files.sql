-- The set of a column default `p IN table` evaluated by the format is built once per reading query also when
-- the query reads several files, each with its own reader, from one or several streams.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_default_set_many_files_rhs;

CREATE TABLE t_default_set_many_files_rhs (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_many_files_rhs SELECT number * 2 FROM numbers(100);

-- The records contain only `p`: `f` is evaluated by the reader of every file.
INSERT INTO FUNCTION file(currentDatabase() || '_05569_1.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(0, 25) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05569_2.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(25, 25) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05569_3.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(50, 25) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05569_4.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(75, 25) SETTINGS engine_file_truncate_on_insert = 1;

SELECT sum(f), sum(p)
FROM file(currentDatabase() || '_05569_{1..4}.jsonl', 'JSONEachRow', 'p UInt64, f UInt8 DEFAULT p IN t_default_set_many_files_rhs')
SETTINGS max_threads = 1, log_comment = 'default_set_05569_single_stream';

SELECT sum(f), sum(p)
FROM file(currentDatabase() || '_05569_{1..4}.jsonl', 'JSONEachRow', 'p UInt64, f UInt8 DEFAULT p IN t_default_set_many_files_rhs')
SETTINGS max_threads = 4, log_comment = 'default_set_05569_many_streams';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'default_set_05569_%'
ORDER BY log_comment;

DROP TABLE t_default_set_many_files_rhs;

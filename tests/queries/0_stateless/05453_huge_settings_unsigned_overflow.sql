-- Huge values of settings must not lead to unsigned integer overflows (checked by UBSan).

DROP TABLE IF EXISTS t_marks;
CREATE TABLE t_marks (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_marks VALUES (1), (2);
SELECT sum(x) FROM t_marks SETTINGS max_threads = 2, merge_tree_min_read_task_size = 18446744073709551615;
SELECT x FROM t_marks ORDER BY x SETTINGS max_threads = 2, optimize_read_in_order = 1, merge_tree_min_read_task_size = 18446744073709551615;
DROP TABLE t_marks;

SELECT * FROM format(BSONEachRow, 'x Int32', unhex('0c0000001078000100000000'))
SETTINGS max_threads = 1, max_parsing_threads = 2, input_format_parallel_parsing = 1,
    min_chunk_bytes_for_parallel_parsing = 18446744073709551615, max_memory_usage = 0, max_memory_usage_for_user = 0;

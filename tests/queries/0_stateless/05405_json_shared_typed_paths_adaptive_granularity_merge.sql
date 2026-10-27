-- A merge whose later blocks have much wider rows than the open granule: the writer closes the granule with the rows
-- written so far, and the typed shared data must complete it without a data call.
DROP TABLE IF EXISTS t_shared;
DROP TABLE IF EXISTS t_memory;

CREATE TABLE t_shared (x UInt64, json JSON(SHARED a Int64, SHARED s String)) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1000, index_granularity_bytes = 8192, merge_max_block_size = 50,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1000000;

INSERT INTO t_shared SELECT number, format('{{"a":{},"s":"x"}}', number) FROM numbers(230);
INSERT INTO t_shared SELECT number + 230, format('{{"a":{},"s":"{}"}}', number, repeat('y', 2000)) FROM numbers(60);
CREATE TABLE t_memory ENGINE = Memory AS SELECT * FROM t_shared;

OPTIMIZE TABLE t_shared FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active;

SELECT count(), sum(json.a), sum(length(json.s)) FROM t_shared;
SELECT count(), sum(json.a), sum(length(json.s)) FROM t_memory;
SELECT count() FROM (SELECT x, json FROM t_shared EXCEPT SELECT x, json FROM t_memory);
SELECT count() FROM (SELECT x, json.a, json.s FROM t_shared EXCEPT SELECT x, json.a, json.s FROM t_memory);
SELECT count() FROM (SELECT x, json.a FROM t_shared WHERE x % 7 = 0 EXCEPT SELECT x, json.a FROM t_memory WHERE x % 7 = 0);
SELECT count() FROM (SELECT x, json.s FROM t_shared SETTINGS max_block_size = 1 EXCEPT SELECT x, json.s FROM t_memory);

DROP TABLE t_shared;
DROP TABLE t_memory;

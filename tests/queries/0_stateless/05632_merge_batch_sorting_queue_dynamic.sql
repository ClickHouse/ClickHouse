-- The result of a merge must not depend on `merge_use_batch_sorting_queue` for `JSON` and `Dynamic` columns, both
-- when the batch queue inserts their rows in batches and when it inserts them one by one, because a value moves
-- between the dynamic paths and the shared data.

DROP TABLE IF EXISTS t_merge_batch_dynamic_off;
DROP TABLE IF EXISTS t_merge_batch_dynamic_on;

CREATE TABLE t_merge_batch_dynamic_off
(
    k UInt64,
    j JSON,
    d Dynamic,
    j2 JSON(max_dynamic_paths = 2)
)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_use_batch_sorting_queue = 0, merge_max_block_size_bytes = 65536, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_merge_batch_dynamic_on AS t_merge_batch_dynamic_off
ENGINE = MergeTree ORDER BY k
SETTINGS merge_use_batch_sorting_queue = 1, merge_max_block_size_bytes = 65536, enable_vertical_merge_algorithm = 0;

SYSTEM STOP MERGES t_merge_batch_dynamic_off;
SYSTEM STOP MERGES t_merge_batch_dynamic_on;

-- Four parts interleaved in runs of 10 rows: all paths and arrays of objects, a subset of the paths, empty objects,
-- and `j2` paths that the merge moves into the shared data.
INSERT INTO t_merge_batch_dynamic_off SELECT intDiv(number, 10) * 40 + number % 10,
    concat('{"a":', toString(number), ',"b":"v', toString(number % 100), '","c":[', toString(number), ',1],"arr":[{"x":', toString(number), '},{"x":1}]}')::JSON,
    if(number % 2, number::Dynamic, concat('s', toString(number))::Dynamic),
    concat('{"p1":', toString(number), ',"p2":', toString(number), '}')::JSON(max_dynamic_paths = 2)
FROM numbers(3000);
INSERT INTO t_merge_batch_dynamic_off SELECT intDiv(number, 10) * 40 + 10 + number % 10,
    concat('{"a":', toString(number), ',"arr":[{"x":', toString(number), '}]}')::JSON,
    number::Dynamic,
    concat('{"p1":', toString(number), '}')::JSON(max_dynamic_paths = 2)
FROM numbers(3000);
INSERT INTO t_merge_batch_dynamic_off SELECT intDiv(number, 10) * 40 + 20 + number % 10, '{}'::JSON, NULL, '{}'::JSON(max_dynamic_paths = 2)
FROM numbers(3000);
INSERT INTO t_merge_batch_dynamic_off SELECT intDiv(number, 10) * 40 + 30 + number % 10,
    concat('{"a":', toString(number), '}')::JSON,
    'str'::Dynamic,
    concat('{"q1":', toString(number), ',"q2":', toString(number), ',"q3":', toString(number), '}')::JSON(max_dynamic_paths = 2)
FROM numbers(3000);

-- Both tables merge identical parts.
ALTER TABLE t_merge_batch_dynamic_on ATTACH PARTITION ID 'all' FROM t_merge_batch_dynamic_off;

SYSTEM START MERGES t_merge_batch_dynamic_off;
SYSTEM START MERGES t_merge_batch_dynamic_on;

OPTIMIZE TABLE t_merge_batch_dynamic_off FINAL;
OPTIMIZE TABLE t_merge_batch_dynamic_on FINAL;

SELECT table, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_merge_batch_dynamic_%' AND active GROUP BY table ORDER BY table;
SELECT (SELECT count() FROM t_merge_batch_dynamic_off), (SELECT count() FROM t_merge_batch_dynamic_on);
SELECT count() FROM
(
    SELECT k, toString(j), toString(d), toString(j2) FROM t_merge_batch_dynamic_off
    EXCEPT SELECT k, toString(j), toString(d), toString(j2) FROM t_merge_batch_dynamic_on
);
SELECT count() FROM
(
    SELECT k, toString(j), toString(d), toString(j2) FROM t_merge_batch_dynamic_on
    EXCEPT SELECT k, toString(j), toString(d), toString(j2) FROM t_merge_batch_dynamic_off
);

DROP TABLE t_merge_batch_dynamic_off;
DROP TABLE t_merge_batch_dynamic_on;

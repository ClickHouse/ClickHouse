-- Tags: no-fasttest
-- - no-fasttest -- S3 is required

-- A vertical merge into a Wide part reads Compact source parts on object storage with a buffer per column,
-- including the empty parts left by a mutation that deleted all their rows.

DROP TABLE IF EXISTS t_compact_multibuffer_empty;

CREATE TABLE t_compact_multibuffer_empty
(
    key UInt64,
    value1 UInt64,
    value2 Date,
    value3 Nullable(UInt64)
)
ENGINE = MergeTree
PRIMARY KEY (key, value1)
ORDER BY (key, value1, value2)
SETTINGS storage_policy = 's3_no_cache',
    min_rows_for_wide_part = 100,
    compact_parts_merge_max_bytes_to_prefetch_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1;

INSERT INTO t_compact_multibuffer_empty SELECT number, number + 10, toDate(2), number FROM numbers(10);
INSERT INTO t_compact_multibuffer_empty SELECT number, number + 10, toDate(2), number FROM numbers(10, 10);
INSERT INTO t_compact_multibuffer_empty SELECT number, number + 10, toDate(2), number FROM numbers(10, 20);
INSERT INTO t_compact_multibuffer_empty SELECT number, number + 10, toDate(2), number FROM numbers(10, 30);

ALTER TABLE t_compact_multibuffer_empty DELETE WHERE value2 = toDate(2) SETTINGS mutations_sync = 1;

INSERT INTO t_compact_multibuffer_empty SELECT number, number + 10, toDate(2), number FROM numbers(10000);

OPTIMIZE TABLE t_compact_multibuffer_empty FINAL;

SELECT count() FROM t_compact_multibuffer_empty WHERE NOT ignore(*);

DROP TABLE t_compact_multibuffer_empty;

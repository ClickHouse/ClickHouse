-- The search functions compare a `FixedString` constant with the names of an `Enum` with all its bytes, including the
-- zero padding, and an enum name can end with a zero byte. The bloom filter index must hash the same enum value.

DROP TABLE IF EXISTS t_enum_zero;

CREATE TABLE t_enum_zero
(
    id UInt64,
    v Array(Enum8('a' = 1, 'a\0' = 2)),
    INDEX idx v TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_enum_zero VALUES (1, ['a']), (2, ['a\0']);

SELECT 'hasAny', id FROM t_enum_zero WHERE hasAny(v, [toFixedString('a', 2)]) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'hasAny', id FROM t_enum_zero WHERE hasAny(v, [toFixedString('a', 2)]) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'hasAll', id FROM t_enum_zero WHERE hasAll(v, [toFixedString('a', 2)]) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'hasAll', id FROM t_enum_zero WHERE hasAll(v, [toFixedString('a', 2)]) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'has', id FROM t_enum_zero WHERE has(v, toFixedString('a', 2)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'has', id FROM t_enum_zero WHERE has(v, toFixedString('a', 2)) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'has 1', id FROM t_enum_zero WHERE has(v, toFixedString('a', 1)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'has 1', id FROM t_enum_zero WHERE has(v, toFixedString('a', 1)) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_enum_zero;

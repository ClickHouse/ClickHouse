-- Index analysis does not strip a cast to `Bool` of a column holding a byte other than 0 and 1: the cast makes it 1.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    x Bool,
    n Nullable(Bool),
    a Array(Bool),
    s SimpleAggregateFunction(any, Bool),
    l LowCardinality(Bool),
    ln LowCardinality(Nullable(Bool)),
    INDEX idx_x x TYPE bloom_filter GRANULARITY 1,
    INDEX idx_n n TYPE bloom_filter GRANULARITY 1,
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1,
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
    INDEX idx_l l TYPE bloom_filter GRANULARITY 1,
    INDEX idx_ln ln TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- Rows with the bytes 0, 1 and 2 in every column, which `RowBinary` input stores as they are.
INSERT INTO tab SELECT * FROM format(RowBinary, 'id UInt32, x Bool, n Nullable(Bool), a Array(Bool), s SimpleAggregateFunction(any, Bool), l LowCardinality(Bool), ln LowCardinality(Nullable(Bool))',
    '\x01\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00' || '\x02\x00\x00\x00\x01\x00\x01\x01\x01\x01\x01\x00\x01' || '\x03\x00\x00\x00\x02\x00\x02\x01\x02\x02\x02\x00\x02');

SELECT id, reinterpret(x, 'UInt8'), reinterpret(assumeNotNull(n), 'UInt8'), arrayMap(e -> reinterpret(e, 'UInt8'), a), reinterpret(s, 'UInt8'), CAST(l, 'UInt8'), CAST(ln, 'Nullable(UInt8)') FROM tab ORDER BY id;

SELECT id FROM tab WHERE CAST(x, 'Bool') = true ORDER BY id;
SELECT id FROM tab WHERE CAST(x, 'Bool') IN (true) ORDER BY id;
SELECT id FROM tab WHERE CAST(x, 'Nullable(Bool)') = true ORDER BY id;
SELECT id FROM tab WHERE CAST(x, 'LowCardinality(Bool)') = true ORDER BY id;
SELECT id FROM tab WHERE CAST(n, 'Bool') = true ORDER BY id;
SELECT id FROM tab WHERE has(CAST(a, 'Array(Nullable(Bool))'), true) ORDER BY id;
SELECT id FROM tab WHERE CAST(s, 'SimpleAggregateFunction(any, Bool)') = true ORDER BY id;
SELECT id FROM tab WHERE CAST(l, 'Bool') = true ORDER BY id;
SELECT id FROM tab WHERE CAST(ln, 'Bool') = true ORDER BY id;

SELECT '-- wrappers and identical types are still looked through';
SELECT id FROM tab WHERE toNullable(x) = false ORDER BY id SETTINGS force_data_skipping_indices = 'idx_x';
SELECT id FROM tab WHERE has(CAST(a, 'Array(Bool)'), false) ORDER BY id SETTINGS force_data_skipping_indices = 'idx_a';

SELECT '-- primary key and set index';
DROP TABLE IF EXISTS tab_key;
CREATE TABLE tab_key
(
    k Bool,
    id UInt32,
    INDEX idx_set k TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO tab_key SELECT * FROM format(RowBinary, 'k Bool, id UInt32', '\x00\x01\x00\x00\x00' || '\x01\x02\x00\x00\x00' || '\x02\x03\x00\x00\x00');

SELECT id FROM tab_key WHERE CAST(k, 'Bool') = true ORDER BY id SETTINGS use_skip_indexes = 0, force_primary_key = 1;
SELECT id FROM tab_key WHERE CAST(k, 'Bool') IN (true) ORDER BY id SETTINGS use_skip_indexes = 0, force_primary_key = 1;
SELECT id FROM tab_key WHERE CAST(k, 'Bool') = true ORDER BY id SETTINGS use_primary_key = 0, force_data_skipping_indices = 'idx_set';
SELECT id FROM tab_key WHERE CAST(k, 'Bool') ORDER BY id SETTINGS force_primary_key = 1;

DROP TABLE tab_key;
DROP TABLE tab;

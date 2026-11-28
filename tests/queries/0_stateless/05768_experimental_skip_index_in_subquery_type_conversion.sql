-- `x IN (subquery)` with a `cuckoo_filter` or `binary_fuse_filter` index: `Set::execute` casts the key to the
-- type of the set, while the index casts the set to the key type. The index is used only when that keeps the
-- stored hash domain, e.g. a `String` key holding `'01'` matches the `UInt64` set element `1`, so such a set
-- must not prune granules. Every query is run with and without the skip indexes, the results must be the same.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;
SET optimize_functions_to_subcolumns = 0;

SELECT 'cuckoo_filter';

DROP TABLE IF EXISTS t_conv;
CREATE TABLE t_conv
(
    id UInt64,
    s String,
    u UInt32,
    a Array(UInt64),
    m Map(String, String),
    INDEX idx_s s TYPE cuckoo_filter GRANULARITY 1,
    INDEX idx_u u TYPE cuckoo_filter GRANULARITY 1,
    INDEX idx_a a TYPE cuckoo_filter GRANULARITY 1,
    INDEX idx_m mapValues(m) TYPE cuckoo_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_conv VALUES (1, '01', 1, [1], {'k': '01'}), (2, 'x', 7, [2], {'k': 'x'});

SELECT 'String key, UInt64 set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT number + 1 FROM numbers(1)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT number + 1 FROM numbers(1)) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Map String values, UInt64 set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE m['k'] IN (SELECT number + 1 FROM numbers(1)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE m['k'] IN (SELECT number + 1 FROM numbers(1)) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Array(UInt64) key, Array(Nullable(UInt64)) set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [NULL]::Array(Nullable(UInt64))) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [NULL]::Array(Nullable(UInt64))) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [1]::Array(Nullable(UInt64))) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [1]::Array(Nullable(UInt64))) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Same type and integer sets still use the index';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT '01') ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT toNullable('x')) ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE u IN (SELECT number FROM numbers(5)) ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_u';

DROP TABLE t_conv;

SELECT 'binary_fuse_filter';

DROP TABLE IF EXISTS t_conv;
CREATE TABLE t_conv
(
    id UInt64,
    s String,
    u UInt32,
    a Array(UInt64),
    m Map(String, String),
    INDEX idx_s s TYPE binary_fuse_filter GRANULARITY 1,
    INDEX idx_u u TYPE binary_fuse_filter GRANULARITY 1,
    INDEX idx_a a TYPE binary_fuse_filter GRANULARITY 1,
    INDEX idx_m mapValues(m) TYPE binary_fuse_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_conv VALUES (1, '01', 1, [1], {'k': '01'}), (2, 'x', 7, [2], {'k': 'x'});

SELECT 'String key, UInt64 set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT number + 1 FROM numbers(1)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT number + 1 FROM numbers(1)) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Map String values, UInt64 set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE m['k'] IN (SELECT number + 1 FROM numbers(1)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE m['k'] IN (SELECT number + 1 FROM numbers(1)) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Array(UInt64) key, Array(Nullable(UInt64)) set';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [NULL]::Array(Nullable(UInt64))) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [NULL]::Array(Nullable(UInt64))) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [1]::Array(Nullable(UInt64))) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE a IN (SELECT [1]::Array(Nullable(UInt64))) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'Same type and integer sets still use the index';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT '01') ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE s IN (SELECT toNullable('x')) ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT groupArray(id) FROM (SELECT id FROM t_conv WHERE u IN (SELECT number FROM numbers(5)) ORDER BY id) SETTINGS force_data_skipping_indices = 'idx_u';

DROP TABLE t_conv;

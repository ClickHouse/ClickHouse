-- Random settings limits: optimize_functions_to_subcolumns=(1, 1)
-- A text index on `mapKeys` stores a `FixedString(N)` key padded to N bytes, so a key is looked up padded too.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_basic;
DROP TABLE IF EXISTS tab_buckets;

CREATE TABLE tab_basic
(
    id UInt32,
    m Map(FixedString(4), UInt8),
    l Map(LowCardinality(FixedString(4)), UInt8),
    INDEX idx_m mapKeys(m) TYPE text(tokenizer = 'array'),
    INDEX idx_l mapKeys(l) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

CREATE TABLE tab_buckets
(
    id UInt32,
    m Map(FixedString(4), UInt8),
    l Map(LowCardinality(FixedString(4)), UInt8),
    INDEX idx_m mapKeys(m) TYPE text(tokenizer = 'array'),
    INDEX idx_l mapKeys(l) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

INSERT INTO tab_basic VALUES (1, {'ab': 1}, {'ab': 1}), (2, {'x': 2}, {'x': 2}), (3, {'ab': 3}, {'ab': 3});
INSERT INTO tab_buckets SELECT * FROM tab_basic;

-- { echoOn }

SELECT id FROM tab_basic WHERE m.key_ab = 1 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_m';
SELECT id FROM tab_basic WHERE l.key_ab = 3 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_l';
SELECT id FROM tab_basic WHERE m['ab'] = 1 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_m';
SELECT id FROM tab_basic WHERE m['ab'] = 1 ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT id FROM tab_buckets WHERE m.key_ab = 1 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_m';
SELECT id FROM tab_buckets WHERE l.key_ab = 3 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_l';
SELECT id FROM tab_buckets WHERE m['ab'] = 1 ORDER BY id SETTINGS force_data_skipping_indices = 'idx_m';
SELECT id FROM tab_buckets WHERE m['ab'] = 1 ORDER BY id SETTINGS use_skip_indexes = 0;

-- { echoOff }

DROP TABLE tab_basic;
DROP TABLE tab_buckets;

-- A key the map does not hold reads the column's own default, which for an `Enum` (its zero, not its lowest
-- member) or a `Date32` (1970-01-01, not 1900-01-01) is not the type default: a text index on `mapKeys` must
-- not skip such rows.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    e Map(String, Enum8('m' = -3, 'n' = 0, 'o' = 5)),
    t Map(String, Tuple(x Enum8('m' = -3, 'n' = 0, 'o' = 5))),
    j Map(String, JSON(x Enum8('m' = -3, 'n' = 0, 'o' = 5))),
    d Map(String, Date32),
    INDEX idx_e mapKeys(e) TYPE text(tokenizer = 'array'),
    INDEX idx_t mapKeys(t) TYPE text(tokenizer = 'array'),
    INDEX idx_j mapKeys(j) TYPE text(tokenizer = 'array'),
    INDEX idx_d mapKeys(d) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

-- Row 2 has no key 'k'.
INSERT INTO tab SELECT 1, map('k', 'm'), map('k', tuple('m')), map('k', '{"x":"m"}'), map('k', toDate32('2000-01-01'))
    UNION ALL SELECT 2, map('other', 'm'), map('other', tuple('m')), map('other', '{"x":"m"}'), map('other', toDate32('2000-01-01'))
    UNION ALL SELECT 3, map('k', 'o'), map('k', tuple('o')), map('k', '{"x":"o"}'), map('k', toDate32('2001-01-01'));

SELECT id FROM tab WHERE e['k'] = 'n' ORDER BY id;
SELECT id FROM tab WHERE e['k'] IN ('n') ORDER BY id;
SELECT id FROM tab WHERE e['k'] != 'm' ORDER BY id;
SELECT id FROM tab WHERE e.key_k = 'n' ORDER BY id;
SELECT id FROM tab WHERE e['k'] = 'm' ORDER BY id;
-- The type default ('m') is probed as well.
SELECT id FROM tab WHERE e['k'] = 'm' ORDER BY id SETTINGS force_data_skipping_indices = 'idx_e'; -- { serverError INDEX_NOT_USED }
SELECT id FROM tab WHERE e['k'] = 'o' ORDER BY id SETTINGS force_data_skipping_indices = 'idx_e';
SELECT id FROM tab WHERE t['k'].x = 'n' ORDER BY id;
SELECT id FROM tab WHERE t['k'] = tuple('n') ORDER BY id;
SELECT id FROM tab WHERE j['k'].x = 'n' ORDER BY id;
SELECT id FROM tab WHERE d['k'] = toDate32('1970-01-01') ORDER BY id;
SELECT id FROM tab WHERE d['k'] = toDate32('1900-01-01') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_d';

DROP TABLE tab;

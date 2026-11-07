-- Tags: no-parallel-replicas
-- A skip index declared on the `keys` / `values` subcolumn of a Map is used for `m['key']` and `mapContains*`
-- like an index on `mapKeys(m)` / `mapValues(m)`. With optimize_functions_to_subcolumns = 1 `m['key']` reads
-- the subcolumn `m.key_<key>`, with 0 it is `arrayElement(m, 'key')`.

SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bloom;
DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_json;

-- Key `k<N>` exists only in granule N (64 rows per granule); the value of key `a` is unique per row.
CREATE TABLE t_bloom
(
    id UInt64,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX ik m.keys TYPE bloom_filter GRANULARITY 1,
    INDEX iv m.values TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bloom SELECT number, map('a', toString(number), concat('k', toString(intDiv(number, 64))), 'x') FROM numbers(6400);
OPTIMIZE TABLE t_bloom FINAL;
SELECT 'bloom_filter parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_bloom' AND active;

SELECT 'bloom_filter';
SELECT count() FROM t_bloom WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bloom WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bloom WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bloom WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bloom WHERE m['k7'] IN ('x', 'y') SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bloom WHERE m['k7'] IN ('x', 'y') SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bloom WHERE m['a'] IN ('100', '200') SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bloom WHERE m['a'] IN ('100', '200') SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bloom WHERE mapContains(m, 'k7') SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bloom WHERE mapContainsValue(m, '100') SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 0;
-- An absent key reads the default value, so the index must not prune.
SELECT count() FROM t_bloom WHERE m['zz'] = '';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_bloom WHERE m['k7'] = 'x' SETTINGS ignore_data_skipping_indices = 'iv')
WHERE explain ILIKE '%Granules:%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_bloom WHERE m['a'] = '100' SETTINGS ignore_data_skipping_indices = 'ik')
WHERE explain ILIKE '%Granules:%';

CREATE TABLE t_text
(
    id UInt64,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX ik m.keys TYPE text(tokenizer = 'array'),
    INDEX iv m.values TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_text SELECT number, map('a', toString(number), concat('k', toString(intDiv(number, 64))), 'x') FROM numbers(6400);
OPTIMIZE TABLE t_text FINAL;
SELECT 'text parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_text' AND active;

SELECT 'text';
SELECT count() FROM t_text WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_text WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_text WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_text WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_text WHERE mapContainsKey(m, 'k7') SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;

CREATE TABLE t_tokenbf
(
    id UInt64,
    m Map(String, String),
    INDEX ik m.keys TYPE tokenbf_v1(4096, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_tokenbf SELECT number, map('a', toString(number), concat('k', toString(intDiv(number, 64))), 'x') FROM numbers(6400);
OPTIMIZE TABLE t_tokenbf FINAL;
SELECT 'tokenbf_v1 parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_tokenbf' AND active;

SELECT 'tokenbf_v1';
SELECT count() FROM t_tokenbf WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_tokenbf WHERE m['k7'] = 'x' SETTINGS force_data_skipping_indices = 'ik', optimize_functions_to_subcolumns = 1;

-- A bloom_filter index on `mapValues(m)` is refused for these values (Array(Nullable(String))), one on `m.values` is not.
CREATE TABLE t_nullable
(
    id UInt64,
    m Map(String, LowCardinality(Nullable(String))),
    INDEX iv m.values TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nullable SELECT number, map('a', toString(number), 'b', if(number % 64 = 0, NULL, 'v')) FROM numbers(6400);
OPTIMIZE TABLE t_nullable FINAL;
SELECT 'nullable parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nullable' AND active;

SELECT 'nullable';
SELECT count() FROM t_nullable WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_nullable WHERE m['a'] = '100' SETTINGS force_data_skipping_indices = 'iv', optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_nullable WHERE m['zz'] = '100';

-- The part builds the index on `j.m.keys` from the typed path `m.keys` of `j`, not from the keys of `j.m`.
CREATE TABLE t_json
(
    id UInt64,
    `j.m` Map(String, String),
    j JSON(`m.keys` Array(String)),
    INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_json SELECT number, map('a', toString(number), concat('k', toString(intDiv(number, 64))), 'x'),
    concat('{"m.keys":["zzz', toString(intDiv(number, 64)), '"]}') FROM numbers(6400);
OPTIMIZE TABLE t_json FINAL;
SELECT 'json parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json' AND active;

SELECT 'json';
SELECT count() FROM t_json WHERE `j.m`['k7'] = 'x' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_json WHERE `j.m`['k7'] = 'x' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_json WHERE `j.m`['k7'] = 'x' SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_bloom;
DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_nullable;
DROP TABLE t_json;

-- An array element out of range and a missing Map key are the default of the element type.
-- For an Enum that is its lowest member, not the column zero.

DROP TABLE IF EXISTS t_enum_elem;
DROP TABLE IF EXISTS t_enum_elem_buckets;
DROP TABLE IF EXISTS t_enum_elem_memory;
DROP TABLE IF EXISTS t_enum_elem_dst;
DROP TABLE IF EXISTS t_enum_elem_idx;
DROP TABLE IF EXISTS t_enum_tuple_idx;
DROP TABLE IF EXISTS t_scalar_tbf_idx;

CREATE TABLE t_enum_elem (id UInt8, arr Array(Enum8('x' = 1, 'y' = 2)), m Map(String, Enum8('x' = 1, 'y' = 2)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_enum_elem VALUES (1, ['y'], {'k': 'y'}), (2, [], {});

SELECT 'table';
SELECT id, arr[1], arr[-1], arr[id + 1], m['k'] FROM t_enum_elem ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'const array';
SELECT [CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')][number + 3] FROM numbers(1);
SELECT [tuple(CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), 1)][number + 3] FROM numbers(1);

SELECT 'tuple';
SELECT arrayElement(materialize([tuple(CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), 1)]), 3);

SELECT 'json';
SELECT materialize([CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')])[3];
SELECT [CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')][3];

SELECT 'array of indexes';
SELECT [CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')][[1, 5]];
SELECT [tuple(CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), 1)][[1, 5]];
SELECT [CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')][[1, 5]];

SELECT 'replicated';
SELECT arr[i] FROM (SELECT materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]) AS arr) ARRAY JOIN [1, 5] AS i SETTINGS enable_lazy_columns_replication = 1;
SELECT arr[i] FROM (SELECT materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]) AS arr) ARRAY JOIN [1, 5] AS i SETTINGS enable_lazy_columns_replication = 0;

SELECT 'map';
SELECT map('a', CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'))[toString(number)] FROM numbers(1);

CREATE TABLE t_enum_elem_buckets
(
    id UInt8,
    m Map(String, Enum8('x' = 1, 'y' = 2)),
    mt Map(String, Tuple(e Enum8('x' = 1, 'y' = 2), n UInt8)),
    mn Map(String, Nullable(Enum8('x' = 1, 'y' = 2)))
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0;
INSERT INTO t_enum_elem_buckets VALUES (1, {'k': 'y'}, {'k': ('y', 1)}, {'k': 'y'}), (2, {}, {}, {});

SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT m['k'] FROM t_enum_elem_buckets SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%m.key_k%';
SELECT id, m['k'], mt['k'] FROM t_enum_elem_buckets ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, m['k'], mt['k'] FROM t_enum_elem_buckets ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

CREATE TABLE t_enum_elem_memory (m Map(String, Enum8('x' = 1, 'y' = 2))) ENGINE = Memory;
INSERT INTO t_enum_elem_memory VALUES ({'k': 'y'}), ({});
SELECT m.key_k FROM t_enum_elem_memory ORDER BY ALL;

SELECT 'lowest member';
SELECT [CAST('y', 'Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)')][5], [CAST('y', 'Enum16(\'a\' = -1000, \'b\' = 0, \'y\' = 2)')][5];

SELECT 'insert select';
CREATE TABLE t_enum_elem_dst (e Enum8('x' = 1, 'y' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_elem_dst SELECT arr[1] FROM t_enum_elem;
SELECT e FROM t_enum_elem_dst ORDER BY ALL;

SELECT 'unchanged';
SELECT arrayElementOrNull([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')], 5);
SELECT map('a', CAST('y', 'Nullable(Enum8(\'x\' = 1, \'y\' = 2))'))['b'];
SELECT [CAST('y', 'Enum8(\'z\' = 0, \'y\' = 2)')][5];
SELECT toInt8(assumeNotNull(arrayElementOrNull(materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]), 5)));
SELECT toInt8(assumeNotNull(materialize([CAST('y', 'Nullable(Enum8(\'x\' = 1, \'y\' = 2))')])[5]));
SELECT toInt8(assumeNotNull(map('a', CAST('y', 'Nullable(Enum8(\'x\' = 1, \'y\' = 2))'))[toString(number)])) FROM numbers(1);
SELECT id, toInt8(assumeNotNull(mn['k'])) FROM t_enum_elem_buckets ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'null index';
SELECT i, toInt8(assumeNotNull(arr[i])), toInt8(assumeNotNull([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')][i])),
    toInt8(assumeNotNull(arr_t[i]).1), toInt8(assumeNotNull(arr_j[i]).a)
FROM
(
    SELECT
        materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]) AS arr,
        materialize([tuple(CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), 1)]) AS arr_t,
        materialize([CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')]) AS arr_j,
        arrayJoin([CAST(NULL, 'Nullable(UInt64)'), 1, 5]) AS i
)
ORDER BY i NULLS FIRST SETTINGS enable_lazy_columns_replication = 0;
SELECT k, toInt8(assumeNotNull(m[k])), toInt8(assumeNotNull(map('a', CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'))[k]))
FROM (SELECT materialize(map('a', CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'))) AS m, arrayJoin([CAST(NULL, 'Nullable(String)'), 'a', 'b']) AS k)
ORDER BY k NULLS FIRST SETTINGS enable_lazy_columns_replication = 0;
SELECT i, toInt8(assumeNotNull(arr[i])) FROM (SELECT materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]) AS arr)
ARRAY JOIN [CAST(NULL, 'Nullable(UInt64)'), 1, 5] AS i ORDER BY i NULLS FIRST SETTINGS enable_lazy_columns_replication = 1;
SELECT i, toInt8(assumeNotNull(arr[i]).1), toInt8(assumeNotNull(arr[5]).1)
FROM (SELECT materialize([CAST(tuple('y', 1), 'Nullable(Tuple(Enum8(\'x\' = 1, \'y\' = 2), UInt8))')]) AS arr, arrayJoin([1, 5]) AS i)
ORDER BY i SETTINGS enable_lazy_columns_replication = 0;
SELECT toInt8(assumeNotNull(m['b']).1) FROM (SELECT materialize(map('a', CAST(tuple('y', 1), 'Nullable(Tuple(Enum8(\'x\' = 1, \'y\' = 2), UInt8))'))) AS m);

SELECT 'tuple with nothing';
SELECT tupleElement(arrayZip(CAST([], 'Array(Enum8(\'a\' = -1, \'z\' = 0))'), CAST([], 'Array(Nothing)'))[number + 1], 1) FROM numbers(1);
SELECT arrayMap(x -> tupleElement(x, 1), arrayZip(CAST([], 'Array(Enum8(\'a\' = -1, \'z\' = 0))'), CAST([], 'Array(Nothing)'))[[number + 1]]) FROM numbers(1);
SELECT tupleElement(m.key_k, 1) FROM (SELECT mapFromArrays(CAST([], 'Array(String)'), arrayZip(CAST([], 'Array(Enum8(\'a\' = -1, \'z\' = 0))'), CAST([], 'Array(Nothing)'))) AS m);

SELECT 'skip index';
CREATE TABLE t_enum_elem_idx
(
    id UInt8,
    m Map(String, Enum8('x' = 1, 'y' = 2)),
    mt Map(String, Tuple(Enum8('x' = 1, 'y' = 2))),
    INDEX i_tbf mapKeys(m) TYPE tokenbf_v1(512, 3, 0),
    INDEX i_bf mapKeys(mt) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0;
INSERT INTO t_enum_elem_idx SELECT 1, map('k', 'y'), map('k', tuple('y')) UNION ALL SELECT 2, map('o', 'y'), map('o', tuple('y'));
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] = 'x' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] = 'x' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE mt['k'] = tuple('x') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE mt['k'] = tuple('x') SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] LIKE 'x%' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] LIKE 'x%' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m[toNullable('k')] = 'x' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m[toNullable('k')] LIKE 'x%' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] = 'y' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_tbf';
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] = 'y' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, force_data_skipping_indices = 'i_tbf';
SELECT groupArray(id) FROM t_enum_elem_idx WHERE m['k'] LIKE 'y%' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_tbf';
SELECT groupArray(id) FROM t_enum_elem_idx WHERE mt['k'] = tuple('y') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_bf';
SELECT groupArray(id) FROM t_enum_elem_idx WHERE mt['k'] = tuple('y') SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, force_data_skipping_indices = 'i_bf';

SELECT 'skip index, tuple';
CREATE TABLE t_enum_tuple_idx
(
    id UInt8,
    m Map(String, Tuple(Enum8('x' = -1, 'z' = 0, 'y' = 2), UInt8)),
    INDEX i_bf mapKeys(m) TYPE bloom_filter,
    INDEX i_txt mapKeys(m) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0;
INSERT INTO t_enum_tuple_idx SELECT 1, map('k', tuple('y', 1)) UNION ALL SELECT 2, map('o', tuple('y', 1));
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('x', 0) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_txt';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('x', 0) SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_txt';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('x', 0) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_bf';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('x', 0) SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_bf';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] IN (SELECT tuple('x', 0)) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_txt';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] IN (SELECT tuple('x', 0)) SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_txt';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('y', 1) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_bf';
SELECT groupArray(id) FROM t_enum_tuple_idx WHERE m['k'] = tuple('y', 1) SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, force_data_skipping_indices = 'i_txt';

SELECT 'skip index, UInt8, Bool and strings';
CREATE TABLE t_scalar_tbf_idx
(
    id UInt8,
    u Map(String, UInt8),
    b Map(String, Bool),
    s Map(String, String),
    f Map(String, FixedString(3)),
    ts Map(String, Tuple(String, Enum8('x' = 1, 'y' = 2))),
    INDEX i_u mapKeys(u) TYPE tokenbf_v1(512, 3, 0),
    INDEX i_b mapKeys(b) TYPE tokenbf_v1(512, 3, 0),
    INDEX i_bbf mapKeys(b) TYPE bloom_filter,
    INDEX i_s mapKeys(s) TYPE tokenbf_v1(512, 3, 0),
    INDEX i_f mapKeys(f) TYPE ngrambf_v1(1, 512, 3, 0),
    INDEX i_sbf mapKeys(s) TYPE bloom_filter,
    INDEX i_fbf mapKeys(f) TYPE bloom_filter,
    INDEX i_tsbf mapKeys(ts) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0;
INSERT INTO t_scalar_tbf_idx VALUES (1, {'k': 1}, {'k': true}, {'k': 'abc'}, {'k': 'abc'}, {'k': ('abc', 'y')}),
    (2, {'o': 1}, {'o': true}, {'o': 'abc'}, {'o': 'abc'}, {'o': ('abc', 'y')});
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE u['k'] = '0' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE u['k'] = '0' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE b['k'] = 'false' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_bbf';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE b['k'] = 'false' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_b';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] LIKE '%' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] LIKE '%' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE match(f['k'], 'a*') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_regexp_functions = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE f['k'] = '\0\0\0' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE startsWith(f['k'], '\0') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_like_perfect_affix = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE endsWith(f['k'], '\0') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_like_perfect_affix = 0;
-- `=` ignores the trailing zeros of a `FixedString`, so these match the missing key too.
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE ts['k'] = tuple(toFixedString('', 3), 'x') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE ts['k'] = tuple(toFixedString('', 3), 'x') SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0;
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] = toFixedString('', 3) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_s';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE f['k'] = '\0\0\0\0' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_fbf';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE f['k'] = '\0\0\0\0' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, ignore_data_skipping_indices = 'i_f';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE u['k'] = '1' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_u';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE b['k'] = 'true' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_b';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE b['k'] = 'true' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_bbf';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] LIKE 'a%' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_s';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] LIKE 'a%' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, force_data_skipping_indices = 'i_s';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE match(f['k'], 'abc') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_regexp_functions = 0, force_data_skipping_indices = 'i_f';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE startsWith(f['k'], 'a') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_like_perfect_affix = 0, force_data_skipping_indices = 'i_f';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE endsWith(f['k'], 'c') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, optimize_rewrite_like_perfect_affix = 0, force_data_skipping_indices = 'i_f';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE ts['k'] = tuple(toFixedString('abc', 3), 'y') SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_tsbf';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE s['k'] = toFixedString('abc', 3) SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_sbf';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE f['k'] = 'abc\0' SETTINGS optimize_functions_to_subcolumns = 0, use_query_condition_cache = 0, force_data_skipping_indices = 'i_f';
SELECT groupArray(id) FROM t_scalar_tbf_idx WHERE f['k'] = 'abc\0' SETTINGS optimize_functions_to_subcolumns = 1, use_query_condition_cache = 0, force_data_skipping_indices = 'i_fbf';

DROP TABLE t_enum_elem;
DROP TABLE t_enum_elem_buckets;
DROP TABLE t_enum_elem_memory;
DROP TABLE t_enum_elem_dst;
DROP TABLE t_enum_elem_idx;
DROP TABLE t_enum_tuple_idx;
DROP TABLE t_scalar_tbf_idx;

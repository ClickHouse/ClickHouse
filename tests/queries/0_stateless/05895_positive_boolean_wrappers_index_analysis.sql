-- Random settings limits: use_query_condition_cache=(0, 0)
-- The analyzer removes `= true`, `!= false`, `IN (true)` and `IS TRUE` around a non-Nullable boolean function,
-- so the primary key, partition and skip indexes see the bare predicate.
-- https://github.com/ClickHouse/ClickHouse/issues/110012, https://github.com/ClickHouse/ClickHouse/issues/79712

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS e_pk;
DROP TABLE IF EXISTS e_pk2;
DROP TABLE IF EXISTS e_part;
DROP TABLE IF EXISTS e_mm;
DROP TABLE IF EXISTS e_bf;
DROP TABLE IF EXISTS e_and;

CREATE TABLE t
(
    k String,
    ps String,
    s String,
    ns Nullable(String),
    arr Array(String),
    INDEX i_tk s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX i_txt ns TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX i_bf arr TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY ps ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;

INSERT INTO t SELECT 'k' || leftPad(toString(number), 2, '0'), if(number < 8, 'a', 'b'), 'w' || toString(number),
    if(number % 2 = 0, 'w' || toString(number), NULL), ['f' || toString(number)] FROM numbers(16);

-- Selected parts and granules. Plan-based parallel replicas prefix these EXPLAIN lines with `│`.
SELECT 'pk bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(k, 'k05')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk = true', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(k, 'k05') = true) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk != false', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(k, 'k05') != false) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk IN (true)', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(k, 'k05') IN (true)) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk IS TRUE', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(k, 'k05') IS TRUE) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk LIKE = true', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE (k LIKE 'k05%') = true) WHERE explain LIKE '%Parts: % | Granules: %';

-- The partition key's Min-Max index is the first `Parts:` line after the summary.
SELECT 'partition bare', arrayStringConcat(groupArray(trimBoth(replaceAll(explain, '│', ''))), ', ') FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(ps, 'b')) WHERE explain LIKE '%Parts: %';
SELECT 'partition = true', arrayStringConcat(groupArray(trimBoth(replaceAll(explain, '│', ''))), ', ') FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE startsWith(ps, 'b') = true) WHERE explain LIKE '%Parts: %';

SELECT 'tokenbf bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE hasToken(s, 'w5')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'tokenbf = true', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE hasToken(s, 'w5') = true) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'tokenbf != false', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE hasToken(s, 'w5') != false) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'tokenbf IN (true)', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE hasToken(s, 'w5') IN (true)) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'tokenbf IS TRUE', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE hasToken(s, 'w5') IS TRUE) WHERE explain LIKE '%Parts: % | Granules: %';

SELECT 'bloom_filter bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE has(arr, 'f5')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE has(arr, 'f5') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter = true', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE has(arr, 'f5') = true) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter = 0', count() FROM t WHERE has(arr, 'f5') = 0 SETTINGS use_skip_indexes = 1;
SELECT 'bloom_filter = 0 no index', count() FROM t WHERE has(arr, 'f5') = 0 SETTINGS use_skip_indexes = 0;

-- A Nullable predicate keeps its wrapper.
SELECT 'nullable = true', count() FROM (EXPLAIN QUERY TREE SELECT k FROM t WHERE hasToken(ns, 'w4') = true) WHERE explain LIKE '%function_name: equals%';
SELECT 'nullable values', countIf(hasToken(ns, 'w4') = true), countIf(hasToken(ns, 'w4') != false), countIf(hasToken(ns, 'w4') IN (true)), countIf(hasToken(ns, 'w4') IS TRUE), countIf(isNull(hasToken(ns, 'w4') IS TRUE)) FROM t;
SELECT 'nullable null_in', countIf(hasToken(ns, 'w4') IN (true)) FROM t SETTINGS transform_null_in = 1;
SELECT 'nullable direct read', count() FROM t WHERE (hasToken(ns, 'w4') GLOBAL IN (true)) OR isNull(hasToken(ns, 'w4'))
    SETTINGS query_plan_direct_read_from_text_index = 1, optimize_move_to_prewhere = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'nullable no direct read', count() FROM t WHERE (hasToken(ns, 'w4') GLOBAL IN (true)) OR isNull(hasToken(ns, 'w4'))
    SETTINGS query_plan_direct_read_from_text_index = 0, optimize_move_to_prewhere = 0, query_plan_optimize_count_from_text_index = 0;

CREATE TABLE m (x Nullable(UInt8), y UInt8, z UInt8) ENGINE = Memory;
INSERT INTO m VALUES (NULL, 0, 0);
-- `IS TRUE` turns NULL into 0, so `intDiv` is not evaluated for that row.
SELECT 'nullable short circuit', count() FROM m WHERE ((x = 1) IS TRUE AND intDiv(1, y)) OR z;

-- The rewrite returns the same values in the SELECT list.
SELECT 'select list wrappers', count() FROM (EXPLAIN QUERY TREE SELECT hasToken(s, 'w5') != false, hasToken(s, 'w5') IN (true), hasToken(s, 'w5') IS TRUE FROM t)
    WHERE explain LIKE '%function_name: notEquals%' OR explain LIKE '%function_name: in,%' OR explain LIKE '%function_name: isNotDistinctFrom%';
SELECT 'values positive',
    groupArray(hasToken(s, 'w5') = true) = groupArray(hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') != false) = groupArray(hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') IN (true)) = groupArray(hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') IS TRUE) = groupArray(hasToken(s, 'w5'))
FROM t;
SELECT 'values negative',
    groupArray(hasToken(s, 'w5') = false) = groupArray(NOT hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') != true) = groupArray(NOT hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') IN (false)) = groupArray(NOT hasToken(s, 'w5')),
    groupArray(hasToken(s, 'w5') IS FALSE) = groupArray(NOT hasToken(s, 'w5'))
FROM t;
SELECT 'values null_in', groupArray(hasToken(s, 'w5') IN (true)) = groupArray(hasToken(s, 'w5')) FROM t SETTINGS transform_null_in = 1;
-- `and` over a `Bool` returns `Bool`; the new forms keep their `UInt8` result.
SELECT 'bool result', (materialize(true) AND number = 0) != false, (materialize(true) AND number = 0) IN (true), (materialize(true) AND number = 0) IS TRUE FROM numbers(1);
SELECT 'global not null in', count() FROM (EXPLAIN QUERY TREE SELECT (number GLOBAL NOT IN (1, 2)) != false FROM numbers(3) SETTINGS transform_null_in = 1) WHERE explain LIKE '%function_name: notEquals%';

-- With `group_by_use_nulls` the keys become Nullable, and the rewrite keeps their types.
SELECT (2 = dummy) IS TRUE GROUP BY GROUPING SETS ((1)) SETTINGS group_by_use_nulls = 1;
SELECT (2 = dummy) != false GROUP BY GROUPING SETS ((1)) SETTINGS group_by_use_nulls = 1;
SELECT (2 = dummy) IN (true) GROUP BY GROUPING SETS ((1)) SETTINGS group_by_use_nulls = 1;
SELECT (dummy = 0) IS TRUE, (dummy = 0) != false, (dummy = 0) IN (true) FROM system.one GROUP BY dummy = 0 WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1;

-- A key expression that is itself the predicate is used by the bare predicate and by its wrapped form.
CREATE TABLE e_pk (url String) ENGINE = MergeTree ORDER BY startsWith(url, 'https')
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_pk SELECT if(number < 4, 'https://a', 'http://b') FROM numbers(16);
CREATE TABLE e_pk2 (url String) ENGINE = MergeTree ORDER BY (startsWith(url, 'https'), url)
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_pk2 SELECT if(number < 4, 'https://a', 'http://b') FROM numbers(16);
CREATE TABLE e_part (arr Array(String)) ENGINE = MergeTree PARTITION BY has(arr, 'x') ORDER BY tuple()
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_part SELECT if(number < 4, ['x'], ['y']) FROM numbers(16);
CREATE TABLE e_mm (arr Array(String), INDEX i_mm has(arr, 'x') TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_mm SELECT if(number < 4, ['x'], ['y']) FROM numbers(16);

SELECT 'pk expr bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT url FROM e_pk WHERE startsWith(url, 'https')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk expr = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT url FROM e_pk WHERE startsWith(url, 'https') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk2 expr bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT url FROM e_pk2 WHERE startsWith(url, 'https')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'pk2 expr = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT url FROM e_pk2 WHERE startsWith(url, 'https') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'partition expr bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_part WHERE has(arr, 'x')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'partition expr = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_part WHERE has(arr, 'x') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'minmax expr bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_mm WHERE has(arr, 'x')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'minmax expr = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_mm WHERE has(arr, 'x') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'expr values', (SELECT count() FROM e_pk WHERE startsWith(url, 'https')), (SELECT count() FROM e_pk2 WHERE startsWith(url, 'https')), (SELECT count() FROM e_part WHERE has(arr, 'x')), (SELECT count() FROM e_mm WHERE has(arr, 'x'));
-- A 0/1 function that is a `bloom_filter` index expression; `toUInt8(length(arr))` (1 or 2) is not read as `= 1`.
CREATE TABLE e_bf (arr Array(String), INDEX i_bf has(arr, 'x') TYPE bloom_filter GRANULARITY 1, INDEX i_len toUInt8(length(arr)) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_bf SELECT if(number < 4, ['x'], ['y', 'z']) FROM numbers(16);
SELECT 'bloom_filter expr bare', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_bf WHERE has(arr, 'x')) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter expr = 1', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_bf WHERE has(arr, 'x') = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter expr IN (true)', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT arr FROM e_bf WHERE has(arr, 'x') IN (true)) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'bloom_filter expr values', (SELECT count() FROM e_bf WHERE has(arr, 'x')), (SELECT count() FROM e_bf WHERE has(arr, 'x') = 0), (SELECT count() FROM e_bf WHERE toUInt8(length(arr)));
-- `and` keeps the new wrappers, so a key on the whole `a AND b` still matches the forms index analysis does not unwrap.
CREATE TABLE e_and (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY (a AND b)
SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;
INSERT INTO e_and SELECT number < 4, number < 4 FROM numbers(16);
SELECT 'and expr GLOBAL IN (true)', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT a FROM e_and WHERE (a AND b) GLOBAL IN (true)) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'and expr null_in', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT a FROM e_and WHERE (a AND b) IN (true) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'and expr != true', trimBoth(replaceAll(explain, '│', '')) FROM (EXPLAIN indexes = 1 SELECT a FROM e_and WHERE (a AND b) != true) WHERE explain LIKE '%Parts: % | Granules: %';
SELECT 'and expr values', (SELECT count() FROM e_and WHERE (a AND b) GLOBAL IN (true)), (SELECT count() FROM e_and WHERE (a AND b) IN (true) SETTINGS transform_null_in = 1), (SELECT count() FROM e_and WHERE (a AND b) != true), (SELECT count() FROM e_and WHERE (a AND b) IS TRUE);

DROP TABLE t;
DROP TABLE m;
DROP TABLE e_pk;
DROP TABLE e_pk2;
DROP TABLE e_part;
DROP TABLE e_mm;
DROP TABLE e_bf;
DROP TABLE e_and;

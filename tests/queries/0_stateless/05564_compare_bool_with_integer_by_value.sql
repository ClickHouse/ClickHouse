-- Comparisons of `Bool` with integers inside arrays, maps, JSON, under `<=>` and in `has`-like functions do not depend on the operand order or on index analysis.

SET allow_suspicious_low_cardinality_types = 1;
SELECT 'arr eq', CAST([true], 'Array(Nullable(Bool))') = [toUInt8(2)], [toUInt8(2)] = CAST([true], 'Array(Nullable(Bool))');
SELECT 'arr ne', CAST([true], 'Array(Nullable(Bool))') != [toUInt8(2)], [toUInt8(2)] != CAST([true], 'Array(Nullable(Bool))');
SELECT 'arr lt', CAST([true], 'Array(Nullable(Bool))') < [toUInt8(2)], [toUInt8(2)] > CAST([true], 'Array(Nullable(Bool))');
SELECT 'arr ndf', CAST([true], 'Array(Nullable(Bool))') <=> [toUInt8(2)], [toUInt8(2)] <=> CAST([true], 'Array(Nullable(Bool))');
SELECT 'arr df', isDistinctFrom(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), isDistinctFrom([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))'));
SELECT 'arr lc', CAST([true], 'Array(LowCardinality(Nullable(Bool)))') = [toUInt8(2)], [toUInt8(2)] = CAST([true], 'Array(LowCardinality(Nullable(Bool)))');
SELECT 'arr arr', CAST([[true]], 'Array(Array(Nullable(Bool)))') = [[toUInt8(2)]], [[toUInt8(2)]] = CAST([[true]], 'Array(Array(Nullable(Bool)))');
SELECT 'arr tuple', CAST([(true, 1)], 'Array(Tuple(Nullable(Bool), UInt8))') = [(toUInt8(2), toUInt8(1))], [(toUInt8(2), toUInt8(1))] = CAST([(true, 1)], 'Array(Tuple(Nullable(Bool), UInt8))');
SELECT 'tuple arr', CAST(([true], 1), 'Tuple(Array(Nullable(Bool)), UInt8)') = ([toUInt8(2)], toUInt8(1)), ([toUInt8(2)], toUInt8(1)) = CAST(([true], 1), 'Tuple(Array(Nullable(Bool)), UInt8)');
SELECT 'map eq', CAST(map('a', true), 'Map(String, Nullable(Bool))') = map('a', toUInt8(2)), map('a', toUInt8(2)) = CAST(map('a', true), 'Map(String, Nullable(Bool))');
SELECT 'map lt', CAST(map('a', true), 'Map(String, Nullable(Bool))') < map('a', toUInt8(2)), map('a', toUInt8(2)) > CAST(map('a', true), 'Map(String, Nullable(Bool))');
SELECT 'map key', CAST(map(true, 'x'), 'Map(Bool, Nullable(String))') = map(toUInt8(2), 'x'), map(toUInt8(2), 'x') = CAST(map(true, 'x'), 'Map(Bool, Nullable(String))');
SELECT 'tuple named', CAST([tuple(true)], 'Array(Tuple(a Nullable(Bool)))') = CAST([tuple(2)], 'Array(Tuple(UInt8))'), CAST([tuple(2)], 'Array(Tuple(UInt8))') = CAST([tuple(true)], 'Array(Tuple(a Nullable(Bool)))');
SELECT 'json', CAST('{"a":true}', 'JSON(a Nullable(Bool))') = CAST('{"a":2}', 'JSON(a UInt8)'), CAST('{"a":2}', 'JSON(a UInt8)') = CAST('{"a":true}', 'JSON(a Nullable(Bool))');
SELECT 'json arr', CAST('{"a":[true]}', 'JSON(a Array(Nullable(Bool)))') = CAST('{"a":[2]}', 'JSON(a Array(UInt8))'), CAST('{"a":[2]}', 'JSON(a Array(UInt8))') = CAST('{"a":[true]}', 'JSON(a Array(Nullable(Bool)))');
SELECT 'map json', CAST(map('k', '{"a":true}'), 'Map(String, JSON(a Nullable(Bool)))') = CAST(map('k', '{"a":2}'), 'Map(String, JSON(a UInt8))'), CAST(map('k', '{"a":2}'), 'Map(String, JSON(a UInt8))') = CAST(map('k', '{"a":true}'), 'Map(String, JSON(a Nullable(Bool)))');
SELECT 'scalar ndf', CAST(true, 'Nullable(Bool)') <=> toUInt8(2), toUInt8(2) <=> CAST(true, 'Nullable(Bool)');
SELECT 'scalar df', isDistinctFrom(CAST(true, 'Nullable(Bool)'), toUInt8(2)), isDistinctFrom(toUInt8(2), CAST(true, 'Nullable(Bool)'));
SELECT 'scalar ndf lc', CAST(true, 'LowCardinality(Nullable(Bool))') <=> toUInt8(2), toUInt8(2) <=> CAST(true, 'LowCardinality(Nullable(Bool))');
SELECT 'tuple ndf', CAST(tuple(true), 'Tuple(Nullable(Bool))') <=> tuple(toUInt8(2)), tuple(toUInt8(2)) <=> CAST(tuple(true), 'Tuple(Nullable(Bool))');
SELECT 'keep 0/1', CAST([true], 'Array(Nullable(Bool))') = [toUInt8(1)], [toUInt8(0)] = CAST([false], 'Array(Nullable(Bool))'), CAST(true, 'Nullable(Bool)') <=> toUInt8(1), CAST(NULL, 'Nullable(Bool)') <=> CAST(NULL, 'Nullable(UInt8)'), CAST([NULL], 'Array(Nullable(Bool))') = [toUInt8(2)];
SELECT 'keep bool', CAST([true], 'Array(Nullable(Bool))') = CAST([true], 'Array(Bool)'), CAST([false], 'Array(Nullable(Bool))') < CAST([true], 'Array(Bool)'), CAST(true, 'Nullable(Bool)') <=> true;
SELECT 'keep json', CAST('{"a":true}', 'JSON(a Nullable(Bool))') = CAST('{"a":1}', 'JSON(a UInt8)'), CAST([tuple(true)], 'Array(Tuple(a Nullable(Bool)))') = CAST([tuple(1)], 'Array(Tuple(UInt8))');
SELECT 'keep variant', CAST(map('a', true), 'Map(String, Variant(Bool, String))') = CAST(map('a', true), 'Map(LowCardinality(String), Variant(Bool, String))');
SELECT 'keep lc', CAST([true], 'Array(LowCardinality(Bool))') = [toUInt8(2)], [toUInt8(2)] = CAST([true], 'Array(LowCardinality(Bool))'), CAST([true], 'Array(Bool)') = CAST([2], 'Array(Nullable(UInt8))'), CAST([2], 'Array(Nullable(UInt8))') = CAST([true], 'Array(Bool)');
SELECT 'cols', countIf(a = b), countIf(b = a), countIf(a < b), countIf(b > a), countIf(a <=> b), countIf(b <=> a)
FROM (SELECT CAST([number % 2 = 1], 'Array(Nullable(Bool))') AS a, [toUInt8(number)] AS b FROM numbers(4));
SELECT 'cols scalar ndf', countIf(a <=> b), countIf(b <=> a) FROM (SELECT CAST(number % 2 = 1, 'Nullable(Bool)') AS a, toUInt8(number) AS b FROM numbers(4));
DROP TABLE IF EXISTS t_bool_arr;
DROP TABLE IF EXISTS t_uint_arr;
CREATE TABLE t_bool_arr (v Array(Nullable(Bool))) ENGINE = Memory; INSERT INTO t_bool_arr VALUES ([true]);
CREATE TABLE t_uint_arr (v Array(UInt8)) ENGINE = Memory; INSERT INTO t_uint_arr VALUES ([2]);
SELECT 'cross', countIf(a.v = b.v), countIf(b.v = a.v) FROM t_bool_arr AS a CROSS JOIN t_uint_arr AS b;
DROP TABLE t_bool_arr;
DROP TABLE t_uint_arr;
SELECT 'tuple variant', CAST([(true, 'x')], 'Array(Tuple(Nullable(Bool), Variant(Bool, String)))') = CAST([(2, 'x')], 'Array(Tuple(UInt8, Variant(Bool, String)))'), CAST([(2, 'x')], 'Array(Tuple(UInt8, Variant(Bool, String)))') = CAST([(true, 'x')], 'Array(Tuple(Nullable(Bool), Variant(Bool, String)))');
SELECT 'keep tuple variant', CAST([(true, 'x')], 'Array(Tuple(Nullable(Bool), Variant(Bool, String)))') = CAST([(1, 'x')], 'Array(Tuple(UInt8, Variant(Bool, String)))');
SELECT 'exists', arrayExists(x -> x = [toUInt8(2)], [CAST([true], 'Array(Nullable(Bool))')]) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'has col', has(a, [toUInt8(2)]), indexOf(a, [toUInt8(2)]), countEqual(a, [toUInt8(2)]), has(b, CAST([true], 'Array(Nullable(Bool))')) FROM (SELECT materialize([CAST([true], 'Array(Nullable(Bool))')]) AS a, materialize([[toUInt8(2)]]) AS b);
SELECT 'exists col', arrayExists(x -> x = [toUInt8(2)], a) FROM (SELECT materialize([CAST([true], 'Array(Nullable(Bool))')]) AS a) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT 'exists col noopt', arrayExists(x -> x = [toUInt8(2)], a) FROM (SELECT materialize([CAST([true], 'Array(Nullable(Bool))')]) AS a) SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT 'exists col plan', countIf(explain ILIKE '%has(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT arrayExists(x -> x = [toUInt8(2)], a) FROM (SELECT materialize([CAST([true], 'Array(Nullable(Bool))')]) AS a) SETTINGS optimize_rewrite_array_exists_to_has = 1);
SELECT 'has any', hasAny(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), hasAny([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))')), hasAll(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), hasAll([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))')), hasSubstr(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), hasSubstr([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))'));
SELECT 'keep has', has(a, [toUInt8(1)]), hasAny(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(1)]), hasAll(CAST([true, false], 'Array(Nullable(Bool))'), [toUInt8(0)]) FROM (SELECT materialize([CAST([true], 'Array(Nullable(Bool))')]) AS a);
SELECT 'starts ends', startsWith(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), startsWith([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))')), endsWith(CAST([true], 'Array(Nullable(Bool))'), [toUInt8(2)]), endsWith([toUInt8(2)], CAST([true], 'Array(Nullable(Bool))'));
SELECT 'map remove', mapRemove(CAST(map([true], 'x'), 'Map(Array(Nullable(Bool)), String)'), [toUInt8(2)]), mapRemove(CAST(map(tuple(true), 'x'), 'Map(Tuple(Nullable(Bool)), String)'), tuple(toUInt8(2)));
SELECT 'keep starts remove', startsWith(CAST([true, false], 'Array(Nullable(Bool))'), [toUInt8(1)]), endsWith(CAST([true, false], 'Array(Nullable(Bool))'), [toUInt8(0)]), mapRemove(CAST(map([true], 'x', [false], 'y'), 'Map(Array(Nullable(Bool)), String)'), [toUInt8(1)]);

DROP TABLE IF EXISTS t_key;
CREATE TABLE t_key (arr Array(Nullable(Bool))) ENGINE = MergeTree ORDER BY arr SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_key SELECT [number % 2 = 1] FROM numbers(3000);
SELECT 'key lt', count() FROM t_key WHERE arr < [toUInt8(2)] SETTINGS use_primary_key = 1;
SELECT 'key lt noidx', count() FROM t_key WHERE arr < [toUInt8(2)] SETTINGS use_primary_key = 0;
SELECT 'key eq', count() FROM t_key WHERE arr = [toUInt8(2)] SETTINGS use_primary_key = 1;
SELECT 'key eq noidx', count() FROM t_key WHERE arr = [toUInt8(2)] SETTINGS use_primary_key = 0;
SELECT 'key lt plan', countIf(explain ILIKE '%Condition: (arr in (-Inf, [2]))%') FROM (EXPLAIN indexes = 1 SELECT arr FROM t_key WHERE arr < [toUInt8(2)] SETTINGS use_primary_key = 1, enable_parallel_replicas = 0);
DROP TABLE t_key;

DROP TABLE IF EXISTS t_key_expr;
CREATE TABLE t_key_expr (arr Array(Nullable(Bool))) ENGINE = MergeTree ORDER BY materialize(arr) SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_key_expr SELECT [number % 2 = 1, true] FROM numbers(3000);
SELECT 'expr key', (SELECT count() FROM t_key_expr WHERE arr = [toUInt8(2), toUInt8(1)]), (SELECT count() FROM t_key_expr WHERE arr != [toUInt8(2), toUInt8(1)]), (SELECT count() FROM t_key_expr WHERE arr < [toUInt8(2), toUInt8(0)]) SETTINGS use_primary_key = 1;
SELECT 'expr key keep plan', countIf(explain ILIKE '%Condition: (materialize(arr) in [[1, 1], [1, 1]])%') FROM (EXPLAIN indexes = 1 SELECT arr FROM t_key_expr WHERE arr = [toUInt8(1), toUInt8(1)] SETTINGS use_primary_key = 1, enable_parallel_replicas = 0);
DROP TABLE t_key_expr;

DROP TABLE IF EXISTS t_key_expr_scalar;
CREATE TABLE t_key_expr_scalar (b Nullable(Bool)) ENGINE = MergeTree ORDER BY materialize(b) SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_key_expr_scalar SELECT number % 2 = 1 FROM numbers(3000);
SELECT 'expr key scalar', (SELECT count() FROM t_key_expr_scalar WHERE b != toUInt8(2)), (SELECT count() FROM t_key_expr_scalar WHERE b > toInt8(-1)) SETTINGS use_primary_key = 1;
SELECT 'expr key string keep plan', countIf(explain ILIKE '%Condition: (materialize(b) in [1, 1])%') FROM (EXPLAIN indexes = 1 SELECT b FROM t_key_expr_scalar WHERE b = 'true' SETTINGS use_primary_key = 1, enable_parallel_replicas = 0);
DROP TABLE t_key_expr_scalar;

DROP TABLE IF EXISTS t_key_expr_tuple;
CREATE TABLE t_key_expr_tuple (v Tuple(Bool, Int64)) ENGINE = MergeTree ORDER BY materialize(v) SETTINGS index_granularity = 8;
INSERT INTO t_key_expr_tuple SELECT tuple(number % 2 = 1, toInt64(1)) FROM numbers(3000);
SELECT 'expr key tuple', (SELECT count() FROM t_key_expr_tuple WHERE v < tuple(toUInt8(2), toUInt64(0))) SETTINGS use_primary_key = 1;
SELECT 'expr key tuple keep plan', countIf(explain ILIKE '%Condition: (materialize(v) in (-Inf, (1, 0)])%') FROM (EXPLAIN indexes = 1 SELECT v FROM t_key_expr_tuple WHERE v < tuple(toUInt8(1), toUInt64(0)) SETTINGS use_primary_key = 1, enable_parallel_replicas = 0);
DROP TABLE t_key_expr_tuple;

DROP TABLE IF EXISTS t_key_has;
CREATE TABLE t_key_has (arr Array(Nullable(Bool))) ENGINE = MergeTree ORDER BY arr SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO t_key_has SELECT [number % 2 = 1] FROM numbers(3000);
SELECT 'key has', (SELECT count() FROM t_key_has WHERE has([[toUInt8(2)]], arr)), (SELECT count() FROM t_key_has WHERE NOT has([[toUInt8(2)]], arr)) SETTINGS use_primary_key = 1, optimize_rewrite_has_to_in = 0;
SELECT 'key not has keep plan', countIf(match(explain, 'Granules: \\d+/') AND toUInt32(extract(explain, 'Granules: (\\d+)/')) < toUInt32(extract(explain, 'Granules: \\d+/(\\d+)'))) FROM (EXPLAIN indexes = 1 SELECT arr FROM t_key_has WHERE NOT has([[toUInt8(1)]], arr) SETTINGS use_primary_key = 1, optimize_rewrite_has_to_in = 0, enable_parallel_replicas = 0);
DROP TABLE t_key_has;

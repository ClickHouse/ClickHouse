-- `toString` renders every non-zero byte of a `Bool` as `true`, so it is not injective for it.
-- A `Bool` holds other bytes after `INSERT SELECT` of a `UInt8`, a `SimpleAggregateFunction(f, Bool)` also after `VALUES`.

DROP TABLE IF EXISTS t_saf;
DROP TABLE IF EXISTS t_saf_max;
DROP TABLE IF EXISTS t_saf_skip;
DROP TABLE IF EXISTS t_saf_part;
DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_bool_key;
DROP TABLE IF EXISTS t_saf_nullable;
DROP TABLE IF EXISTS t_saf_array;
DROP TABLE IF EXISTS t_array_saf;
DROP TABLE IF EXISTS t_ctl;

CREATE TABLE t_saf (k UInt8, b SimpleAggregateFunction(anyLast, Bool))
ENGINE = AggregatingMergeTree ORDER BY (toString(b), k) SETTINGS index_granularity = 1;
INSERT INTO t_saf VALUES (1, 2), (2, 1), (3, 0);

SELECT 'saf bytes', arraySort(groupArray(reinterpretAsUInt8(b))) FROM t_saf;
SELECT 'saf not in', (SELECT count() FROM t_saf WHERE b NOT IN (1)), (SELECT countIf(b NOT IN (1)) FROM t_saf);
SELECT 'saf not has', (SELECT count() FROM t_saf WHERE NOT has([toUInt8(1)], b)), (SELECT countIf(NOT has([toUInt8(1)], b)) FROM t_saf);
SELECT 'saf in', (SELECT count() FROM t_saf WHERE b IN (2)), (SELECT countIf(b IN (2)) FROM t_saf);
SELECT 'saf in uses key', b FROM t_saf WHERE b IN (2) SETTINGS force_primary_key = 1;
SELECT 'saf group by', count() FROM (SELECT 1 FROM t_saf GROUP BY toString(b)) SETTINGS optimize_injective_functions_in_group_by = 1;
SELECT 'saf uniq', uniqExact(toString(b)) FROM t_saf SETTINGS optimize_injective_functions_inside_uniq = 1;
SELECT 'saf limit by', count() FROM (SELECT b FROM t_saf LIMIT 1 BY toString(b)) SETTINGS optimize_injective_functions_in_limit_by = 1;
SELECT 'saf concat', count() FROM (SELECT 1 FROM t_saf GROUP BY concat(b)) SETTINGS optimize_injective_functions_in_group_by = 1;

-- 0/1 data: the literal 2 stays 2 in the set and renders as `true`.
CREATE TABLE t_saf_max (b SimpleAggregateFunction(max, Bool)) ENGINE = MergeTree ORDER BY toString(b) SETTINGS index_granularity = 1;
INSERT INTO t_saf_max VALUES (1), (0);
SELECT 'saf max not in', count() FROM t_saf_max WHERE b NOT IN (2);

CREATE TABLE t_saf_skip (k UInt8, b SimpleAggregateFunction(anyLast, Bool), INDEX i toString(b) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_saf_skip VALUES (1, 1), (2, 0);
SELECT 'saf skip index not in', count() FROM t_saf_skip WHERE b NOT IN (2) SETTINGS use_skip_indexes = 1;

CREATE TABLE t_saf_part (b SimpleAggregateFunction(anyLast, Bool)) ENGINE = MergeTree PARTITION BY toString(b) ORDER BY tuple();
INSERT INTO t_saf_part VALUES (1), (0);
SELECT 'saf partition key not in', count() FROM t_saf_part WHERE b NOT IN (2);

CREATE TABLE t_bool (k UInt8, b Bool) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_bool SELECT number, [toUInt8(2), 1, 0][number + 1] FROM numbers(3);
SELECT 'bool bytes', arraySort(groupArray(reinterpretAsUInt8(b))) FROM t_bool;
SELECT 'bool group by', count() FROM (SELECT 1 FROM t_bool GROUP BY toString(b)) SETTINGS optimize_injective_functions_in_group_by = 1;
SELECT 'bool uniq', uniqExact(toString(b)) FROM t_bool SETTINGS optimize_injective_functions_inside_uniq = 1;

CREATE TABLE t_bool_key (b Bool) ENGINE = MergeTree ORDER BY toString(b) SETTINGS index_granularity = 1;
INSERT INTO t_bool_key VALUES (true), (false);
SELECT 'bool key not in', count() FROM t_bool_key WHERE b NOT IN (2);

CREATE TABLE t_saf_nullable (k UInt8, b SimpleAggregateFunction(anyLast, Nullable(Bool))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_saf_nullable SELECT number, [toNullable(toUInt8(2)), 1, 0][number + 1] FROM numbers(3);
SELECT 'saf nullable bytes', arraySort(groupArray(reinterpretAsUInt8(assumeNotNull(b)))) FROM t_saf_nullable;
SELECT 'saf nullable group by', count() FROM (SELECT 1 FROM t_saf_nullable GROUP BY toString(b)) SETTINGS optimize_injective_functions_in_group_by = 1;

CREATE TABLE t_saf_array (k UInt8, b SimpleAggregateFunction(anyLast, Array(Bool))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_saf_array SELECT number, [[toUInt8(2)], [toUInt8(1)], [toUInt8(0)]][number + 1] FROM numbers(3);
SELECT 'saf array bytes', arraySort(groupArray(arrayMap(x -> reinterpretAsUInt8(x), b))) FROM t_saf_array;
SELECT 'saf array group by', count() FROM (SELECT 1 FROM t_saf_array GROUP BY toString(b)) SETTINGS optimize_injective_functions_in_group_by = 1;

CREATE TABLE t_array_saf (k UInt8, a Array(SimpleAggregateFunction(anyLast, Bool)))
ENGINE = MergeTree ORDER BY (toString(a), k) SETTINGS index_granularity = 1;
INSERT INTO t_array_saf VALUES (1, [2]), (2, [1]), (3, [0]);
SELECT 'array saf bytes', arraySort(groupArray(arrayMap(x -> reinterpretAsUInt8(x), a))) FROM t_array_saf;
SELECT 'array saf not in', count() FROM t_array_saf WHERE a NOT IN ([1]);

-- `toString` stays injective for a `UInt8`, also under the name of a `SimpleAggregateFunction`: the key is unwrapped.
CREATE TABLE t_ctl (u UInt8, su SimpleAggregateFunction(anyLast, UInt8), b Bool) ENGINE = MergeTree ORDER BY tuple();
SELECT 'uint8 key', count() FROM (EXPLAIN QUERY TREE SELECT toString(u) FROM t_ctl GROUP BY toString(u) SETTINGS optimize_injective_functions_in_group_by = 1) WHERE explain ILIKE '%function_name: toString%';
SELECT 'saf uint8 key', count() FROM (EXPLAIN QUERY TREE SELECT toString(su) FROM t_ctl GROUP BY toString(su) SETTINGS optimize_injective_functions_in_group_by = 1) WHERE explain ILIKE '%function_name: toString%';
SELECT 'bool key', count() FROM (EXPLAIN QUERY TREE SELECT toString(b) FROM t_ctl GROUP BY toString(b) SETTINGS optimize_injective_functions_in_group_by = 1) WHERE explain ILIKE '%function_name: toString%';

DROP TABLE t_saf;
DROP TABLE t_saf_max;
DROP TABLE t_saf_skip;
DROP TABLE t_saf_part;
DROP TABLE t_bool;
DROP TABLE t_bool_key;
DROP TABLE t_saf_nullable;
DROP TABLE t_saf_array;
DROP TABLE t_array_saf;
DROP TABLE t_ctl;

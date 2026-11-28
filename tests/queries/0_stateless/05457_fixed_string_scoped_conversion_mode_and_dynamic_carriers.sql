-- The conversion of `FixedString` to `String` keeps the mode of the scope where it was built (`SETTINGS` of a subquery),
-- also in the `if` chain rewritten to `multiIf` and in a serialized query plan; untyped `NULL` in null-safe tuple
-- comparisons with a `FixedString`; and index analysis with a `FixedString` hidden in `Dynamic` or in a map value.

SELECT 'if chain to multiIf with scoped settings';
SELECT * FROM
(
    SELECT number, hex(if(number = 0, 'x', if(number = 1, toFixedString('a', 2), 'y'))) AS v
    FROM numbers(2)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, optimize_if_chain_to_multiif = 1, optimize_if_transform_strings_to_enum = 0
)
ORDER BY number
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT 'null-safe comparison with an untyped NULL tuple element';
SELECT isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2))), toNullable(tuple(NULL)));
SELECT isDistinctFrom(toNullable(tuple(toFixedString('a', 2))), toNullable(tuple(NULL)));
SELECT isNotDistinctFrom(toNullable(tuple(CAST(NULL AS Nullable(FixedString(2))))), toNullable(tuple(NULL)));

SELECT 'serialized plan';
SELECT count() FROM (SELECT toString(toFixedString(toString(number), 2)) AS v FROM numbers(1) SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1)
WHERE v = '0' SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM (SELECT multiIf(number = 0, toFixedString(toString(number), 2), 'x') AS v FROM numbers(1) SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1)
WHERE v = '0' SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM (SELECT arrayStringConcat([toFixedString(toString(number), 2)]) AS v FROM numbers(1) SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1)
WHERE v = '0' SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM (SELECT transform(number, [0], [toFixedString('0', 2)], 'x') AS v FROM numbers(1) SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1)
WHERE v = '0' SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'bloom_filter over mapValues';
DROP TABLE IF EXISTS t_map_bf;
CREATE TABLE t_map_bf (id UInt64, m Map(String, String), INDEX idx mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map_bf VALUES (1, {'k': 'a'}), (2, {'k': 'b'}), (3, {'k': 'a\0'});
SELECT groupArray(id) FROM (SELECT id FROM t_map_bf WHERE m['k'] IN (SELECT toFixedString('a', 2)) ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM t_map_bf WHERE m['k'] IN (SELECT toFixedString('a', 2)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_bf WHERE m['k'] GLOBAL IN (SELECT toFixedString('a', 2)) ORDER BY id);
DROP TABLE t_map_bf;

SELECT 'FixedString in Dynamic';
DROP TABLE IF EXISTS t_dyn_part_hash;
CREATE TABLE t_dyn_part_hash (a Tuple(String)) ENGINE = MergeTree PARTITION BY cityHash64(a) ORDER BY tuple();
INSERT INTO t_dyn_part_hash VALUES (tuple('a')), (tuple('b'));
SELECT count() FROM t_dyn_part_hash WHERE a = CAST(tuple(toFixedString('a', 2)) AS Dynamic) SETTINGS use_partition_pruning = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_dyn_part_hash WHERE a = CAST(tuple(toFixedString('a', 2)) AS Dynamic);
SELECT count() FROM t_dyn_part_hash WHERE a = tuple(CAST(toFixedString('a', 2) AS Dynamic));
DROP TABLE t_dyn_part_hash;

DROP TABLE IF EXISTS t_dyn_order_hash;
CREATE TABLE t_dyn_order_hash (a Tuple(String)) ENGINE = MergeTree ORDER BY cityHash64(a) SETTINGS index_granularity = 1;
INSERT INTO t_dyn_order_hash VALUES (tuple('a')), (tuple('b'));
SELECT count() FROM t_dyn_order_hash WHERE a = CAST(tuple(toFixedString('a', 2)) AS Dynamic) SETTINGS use_primary_key = 0;
SELECT count() FROM t_dyn_order_hash WHERE a = CAST(tuple(toFixedString('a', 2)) AS Dynamic);
DROP TABLE t_dyn_order_hash;

DROP TABLE IF EXISTS t_dyn_part_tuple;
CREATE TABLE t_dyn_part_tuple (a Tuple(String)) ENGINE = MergeTree PARTITION BY a ORDER BY tuple();
INSERT INTO t_dyn_part_tuple VALUES (tuple('a')), (tuple('b'));
SELECT count() FROM t_dyn_part_tuple WHERE a = CAST(tuple(toFixedString('a', 2)) AS Dynamic);
SELECT count() FROM t_dyn_part_tuple WHERE a = tuple(CAST(toFixedString('a', 2) AS Dynamic));
DROP TABLE t_dyn_part_tuple;

DROP TABLE IF EXISTS t_dyn_part_array;
CREATE TABLE t_dyn_part_array (a Array(String)) ENGINE = MergeTree PARTITION BY a ORDER BY tuple();
INSERT INTO t_dyn_part_array VALUES (['a']), (['b']);
SELECT count() FROM t_dyn_part_array WHERE a = CAST([toFixedString('a', 2)] AS Dynamic) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_dyn_part_array WHERE a = CAST([toFixedString('a', 2)] AS Dynamic);
DROP TABLE t_dyn_part_array;

-- Regressions for `FixedArray`, the subscripts `arr[i, j]` and the lambda form of `einsum`: functions bound by aliases inside
-- the lambda function of `einsum`, the user function `__subscripts`, `FINAL` with `FixedArray` keys and the read in order
-- with a `FixedArray` constant.

SET enable_analyzer = 1;
SET allow_experimental_fixed_array_type = 1;

-- A function bound to a lambda function by an alias inside the lambda function of `einsum` is a local name:
-- the array of a tensor element cannot be computed outside of the lambda function.
WITH x -> [100] AS f SELECT einsum(i -> length(arrayMap((x -> [x + 1]) AS f, [0])) + f(1)[i]); -- { serverError BAD_ARGUMENTS }
WITH x -> [100] AS g SELECT einsum(i -> length(arrayMap(g AS f, [0])) + f(1)[i]); -- { serverError BAD_ARGUMENTS }
-- A function bound outside of the lambda function of `einsum` is the same in the arguments of `einsum`.
WITH x -> [100, 200] AS f SELECT einsum(i -> f(1)[i]);

-- A call of a lambda function bound by an alias uses the indices of the lambda function.
SELECT einsum(i -> length(arrayMap((x -> a[i]) AS f, [0])) + f(0) * a[i]) FROM (SELECT [1, 2] AS a);

-- An alias of an identifier that refers to a lambda function binds `multiply` to it as well.
WITH (x, y) -> x + y AS f SELECT einsum(i -> length(arrayMap(f AS multiply, [0], [0])) + multiply(a[i], a[i])) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> length(arrayMap(((x, y) -> x + y) AS f, [0], [0])) + length(arrayMap(f AS multiply, [0], [0])) + multiply(a[i], a[i])) FROM (SELECT [1, 2] AS a);
-- An alias of an identifier that is not a lambda function does not.
SELECT einsum(i -> length([b AS multiply]) + multiply(a[i], a[i])) FROM (SELECT [1, 2] AS a, 0 AS b);

-- The user function `__subscripts` is a function, not several subscripts.
WITH (x, y) -> 1 AS __subscripts SELECT [[10, 20], [30, 40]][__subscripts(1, 2)];
WITH x -> 1 AS __subscripts SELECT [10, 20][__subscripts(2)];
SELECT [[10, 20], [30, 40]][1, 2], [[10, 20], [30, 40]][][2];

-- `FixedArray` keys with `Nullable` elements are not split into layers for `FINAL`.
DROP TABLE IF EXISTS t_fixed_array_final;
CREATE TABLE t_fixed_array_final (a FixedArray(1, Nullable(UInt64)))
ENGINE = ReplacingMergeTree ORDER BY a
SETTINGS index_granularity = 1, allow_nullable_key = 1;
SYSTEM STOP MERGES t_fixed_array_final;
INSERT INTO t_fixed_array_final VALUES ([1]), ([3]), ([NULL]);
INSERT INTO t_fixed_array_final VALUES ([2]), ([NULL]);
SELECT a FROM t_fixed_array_final FINAL ORDER BY a SETTINGS max_threads = 4,
    split_intersecting_parts_ranges_into_layers_final = 1,
    split_parts_ranges_into_intersecting_and_non_intersecting_final = 1;
SELECT count() FROM t_fixed_array_final FINAL SETTINGS max_threads = 4,
    split_intersecting_parts_ranges_into_layers_final = 1,
    split_parts_ranges_into_intersecting_and_non_intersecting_final = 1;
DROP TABLE t_fixed_array_final;

-- An equality with a `FixedArray` constant can match several `Decimal` keys, so it does not fix the prefix of the sorting key.
DROP TABLE IF EXISTS t_fixed_array_read_in_order;
CREATE TABLE t_fixed_array_read_in_order (a Array(Decimal(38, 18)), ts UInt32) ENGINE = MergeTree ORDER BY (a, ts);
INSERT INTO t_fixed_array_read_in_order VALUES (['1'], 20), (['1'], 30), (['1.000000000000000001'], 10), (['1.000000000000000001'], 20);
SELECT ts FROM t_fixed_array_read_in_order WHERE a = CAST([1.0], 'FixedArray(1, Float64)') ORDER BY ts LIMIT 1
SETTINGS optimize_read_in_order = 1, use_primary_key = 0, use_statistics_for_part_pruning = 0;
DROP TABLE t_fixed_array_read_in_order;

DROP TABLE IF EXISTS t_fixed_array_key_read_in_order;
CREATE TABLE t_fixed_array_key_read_in_order (a FixedArray(1, Decimal(38, 18)), ts UInt32) ENGINE = MergeTree ORDER BY (a, ts);
INSERT INTO t_fixed_array_key_read_in_order VALUES (['1'], 20), (['1'], 30), (['1.000000000000000001'], 10), (['1.000000000000000001'], 20);
SELECT ts FROM t_fixed_array_key_read_in_order WHERE a = [1.0] ORDER BY ts LIMIT 1
SETTINGS optimize_read_in_order = 1, use_primary_key = 0, use_statistics_for_part_pruning = 0;
DROP TABLE t_fixed_array_key_read_in_order;

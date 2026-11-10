-- Tags: no-random-merge-tree-settings
-- Random MergeTree settings change the granules that the row limits depend on.

-- A NULL makes only the atoms that read it NULL. Where it can't change the result of the whole condition, the primary
-- key still finds the ranges where the condition is true on every row, and `count()` doesn't read them.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0;
SET merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- Half of the rows have a NULL `x`. The key puts them last for each `k`.
DROP TABLE IF EXISTS t_null_key;
CREATE TABLE t_null_key (k UInt32, x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_null_key SELECT intDiv(number, 100), if(number % 2 = 0, NULL, number % 10) FROM numbers(1000);

SELECT count() FROM t_null_key WHERE x != 5;
-- `k >= 5` is true on every row in the second half, also where `x` is NULL. The count doesn't read these rows.
SELECT count() FROM t_null_key WHERE k >= 5 OR x < 3 SETTINGS max_rows_to_read = 200;

-- `ts > X` makes a group of two atoms, one for `toDate(ts)` and one for `ts`. Both are NULL where `ts` is NULL.
DROP TABLE IF EXISTS t_atom_group;
CREATE TABLE t_atom_group (k UInt32, ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY (k, toDate(ts), ts)
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_atom_group SELECT intDiv(number, 100), if(number % 2 = 0, NULL, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600)
FROM numbers(1000);

SELECT count() FROM t_atom_group WHERE ts > toDateTime('2020-01-02 01:00:00', 'UTC');

-- `ifNull(x, 0)` and `tuple(x)` aren't NULL on a row where `x` is NULL, but `x != 5` and `x NOT IN (5)` are. Only the
-- key range of `x` shows where they are NULL.
DROP TABLE IF EXISTS t_hidden_null;
CREATE TABLE t_hidden_null (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (ifNull(x, 0), tuple(x), x)
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_hidden_null SELECT if(number % 2 = 0, NULL, number % 10) FROM numbers(1000);

SELECT count() FROM t_hidden_null WHERE x != 5;
SELECT count() FROM t_hidden_null WHERE x != 5 OR x = 5;
SELECT count() FROM t_hidden_null WHERE x NOT IN (5);

-- `has` is never NULL, so its atoms on `ifNull(x, 0)` and `tuple(x)` find the ranges without a 5.
DROP TABLE IF EXISTS t_hidden_null_has;
CREATE TABLE t_hidden_null_has (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (ifNull(x, 0), tuple(x))
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_hidden_null_has SELECT if(number % 20 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_hidden_null_has WHERE NOT has([5], x) SETTINGS max_rows_to_read = 20;

-- `assumeNotNull(x)` returns the value stored under a NULL `x`, and this INSERT stores the row number there. So
-- `nullIf(assumeNotNull(x), 0)` is NULL for the first row only, and an ordinary value for the other rows with a NULL `x`.
DROP TABLE IF EXISTS t_hidden_null_value;
CREATE TABLE t_hidden_null_value (x Nullable(Int32)) ENGINE = MergeTree ORDER BY (nullIf(assumeNotNull(x), 0), x)
SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_hidden_null_value SELECT nullIf(toInt32(number), if(number % 7 = 0, toInt32(number), -1)) FROM numbers(100);

SELECT count() FROM t_hidden_null_value WHERE x != 5;

-- Every `a` is different, so the primary key keeps only `a` in memory, and `b` can hold a NULL in every range.
DROP TABLE IF EXISTS t_unloaded_null_key;
CREATE TABLE t_unloaded_null_key (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_null_key SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);

-- `b NOT IN (NULL)` is NULL on the 13 rows with a NULL `b`.
SELECT count() FROM t_unloaded_null_key WHERE a >= 10 AND b NOT IN (NULL) SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- The primary key keeps only `negate(x)` in memory. `x NOT IN (5)` and `x != 5` each make a group of two atoms, and
-- the range of `negate(x)` shows where they are NULL.
DROP TABLE IF EXISTS t_unloaded_atom_group;
CREATE TABLE t_unloaded_atom_group (x Nullable(Int32)) ENGINE = MergeTree ORDER BY (negate(x), x)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_atom_group SELECT if(number % 20 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_unloaded_atom_group WHERE x NOT IN (5) SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_unloaded_atom_group WHERE x != 5 SETTINGS use_lightweight_primary_key_index_analysis = 1, max_rows_to_read = 20;

-- The primary key keeps only `ifNull(x, 0)` in memory, which stores a NULL `x` as 0. `x != 5` is NULL on those rows,
-- and no range shows it.
DROP TABLE IF EXISTS t_unloaded_hidden_null;
CREATE TABLE t_unloaded_hidden_null (x Nullable(Int32)) ENGINE = MergeTree ORDER BY (ifNull(x, 0), x)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_hidden_null SELECT if(number % 20 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_unloaded_hidden_null WHERE x != 5 SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- Half of the rows have a NULL `x`, and the key puts them last.
DROP TABLE IF EXISTS t_null_granules;
CREATE TABLE t_null_granules (x Nullable(Float64)) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_null_granules SELECT if(number % 2 = 0, NULL, number) FROM numbers(100);

-- With a float `x`, the `NOT` stays above the `x < 5` atom. The atom is NULL on the NULL rows, and so is the `NOT`.
SELECT count() FROM t_null_granules WHERE NOT (x < 5);
-- `x <=> 5` is false on a NULL row, so the `NOT` is true there.
SELECT count() FROM t_null_granules WHERE NOT (x <=> 5);
-- `assumeNotNull(x)` returns a value on a NULL row.
SELECT (SELECT count() FROM t_null_granules WHERE assumeNotNull(x) > 40)
    = (SELECT count() FROM t_null_granules WHERE assumeNotNull(x) > 40 SETTINGS use_primary_key = 0);
-- With `transform_null_in`, `x IN (NULL)` is true on a NULL row. The count doesn't read the all-NULL granules.
SELECT count() FROM t_null_granules WHERE x IN (NULL) SETTINGS transform_null_in = 1, max_rows_to_read = 8;

DROP TABLE t_null_key;
DROP TABLE t_atom_group;
DROP TABLE t_hidden_null;
DROP TABLE t_hidden_null_has;
DROP TABLE t_hidden_null_value;
DROP TABLE t_unloaded_null_key;
DROP TABLE t_unloaded_atom_group;
DROP TABLE t_unloaded_hidden_null;
DROP TABLE t_null_granules;

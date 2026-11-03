-- https://github.com/ClickHouse/ClickHouse/issues/78795
-- In AggregatingMergeTree, merges aggregate a SimpleAggregateFunction or AggregateFunction column, so a sorting or partition key computed from it
-- does not match the merged rows, and the primary key and partition pruning skip matching rows. Such a key is rejected at table
-- creation and by an ALTER that makes a key use such a column, unless `allow_aggregating_columns_in_partition_or_order_key` is set.

DROP TABLE IF EXISTS t_sorting;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_allowed;
DROP TABLE IF EXISTS t_other_key;
DROP TABLE IF EXISTS t_in_sorting_key;
DROP TABLE IF EXISTS t_modify_column;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_tuple_other_element;
DROP TABLE IF EXISTS t_tuple_alter;
DROP TABLE IF EXISTS t_tuple_default;
DROP TABLE IF EXISTS t_tuple_elements;
DROP TABLE IF EXISTS t_in_sorting_key_subcolumn;
DROP TABLE IF EXISTS t_in_sorting_key_element;
DROP TABLE IF EXISTS t_in_sorting_key_element_subcolumn;

CREATE TABLE t_sorting (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree ORDER BY intDiv(c0, 55); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_partition (k UInt8, c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree PARTITION BY c0 % 2 ORDER BY k; -- { serverError BAD_ARGUMENTS }
-- A regular aggregate state is aggregated too.
CREATE TABLE t_state (c AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY intDiv(finalizeAggregation(c), 55); -- { serverError BAD_ARGUMENTS }
-- So is the value under a subcolumn of an aggregated column, and an aggregated element of a tuple.
CREATE TABLE t_size (k UInt8, a SimpleAggregateFunction(groupArrayArray, Array(UInt64))) ENGINE = AggregatingMergeTree PARTITION BY a.size0 ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tuple_element (k UInt8, x Tuple(v SimpleAggregateFunction(sum, UInt64), w UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY x.v % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tuple_position (x Tuple(UInt64, SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree
PARTITION BY x.2 % 2 ORDER BY x.1 SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tuple_other_element (k UInt8, x Tuple(v SimpleAggregateFunction(sum, UInt64), w UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY x.w % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
SELECT 'other tuple element created';
CREATE TABLE t_tuple_default (x Tuple(UInt64, SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree
ORDER BY tupleElement(x, 1, 0) SETTINGS allow_tuple_element_aggregation = 1;
SELECT 'element with a default created';
CREATE TABLE t_tuple_alter (k UInt8, x Tuple(v UInt64)) ENGINE = AggregatingMergeTree PARTITION BY x.v % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_tuple_alter MODIFY COLUMN x Tuple(v SimpleAggregateFunction(sum, UInt64)); -- { serverError BAD_ARGUMENTS }

-- The setting allows it. No rows are inserted: a merge of such a table breaks the sort order of the part, which is a logical
-- error in debug builds.
CREATE TABLE t_allowed (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree ORDER BY intDiv(c0, 55)
SETTINGS allow_aggregating_columns_in_partition_or_order_key = 1;
SELECT 'allowed created';

-- An existing table keeps loading and accepts ALTERs.
ALTER TABLE t_allowed MODIFY SETTING allow_aggregating_columns_in_partition_or_order_key = 0;
DETACH TABLE t_allowed;
ATTACH TABLE t_allowed;
ALTER TABLE t_allowed MODIFY COMMENT 'c';
SELECT 'attached', count(), (SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_allowed') FROM t_allowed;
-- Only the columns that an ALTER adds to the keys are rejected.
ALTER TABLE t_allowed ADD COLUMN v SimpleAggregateFunction(sum, Int64), MODIFY ORDER BY (intDiv(c0, 55), intDiv(v, 2)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_allowed ADD COLUMN w UInt8, MODIFY ORDER BY (intDiv(c0, 55), w);
SELECT 'allowed altered', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_allowed';

-- So is another element of a tuple that an ALTER makes aggregated, while the key already used an aggregated one.
CREATE TABLE t_tuple_elements (k UInt8, x Tuple(a SimpleAggregateFunction(max, UInt64), b UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY (tupleElement(x, 'a') % 2, tupleElement(x, 'b') % 2) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1, allow_aggregating_columns_in_partition_or_order_key = 1;
ALTER TABLE t_tuple_elements MODIFY SETTING allow_aggregating_columns_in_partition_or_order_key = 0;
ALTER TABLE t_tuple_elements MODIFY COLUMN x Tuple(a SimpleAggregateFunction(max, UInt64), b SimpleAggregateFunction(sum, UInt64)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_elements MODIFY COLUMN x COMMENT 'kept';
SELECT 'tuple elements altered', comment FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_elements' AND name = 'x';

-- Keys that do not depend on SimpleAggregateFunction columns are not affected.
CREATE TABLE t_other_key (k UInt64, c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree PARTITION BY k % 2 ORDER BY intDiv(k, 10);
INSERT INTO t_other_key SELECT number, number FROM numbers(100);
OPTIMIZE TABLE t_other_key FINAL;
SELECT 'other key', count(), sum(c0) FROM t_other_key;

-- An ALTER that changes the sorting key is checked as well.
ALTER TABLE t_other_key ADD COLUMN v SimpleAggregateFunction(sum, Int64), MODIFY ORDER BY (intDiv(k, 10), intDiv(v, 2)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_other_key ADD COLUMN w UInt8, MODIFY ORDER BY (intDiv(k, 10), w);
SELECT 'other key altered', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_other_key';
-- So is an ALTER that changes the type of a column of the key to SimpleAggregateFunction.
CREATE TABLE t_modify_column (k UInt64, c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree ORDER BY intDiv(k, 10);
ALTER TABLE t_modify_column MODIFY COLUMN k SimpleAggregateFunction(sum, UInt64); -- { serverError BAD_ARGUMENTS }

-- A SimpleAggregateFunction column that is a column of the sorting key itself is not aggregated, so it can be used in the keys.
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_in_sorting_key (k UInt8, c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree PARTITION BY c0 % 2 ORDER BY (k, c0);
SET allow_suspicious_primary_key = 0;
INSERT INTO t_in_sorting_key VALUES (1, 1), (1, 1), (1, 2);
OPTIMIZE TABLE t_in_sorting_key FINAL;
SELECT 'in sorting key', k, c0 FROM t_in_sorting_key ORDER BY ALL;

-- So is a subcolumn of such a column, and with `allow_tuple_element_aggregation` an element of a tuple that is a column of the
-- sorting key, read by name, by position or through a subcolumn of it. Another element of the tuple is still aggregated.
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_in_sorting_key_subcolumn (a SimpleAggregateFunction(groupArrayArray, Array(UInt64))) ENGINE = AggregatingMergeTree
PARTITION BY a.size0 ORDER BY a;
CREATE TABLE t_in_sorting_key_element (x Tuple(a SimpleAggregateFunction(sum, UInt64), b SimpleAggregateFunction(sum, UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY (tupleElement(x, 'a') % 2, tupleElement(x, 1) % 3) ORDER BY x.a
SETTINGS allow_tuple_element_aggregation = 1;
INSERT INTO t_in_sorting_key_element VALUES ((1, 10));
INSERT INTO t_in_sorting_key_element VALUES ((1, 20));
OPTIMIZE TABLE t_in_sorting_key_element FINAL;
SELECT 'in sorting key element', _partition_id, x FROM t_in_sorting_key_element;
CREATE TABLE t_in_sorting_key_element_subcolumn (x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), b SimpleAggregateFunction(sum, UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY x.a.size0 ORDER BY x.a SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_other_element (x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(x, 'a') % 2 ORDER BY x.b SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
SET allow_suspicious_primary_key = 0;

-- A plain MergeTree does not aggregate the column, so the key is fine there.
CREATE TABLE t_plain (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = MergeTree ORDER BY intDiv(c0, 55);
SELECT 'plain created';

DROP TABLE t_allowed;
DROP TABLE t_other_key;
DROP TABLE t_in_sorting_key;
DROP TABLE t_modify_column;
DROP TABLE t_plain;
DROP TABLE t_tuple_other_element;
DROP TABLE t_tuple_alter;
DROP TABLE t_tuple_default;
DROP TABLE t_in_sorting_key_subcolumn;
DROP TABLE t_in_sorting_key_element;
DROP TABLE t_in_sorting_key_element_subcolumn;
DROP TABLE t_tuple_elements;

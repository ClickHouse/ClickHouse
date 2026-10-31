-- https://github.com/ClickHouse/ClickHouse/issues/78795
-- In AggregatingMergeTree, merges aggregate a SimpleAggregateFunction column, so a sorting or partition key computed from it
-- does not match the merged rows, and the primary key and partition pruning skip matching rows. Such a key is rejected at table
-- creation and by an ALTER that makes a key use such a column, unless `allow_aggregating_columns_in_partition_or_order_key` is set.

DROP TABLE IF EXISTS t_sorting;
DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_allowed;
DROP TABLE IF EXISTS t_other_key;
DROP TABLE IF EXISTS t_in_sorting_key;
DROP TABLE IF EXISTS t_modify_column;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_sorting (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree ORDER BY intDiv(c0, 55); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_partition (k UInt8, c0 SimpleAggregateFunction(sum, Int64)) ENGINE = AggregatingMergeTree PARTITION BY c0 % 2 ORDER BY k; -- { serverError BAD_ARGUMENTS }

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

-- A plain MergeTree does not aggregate the column, so the key is fine there.
CREATE TABLE t_plain (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = MergeTree ORDER BY intDiv(c0, 55);
SELECT 'plain created';

DROP TABLE t_allowed;
DROP TABLE t_other_key;
DROP TABLE t_in_sorting_key;
DROP TABLE t_modify_column;
DROP TABLE t_plain;

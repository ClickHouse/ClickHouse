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
DROP TABLE IF EXISTS t_selector_subcolumn;
DROP TABLE IF EXISTS t_selector_negative;
DROP TABLE IF EXISTS t_selector_aggregated;
DROP TABLE IF EXISTS t_selector_path;
DROP TABLE IF EXISTS t_selector_path_aggregated;
DROP TABLE IF EXISTS t_selector_default;
DROP TABLE IF EXISTS t_selector_default_name;
DROP TABLE IF EXISTS t_selector_default_self;
DROP TABLE IF EXISTS t_selector_default_unused;
DROP TABLE IF EXISTS t_selector_default_aggregated;
DROP TABLE IF EXISTS t_aggregated_tuple;
DROP TABLE IF EXISTS t_in_sorting_key_subcolumn;
DROP TABLE IF EXISTS t_in_sorting_key_element;
DROP TABLE IF EXISTS t_in_sorting_key_element_subcolumn;
DROP TABLE IF EXISTS t_subcolumn_in_sorting_key;
DROP TABLE IF EXISTS t_element_subcolumn_in_sorting_key;
DROP TABLE IF EXISTS t_unused_default;
DROP TABLE IF EXISTS t_unused_default_alter;
DROP TABLE IF EXISTS t_unused_default_tuple;
DROP TABLE IF EXISTS t_used_default;
DROP TABLE IF EXISTS t_unused_default_subcolumn;
DROP TABLE IF EXISTS t_unused_default_element;
DROP TABLE IF EXISTS t_tuple_sorting_key;

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

-- An element selected by `getSubcolumn` or by a negative position is the same element.
CREATE TABLE t_selector_subcolumn (k UInt8, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(x, 'b') % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_selector_negative (y Tuple(UInt64, SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree
ORDER BY tupleElement(y, -2) SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_selector_path (k UInt8, x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), n Tuple(c SimpleAggregateFunction(sum, UInt64), d UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY getSubcolumn(x, 'n.d') % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
SELECT 'selectors created';
CREATE TABLE t_selector_aggregated (k UInt8, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(x, 'a') % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
-- A subcolumn of an aggregated element, and an aggregated element of a nested tuple, selected by a path.
CREATE TABLE t_selector_path_aggregated (k UInt8, x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), n Tuple(c SimpleAggregateFunction(sum, UInt64), d UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY getSubcolumn(x, 'a.size0') % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selector_path_aggregated (k UInt8, x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), n Tuple(c SimpleAggregateFunction(sum, UInt64), d UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY getSubcolumn(x, 'n.c') % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- An aggregated value of a tuple type is aggregated as a whole, also inside another tuple and when an ALTER makes it one.
CREATE TABLE t_aggregated_tuple (k UInt8, a SimpleAggregateFunction(sumMap, Tuple(Array(UInt64), Array(UInt64))))
ENGINE = AggregatingMergeTree PARTITION BY length(tupleElement(a, 1)) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_aggregated_tuple (k UInt8, x Tuple(s SimpleAggregateFunction(sumMap, Tuple(Array(UInt64), Array(UInt64))), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY length(tupleElement(tupleElement(x, 's'), 1)) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_aggregated_tuple (k UInt8, a Tuple(Array(UInt64), Array(UInt64))) ENGINE = AggregatingMergeTree
PARTITION BY length(tupleElement(a, 1)) ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_aggregated_tuple MODIFY COLUMN a SimpleAggregateFunction(sumMap, Tuple(Array(UInt64), Array(UInt64))); -- { serverError BAD_ARGUMENTS }

-- With a default as the third argument, `tupleElement` returns the element if it exists, and the default otherwise: the tuple is
-- read only when the element exists, and the default only when it does not, also when the tuple is the default itself.
CREATE TABLE t_selector_default (x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64), k UInt64) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(x, 3, k) ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_selector_default_name (x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64), k UInt64) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(x, 'missing', k) % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_selector_default_self (x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64), k UInt64) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(x, 'b', x) % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_selector_default_unused (x Tuple(a SimpleAggregateFunction(sum, UInt64)), t Tuple(c UInt64), k UInt64)
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(t, 'c', x) % 2 ORDER BY k SETTINGS allow_tuple_element_aggregation = 1;
SELECT 'selected defaults created';
CREATE TABLE t_selector_default_aggregated (x Tuple(a SimpleAggregateFunction(sum, UInt64)), `x.missing.a` UInt64)
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(x, 'missing', x), 'a') % 2 ORDER BY `x.missing.a`
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selector_default_aggregated (x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64), k UInt64)
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(x, 'a', x) % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selector_default_aggregated (x Tuple(a SimpleAggregateFunction(sum, UInt64)), t Tuple(c UInt64), k UInt64)
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(t, 'missing', x), 'a') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- A subcolumn of an aggregated value in the sorting key does not stop merges from aggregating the value: they compare the name of
-- the value itself with the sorting key.
CREATE TABLE t_subcolumn_in_sorting_key (a SimpleAggregateFunction(groupArrayArray, Array(UInt64))) ENGINE = AggregatingMergeTree
PARTITION BY a.size0 ORDER BY a.size0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_in_sorting_key (a Array(UInt64)) ENGINE = AggregatingMergeTree PARTITION BY a.size0 ORDER BY a.size0;
ALTER TABLE t_subcolumn_in_sorting_key MODIFY COLUMN a SimpleAggregateFunction(groupArrayArray, Array(UInt64)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_element_subcolumn_in_sorting_key (x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY x.a.size0 ORDER BY x.a.size0
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_element_subcolumn_in_sorting_key (x Tuple(a Array(UInt64), b UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY x.a.size0 ORDER BY x.a.size0 SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_element_subcolumn_in_sorting_key MODIFY COLUMN x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), b UInt64); -- { serverError BAD_ARGUMENTS }

-- An aggregated column that is only a default of `tupleElement` that `tupleElement` does not return is not read by the key, also
-- without `allow_tuple_element_aggregation`, when an ALTER makes it aggregated, and for an aggregated value of a tuple type.
CREATE TABLE t_unused_default (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(k), 1, c) ORDER BY k;
CREATE TABLE t_unused_default_alter (k UInt64, c UInt64) ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tuple(k), 1, c) ORDER BY k;
ALTER TABLE t_unused_default_alter MODIFY COLUMN c SimpleAggregateFunction(sum, UInt64);
CREATE TABLE t_unused_default_tuple (k UInt64, s SimpleAggregateFunction(sumMap, Tuple(Array(UInt64), Array(UInt64))))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tuple(k), 1, s) ORDER BY k;
INSERT INTO t_unused_default VALUES (1, 2);
INSERT INTO t_unused_default VALUES (1, 3);
OPTIMIZE TABLE t_unused_default FINAL;
SELECT 'unused default', _partition_id, k, c FROM t_unused_default WHERE tupleElement(tuple(k), 1, c) = 1;
CREATE TABLE t_used_default (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(k), 2, c) ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_unused_default_subcolumn (k UInt64, a SimpleAggregateFunction(groupArrayArray, Array(UInt64)))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tuple(k), 1, a.size0) ORDER BY k;
CREATE TABLE t_unused_default_element (k UInt64, x Tuple(a SimpleAggregateFunction(groupArrayArray, Array(UInt64)), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tuple(k), 1, getSubcolumn(x, 'a.size0')) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
-- Also a default computed from the column, and an element of a constructed tuple that `tupleElement` does not select, also of
-- nested tuples; but a value computed from a constructed tuple reads all its elements.
CREATE TABLE t_unused_computed_default (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(k), 1, c + 1) ORDER BY k;
CREATE TABLE t_unused_computed_default_alter (k UInt64, c UInt64) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(k), 1, c + 1) ORDER BY k;
ALTER TABLE t_unused_computed_default_alter MODIFY COLUMN c SimpleAggregateFunction(sum, UInt64);
CREATE TABLE t_unused_computed_default_element (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tuple(k), 1, length(toString(x))) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_unselected_element (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(c, k), 2) ORDER BY k;
CREATE TABLE t_unselected_nested_element (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tupleElement(tuple(tuple(c, k)), 1), 2) ORDER BY k;
-- Also an element of a constructed tuple that `getSubcolumn` does not select, and an ordinary element of a tuple with an aggregated
-- element selected from a constructed tuple, by `tupleElement` or by `getSubcolumn`, at `CREATE` and when an ALTER makes the other
-- value aggregated.
CREATE TABLE t_unselected_subcolumn (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(tuple(c, k), '2') ORDER BY k;
CREATE TABLE t_unselected_subcolumn_alter (k UInt64, c UInt64) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(tuple(c, k), '2') ORDER BY k;
ALTER TABLE t_unselected_subcolumn_alter MODIFY COLUMN c SimpleAggregateFunction(sum, UInt64);
CREATE TABLE t_constructed_other_element (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(tuple(x), 1), 'b') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
CREATE TABLE t_constructed_other_element_alter (k UInt64, x Tuple(a UInt64, b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(tuple(x), 1), 'b') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_constructed_other_element_alter MODIFY COLUMN x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64);
CREATE TABLE t_constructed_other_subcolumn (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY getSubcolumn(tuple(x), '1.b') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
INSERT INTO t_unselected_subcolumn VALUES (1, 2);
INSERT INTO t_unselected_subcolumn VALUES (1, 3);
OPTIMIZE TABLE t_unselected_subcolumn FINAL;
SELECT 'unselected subcolumn', _partition_id, k, c FROM t_unselected_subcolumn WHERE getSubcolumn(tuple(c, k), '2') = 1;
INSERT INTO t_constructed_other_element VALUES (1, (2, 1));
INSERT INTO t_constructed_other_element VALUES (1, (3, 1));
OPTIMIZE TABLE t_constructed_other_element FINAL;
SELECT 'constructed other element', _partition_id, k, x FROM t_constructed_other_element
WHERE tupleElement(tupleElement(tuple(x), 1), 'b') % 2 = 1;
INSERT INTO t_unselected_element VALUES (1, 2);
INSERT INTO t_unselected_element VALUES (1, 3);
OPTIMIZE TABLE t_unselected_element FINAL;
SELECT 'unselected element', _partition_id, k, c FROM t_unselected_element WHERE tupleElement(tuple(c, k), 2) = 1;
CREATE TABLE t_used_computed_default (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(k), 2, c + 1) ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selected_element (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tuple(c, k), 1) ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selected_nested_element (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY tupleElement(tupleElement(tuple(tuple(c, k)), 1), 1) ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_computed_from_constructed (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(tuple(c, k)) % 2 ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selected_subcolumn (k UInt64, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(tuple(c, k), '1') ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_selected_subcolumn_alter (k UInt64, c UInt64) ENGINE = AggregatingMergeTree
PARTITION BY getSubcolumn(tuple(c, k), '1') ORDER BY k;
ALTER TABLE t_selected_subcolumn_alter MODIFY COLUMN c SimpleAggregateFunction(sum, UInt64); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_constructed_element (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(tuple(x), 1), 'a') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_constructed_element_alter (k UInt64, x Tuple(a UInt64, b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY tupleElement(tupleElement(tuple(x), 1), 'a') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_constructed_element_alter MODIFY COLUMN x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_constructed_subcolumn (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY getSubcolumn(tuple(x), '1.a') % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_computed_from_constructed_element (k UInt64, x Tuple(a SimpleAggregateFunction(sum, UInt64), b UInt64))
ENGINE = AggregatingMergeTree PARTITION BY cityHash64(tupleElement(tuple(x), 1)) % 2 ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }
SELECT 'unused defaults created';

-- A sorting key of a tuple type is not supported with `allow_tuple_element_aggregation`, and is reported as such.
CREATE TABLE t_tuple_sorting_key (x Tuple(k UInt64, a SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree
ORDER BY x SETTINGS allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }

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
DROP TABLE t_selector_subcolumn;
DROP TABLE t_selector_negative;
DROP TABLE t_selector_path;
DROP TABLE t_selector_default;
DROP TABLE t_selector_default_name;
DROP TABLE t_selector_default_self;
DROP TABLE t_selector_default_unused;
DROP TABLE t_aggregated_tuple;
DROP TABLE t_subcolumn_in_sorting_key;
DROP TABLE t_element_subcolumn_in_sorting_key;
DROP TABLE t_unused_default;
DROP TABLE t_unused_default_alter;
DROP TABLE t_unused_default_tuple;
DROP TABLE t_unused_default_subcolumn;
DROP TABLE t_unused_default_element;
DROP TABLE t_unused_computed_default;
DROP TABLE t_unused_computed_default_alter;
DROP TABLE t_unused_computed_default_element;
DROP TABLE t_unselected_element;
DROP TABLE t_unselected_nested_element;
DROP TABLE t_unselected_subcolumn;
DROP TABLE t_unselected_subcolumn_alter;
DROP TABLE t_constructed_other_element;
DROP TABLE t_constructed_other_element_alter;
DROP TABLE t_constructed_other_subcolumn;
DROP TABLE t_selected_subcolumn_alter;
DROP TABLE t_constructed_element_alter;

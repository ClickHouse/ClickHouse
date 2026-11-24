-- https://github.com/ClickHouse/ClickHouse/issues/133861
-- Splitting a tuple comparison must not rebuild the element constants from their `Field`s, which lose the
-- `Variant` alternative and the `Dynamic` type: `5::UInt64::Variant(Date, UInt64)` would land in `Date`.

SET enable_analyzer = 1;
SET use_variant_default_implementation_for_comparisons = 1;

DROP TABLE IF EXISTS t_tuple_cmp_variant_const;
CREATE TABLE t_tuple_cmp_variant_const (x DateTime('UTC'), y UInt8) ENGINE = Memory;
INSERT INTO t_tuple_cmp_variant_const VALUES (toDateTime(5, 'UTC'), 1), (toDateTime(432000, 'UTC'), 1);

SELECT x FROM t_tuple_cmp_variant_const WHERE x = 5::UInt64::Variant(Date, UInt64) AND y = 1 ORDER BY x;
SELECT x FROM t_tuple_cmp_variant_const WHERE (x, y) = (5::UInt64::Variant(Date, UInt64), 1) ORDER BY x;
SELECT x FROM t_tuple_cmp_variant_const WHERE (x, y) != (5::UInt64::Variant(Date, UInt64), 1) ORDER BY x;
SELECT x FROM t_tuple_cmp_variant_const WHERE tuple(x) = tuple(5::UInt64::Variant(Date, UInt64)) ORDER BY x;

SELECT x FROM t_tuple_cmp_variant_const WHERE x = toDate(5)::Dynamic AND y = 1 ORDER BY x;
SELECT x FROM t_tuple_cmp_variant_const WHERE (x, y) = (toDate(5)::Dynamic, 1) ORDER BY x;
SELECT x FROM t_tuple_cmp_variant_const WHERE tuple(x) = tuple(toDate(5)::Dynamic) ORDER BY x;

DROP TABLE t_tuple_cmp_variant_const;

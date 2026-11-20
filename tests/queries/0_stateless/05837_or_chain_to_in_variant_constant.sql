-- https://github.com/ClickHouse/ClickHouse/issues/132702
-- The `IN` set is built from the constants' `Field`s, which lose the `Variant` alternative and the `Dynamic` type:
-- `5::UInt64::Variant(Date, UInt64)` would land in `Date` and match a different row than `equals`.

SET use_variant_default_implementation_for_comparisons = 1;
SET optimize_min_equality_disjunction_chain_length = 3;

DROP TABLE IF EXISTS t_or_in_variant_const;
CREATE TABLE t_or_in_variant_const (x DateTime('UTC'), d Date) ENGINE = Memory;
INSERT INTO t_or_in_variant_const VALUES (toDateTime(5, 'UTC'), toDate(5)), (toDateTime(432000, 'UTC'), toDate(100));

SELECT x FROM t_or_in_variant_const WHERE x = 5::UInt64::Variant(Date, UInt64) OR x = 6::UInt64::Variant(Date, UInt64) OR x = 7::UInt64::Variant(Date, UInt64) ORDER BY x;
SELECT x FROM t_or_in_variant_const WHERE x = 5::UInt64::Variant(Date, UInt64) OR x = 6::UInt64::Variant(Date, UInt64) OR x = 7::UInt64::Variant(Date, UInt64) ORDER BY x SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT d FROM t_or_in_variant_const WHERE d = 5::UInt64::Variant(Date, UInt64) OR d = 6::UInt64::Variant(Date, UInt64) OR d = 7::UInt64::Variant(Date, UInt64) ORDER BY d; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM (EXPLAIN QUERY TREE SELECT x FROM t_or_in_variant_const WHERE x = 5::UInt64::Variant(Date, UInt64) OR x = 6::UInt64::Variant(Date, UInt64) OR x = 7::UInt64::Variant(Date, UInt64)) WHERE explain ILIKE '%function_name: in%';

SELECT x FROM t_or_in_variant_const WHERE x = toDate(5)::Variant(Date, UInt64) OR x = toDate(6)::Variant(Date, UInt64) OR x = toDate(7)::Variant(Date, UInt64) ORDER BY x;
SELECT x FROM t_or_in_variant_const WHERE x = toDate(5)::Dynamic OR x = toDate(6)::Dynamic OR x = toDate(7)::Dynamic ORDER BY x;
SELECT x FROM t_or_in_variant_const WHERE x = toDate(5)::Dynamic OR x = toDate(6)::Dynamic OR x = toDate(7)::Dynamic ORDER BY x SETTINGS optimize_min_equality_disjunction_chain_length = 100;

-- A single-alternative `Variant` constant is rebuilt into the same alternative, so the rewrite still fires.
SELECT x FROM t_or_in_variant_const WHERE x = 5::UInt64::Variant(UInt64) OR x = 6::UInt64::Variant(UInt64) OR x = 7::UInt64::Variant(UInt64) ORDER BY x;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT x FROM t_or_in_variant_const WHERE x = 5::UInt64::Variant(UInt64) OR x = 6::UInt64::Variant(UInt64) OR x = 7::UInt64::Variant(UInt64)) WHERE explain ILIKE '%function_name: in%';

DROP TABLE t_or_in_variant_const;

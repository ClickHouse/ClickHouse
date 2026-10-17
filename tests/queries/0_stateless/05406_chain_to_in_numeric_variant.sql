-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- `equals` on a `Variant` compares the value of the active alternative, so a `UInt8` alternative
-- compares equal to the same number in a `UInt64` alternative, while set membership is keyed on the
-- discriminator and the value. A comparison chain over a `Variant` with several alternatives must
-- not be rewritten into `IN`/`NOT IN`, and `arrayExists` must not become `has`.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;
SET use_variant_default_implementation_for_comparisons = 1;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_chain_numeric_variant;
CREATE TABLE t_chain_numeric_variant (v Variant(UInt8, UInt64)) ENGINE = Memory;
INSERT INTO t_chain_numeric_variant SELECT CAST(toUInt8(1), 'Variant(UInt8, UInt64)');

SELECT 'ground truth';
SELECT variantType(v), v = toUInt64(1), v != toUInt64(1) FROM t_chain_numeric_variant;

SELECT 'equals chain';
SELECT v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3) FROM t_chain_numeric_variant;
SELECT count() FROM t_chain_numeric_variant WHERE v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3);
SELECT v = CAST(toUInt64(1), 'Variant(UInt8, UInt64)')
    OR v = CAST(toUInt64(2), 'Variant(UInt8, UInt64)')
    OR v = CAST(toUInt64(3), 'Variant(UInt8, UInt64)')
FROM t_chain_numeric_variant;

SELECT 'not equals chain';
SELECT v != toUInt64(1) AND v != toUInt64(2) AND v != toUInt64(3) FROM t_chain_numeric_variant;

SELECT 'not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT v FROM t_chain_numeric_variant
    WHERE v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3)) WHERE explain LIKE '%function_name: in%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT v FROM t_chain_numeric_variant
    WHERE v != toUInt64(1) AND v != toUInt64(2) AND v != toUInt64(3)) WHERE explain LIKE '%function_name: notIn%';

SELECT 'arrayExists';
SELECT arrayExists(x -> x = CAST(toUInt64(1), 'Variant(UInt8, UInt64)'), [v])
FROM t_chain_numeric_variant
SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE t_chain_numeric_variant;

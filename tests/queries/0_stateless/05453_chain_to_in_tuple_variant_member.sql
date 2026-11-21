-- A tuple member of type `Variant` can hold NULL without a `Nullable` wrapper. `equals` on tuples
-- propagates such a NULL, while set membership and `has` treat it as a value, so the rewrites of
-- comparison chains into `IN` / `NOT IN` and of `arrayExists` into `has` must not apply.

SET enable_analyzer = 1;
SET use_variant_default_implementation_for_comparisons = 1;

DROP TABLE IF EXISTS t_tuple_variant;
CREATE TABLE t_tuple_variant (v Tuple(Variant(UInt64))) ENGINE = Memory;
INSERT INTO t_tuple_variant SELECT tuple(CAST(toUInt64(3), 'Variant(UInt64)'));

SELECT 'equals chain in WHERE';
SELECT count() FROM t_tuple_variant
WHERE NOT (v = tuple(CAST(NULL, 'Variant(UInt64)'))
        OR v = tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
        OR v = tuple(CAST(toUInt64(2), 'Variant(UInt64)')))
SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_tuple_variant
WHERE NOT (v = tuple(CAST(NULL, 'Variant(UInt64)'))
        OR v = tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
        OR v = tuple(CAST(toUInt64(2), 'Variant(UInt64)')))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'equals chain result';
SELECT v = tuple(CAST(NULL, 'Variant(UInt64)'))
    OR v = tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
    OR v = tuple(CAST(toUInt64(2), 'Variant(UInt64)'))
FROM t_tuple_variant
SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT v = tuple(CAST(NULL, 'Variant(UInt64)'))
    OR v = tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
    OR v = tuple(CAST(toUInt64(2), 'Variant(UInt64)'))
FROM t_tuple_variant
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'notEquals chain in WHERE';
SELECT count() FROM t_tuple_variant
WHERE v != tuple(CAST(NULL, 'Variant(UInt64)'))
  AND v != tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
  AND v != tuple(CAST(toUInt64(2), 'Variant(UInt64)'))
SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_tuple_variant
WHERE v != tuple(CAST(NULL, 'Variant(UInt64)'))
  AND v != tuple(CAST(toUInt64(1), 'Variant(UInt64)'))
  AND v != tuple(CAST(toUInt64(2), 'Variant(UInt64)'))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

INSERT INTO t_tuple_variant SELECT tuple(CAST(NULL, 'Variant(UInt64)'));

SELECT 'arrayExists in WHERE';
SELECT count() FROM t_tuple_variant
WHERE arrayExists(x -> x = tuple(CAST(NULL, 'Variant(UInt64)')), [v])
SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_tuple_variant
WHERE arrayExists(x -> x = tuple(CAST(NULL, 'Variant(UInt64)')), [v])
SETTINGS optimize_rewrite_array_exists_to_has = 0;

SELECT 'arrayExists result';
SELECT arrayExists(x -> x = tuple(CAST(NULL, 'Variant(UInt64)')), [v]) AS r FROM t_tuple_variant ORDER BY r
SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = tuple(CAST(NULL, 'Variant(UInt64)')), [v]) AS r FROM t_tuple_variant ORDER BY r
SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE t_tuple_variant;

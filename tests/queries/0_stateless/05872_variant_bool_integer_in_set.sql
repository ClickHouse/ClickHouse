-- An integer constant must not be stored in the `Bool` alternative of a `Variant` that also has an
-- integer alternative: `IN` must match the same integer rows as `=`, and the `Values` expression
-- fallback must store the value in the integer alternative.
SET enable_variant_type = 1;

DROP TABLE IF EXISTS t_variant_bool_in;
CREATE TABLE t_variant_bool_in (id UInt8, v Variant(Bool, UInt32)) ENGINE = Memory;
INSERT INTO t_variant_bool_in VALUES (1, 2), (2, true), (3, 300);
SELECT 'eq', id FROM t_variant_bool_in WHERE v = 2 ORDER BY id;
SELECT 'in', id FROM t_variant_bool_in WHERE v IN (2) ORDER BY id;
SELECT 'in_uint32', id FROM t_variant_bool_in WHERE v IN (toUInt32(2)) ORDER BY id;
SELECT 'in_300', id FROM t_variant_bool_in WHERE v IN (300) ORDER BY id;
SELECT 'in_true', id FROM t_variant_bool_in WHERE v IN (true) ORDER BY id;
SELECT 'not_in', id FROM t_variant_bool_in WHERE v NOT IN (2) ORDER BY id;
DROP TABLE t_variant_bool_in;

DROP TABLE IF EXISTS t_variant_bool_in_mt;
CREATE TABLE t_variant_bool_in_mt (id UInt8, v Variant(Bool, UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant_bool_in_mt VALUES (1, 2), (2, true), (3, 300);
SELECT 'mt_where', id FROM t_variant_bool_in_mt WHERE v IN (2) ORDER BY id;
SELECT 'mt_prewhere', id FROM t_variant_bool_in_mt PREWHERE v IN (2) ORDER BY id;
DROP TABLE t_variant_bool_in_mt;

SELECT 'nested', [2::UInt32::Variant(Bool, UInt32)] IN ([2]);

SELECT v, variantType(v) FROM format(Values, 'v Variant(Bool, UInt32)', '(1+1)');
SELECT v, variantType(v) FROM format(Values, 'v Variant(Bool, String)', '(2+3)');

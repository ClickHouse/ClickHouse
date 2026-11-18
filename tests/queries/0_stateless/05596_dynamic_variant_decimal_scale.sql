-- A Decimal value must not land in a Decimal alternative of another scale as a raw integer.

DROP TABLE IF EXISTS t_dynamic_decimal_scale;
CREATE TABLE t_dynamic_decimal_scale (m Map(String, Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_decimal_scale VALUES (map('a', toDecimal64(1.23, 2)::Dynamic, 'b', toDecimal64(1.234, 3)::Dynamic, 'c', toDecimal64(5.5, 1)::Dynamic));
SELECT toString(m['a']), toString(m['b']), toString(m['c']), dynamicType(m['b']), dynamicType(m['c']) FROM t_dynamic_decimal_scale;
DROP TABLE t_dynamic_decimal_scale;

SELECT toString(m['b']) FROM values('m Map(String, Dynamic)', map('a', toDecimal64(1.23, 2)::Dynamic, 'b', toDecimal64(1.234, 3)::Dynamic));
SELECT toString(a) FROM values('a Array(Dynamic)', [toDecimal64(1.23, 2)::Dynamic, toDecimal64(1.234, 3)::Dynamic]) ARRAY JOIN a;

-- Same for the other Decimal widths.
SELECT toString(m['b']), dynamicType(m['b']) FROM values('m Map(String, Dynamic)', map('a', toDecimal32(1.23, 2)::Dynamic, 'b', toDecimal32(1.234, 3)::Dynamic));
SELECT toString(m['b']), dynamicType(m['b']) FROM values('m Map(String, Dynamic)', map('a', toDecimal128(1.23, 2)::Dynamic, 'b', toDecimal128(1.234, 3)::Dynamic));
SELECT toString(m['b']), dynamicType(m['b']) FROM values('m Map(String, Dynamic)', map('a', toDecimal256(1.23, 2)::Dynamic, 'b', toDecimal256(1.234, 3)::Dynamic));

DROP TABLE IF EXISTS t_variant_decimal_scale;
CREATE TABLE t_variant_decimal_scale (x Variant(Decimal(18, 2), Array(String))) ENGINE = Memory;
INSERT INTO t_variant_decimal_scale VALUES (toDecimal64(1.234, 3)), (toDecimal64(1.2, 1));
SELECT x, variantType(x) FROM t_variant_decimal_scale;
DROP TABLE t_variant_decimal_scale;

DROP TABLE IF EXISTS t_variant_decimal_scale_widths;
CREATE TABLE t_variant_decimal_scale_widths (x32 Variant(Decimal(9, 2), Array(String)), x128 Variant(Decimal(38, 2), Array(String)), x256 Variant(Decimal(76, 2), Array(String))) ENGINE = Memory;
INSERT INTO t_variant_decimal_scale_widths VALUES (toDecimal32(1.234, 3), toDecimal128(1.234, 3), toDecimal256(1.234, 3));
SELECT x32, variantType(x32), x128, variantType(x128), x256, variantType(x256) FROM t_variant_decimal_scale_widths;
DROP TABLE t_variant_decimal_scale_widths;

DROP TABLE IF EXISTS t_variant_datetime64_scale;
CREATE TABLE t_variant_datetime64_scale (x Variant(DateTime64(6, 'UTC'), String)) ENGINE = Memory;
INSERT INTO t_variant_datetime64_scale VALUES (toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'));
SELECT x, variantType(x) FROM t_variant_datetime64_scale;
DROP TABLE t_variant_datetime64_scale;

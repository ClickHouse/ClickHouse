-- A `Date`/`DateTime`/`Enum` inside a `Dynamic` constant must keep its type in `INSERT ... VALUES`
-- and in the `values` table function, instead of being stored as the underlying integer.
-- https://github.com/ClickHouse/ClickHouse/issues/130088

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_dynamic_values_map;
CREATE TABLE t_dynamic_values_map (id UInt8, m Map(String, Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_map VALUES (1, map('d', CAST(toDate('2020-01-01') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (2, map('dt', CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (3, map('e', CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (4, map('b', CAST(true AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (5, map('n', CAST(NULL AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (6, map('d', toDate('2020-01-01')));
INSERT INTO t_dynamic_values_map SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (7, map('d', CAST(toDate('2020-01-01') AS Dynamic)));
SELECT id, arrayMap(x -> (x.1, toString(x.2), dynamicType(x.2)), CAST(m, 'Array(Tuple(String, Dynamic))')) FROM t_dynamic_values_map ORDER BY id;
DROP TABLE t_dynamic_values_map;

DROP TABLE IF EXISTS t_dynamic_values;
CREATE TABLE t_dynamic_values (id UInt8, d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_values VALUES (1, CAST(toDate('2020-01-01') AS Dynamic));
INSERT INTO t_dynamic_values SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (2, CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic));
INSERT INTO t_dynamic_values SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (3, toDate('2020-01-01'));
SELECT id, toString(d), dynamicType(d) FROM t_dynamic_values ORDER BY id;
DROP TABLE t_dynamic_values;

-- `input_format_null_as_default` still replaces a `NULL` nested in a value with dynamic structure.
DROP TABLE IF EXISTS t_dynamic_values_null;
CREATE TABLE t_dynamic_values_null (t Tuple(a Int32, b Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES ((CAST(NULL AS Nullable(Int32)), CAST('x' AS Dynamic)));
SELECT t.a, toString(t.b), dynamicType(t.b) FROM t_dynamic_values_null;
DROP TABLE t_dynamic_values_null;

SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(toDate('2020-01-01') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', toDate('2020-01-01'));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(true AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', 42);
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', 'abc');
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(NULL AS Dynamic));
SELECT id, toString(d), dynamicType(d) FROM values('id UInt8, d Dynamic', (1, CAST(toDate('2020-01-01') AS Dynamic)), (2, toDateTime('2020-01-01 10:20:30')));

-- `Dynamic` nested in other types and `JSON` in the `values` table function.
SET enable_json_type = 1;
SELECT toString(m), dynamicType(m['d']) FROM values('m Map(String, Dynamic)', map('d', CAST(toDate('2020-01-01') AS Dynamic)));
SELECT toString(m), dynamicType(m['d']) FROM values('m Map(String, Dynamic)', map('d', toDate('2020-01-01')));
SELECT toString(a), dynamicType(a[1]) FROM values('a Array(Dynamic)', [CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic)]);
SELECT toString(t), dynamicType(t.1) FROM values('t Tuple(Dynamic)', tuple(CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic)));
SELECT toString(j), dynamicType(j.d), j.d.:Date FROM values('j JSON', CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON));

-- `input_format_null_as_default` replaces a `NULL` nested in a value with dynamic structure and keeps the types of the other values.
DROP TABLE IF EXISTS t_dynamic_values_null_date;
CREATE TABLE t_dynamic_values_null_date (id UInt8, t Tuple(a Int32, b Dynamic), m Map(String, Tuple(a Array(Int32), b Dynamic))) ENGINE = Memory;
INSERT INTO t_dynamic_values_null_date SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, (CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)), map('k', (CAST([1, NULL] AS Array(Nullable(Int32))), CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic))));
INSERT INTO t_dynamic_values_null_date SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (2, (CAST(5 AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)), map('k', (CAST([1] AS Array(Nullable(Int32))), CAST(toDate('2020-01-01') AS Dynamic))));
SELECT id, t.a, toString(t.b), dynamicType(t.b), toString(m['k'].a), toString(m['k'].b), dynamicType(m['k'].b) FROM t_dynamic_values_null_date ORDER BY id;
DROP TABLE t_dynamic_values_null_date;

-- A string is still parsed as text of the destination type.
SELECT toString(m) FROM values('m Map(String, JSON)', '{\'x\' : \'{"y" : 1}\'}');

-- A typed `NULL` going into `Dynamic` stays `NULL` with `input_format_null_as_default`.
DROP TABLE IF EXISTS t_dynamic_values_typed_null;
CREATE TABLE t_dynamic_values_typed_null (id UInt8, t Tuple(Dynamic), a Array(Dynamic), m Map(String, Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_typed_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, tuple(CAST(NULL AS Nullable(Date))), [CAST(NULL AS Nullable(Int32))], map('k', CAST(NULL AS Nullable(String))));
SELECT id, isNull(t.1), dynamicType(t.1), isNull(a[1]), dynamicType(a[1]), isNull(m['k']), dynamicType(m['k']) FROM t_dynamic_values_typed_null;
DROP TABLE t_dynamic_values_typed_null;

-- An overflowing nullable sibling of `Dynamic` is still converted to `NULL`.
SELECT t.1, isNull(t.1), toString(t.2), dynamicType(t.2) FROM values('t Tuple(Nullable(UInt8), Dynamic)', (256, CAST('x' AS Dynamic)));
SELECT t.1, isNull(t.1) FROM values('t Tuple(Nullable(Int8), Dynamic)', (128, CAST(toDate('2020-01-01') AS Dynamic)));
SELECT toString(a) FROM values('a Array(Tuple(Nullable(UInt8), Dynamic))', [(256, CAST(toDate('2020-01-01') AS Dynamic))]);

-- Tuple elements are converted by position, whatever their names.
SELECT t, dynamicType(t.b) FROM values('t Tuple(a UInt8, b Dynamic)', CAST((1, CAST('keep' AS Dynamic)), 'Tuple(a UInt8, c Dynamic)'));
SELECT t, dynamicType(t.b) FROM values('t Tuple(a UInt8, b Dynamic)', CAST((1, CAST(toDate('2020-01-01') AS Dynamic)), 'Tuple(b UInt8, a Dynamic)'));
SELECT t, dynamicType(t.x.d) FROM values('t Tuple(x Tuple(n UInt8, d Dynamic))', CAST(tuple((1, CAST(toDate('2020-01-01') AS Dynamic))), 'Tuple(y Tuple(m UInt8, e Dynamic))'));

-- Numbers keep the type `Dynamic` stored them with before.
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', toUInt64(42));
SELECT toString(t.2), dynamicType(t.2) FROM values('t Tuple(UInt8, Dynamic)', (1, toUInt64(5)));

-- Types inside `Nullable` and `Variant` destinations, and a `Dynamic` source holding a composite, are kept as by `CAST`.
SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1;
SELECT toString(j), dynamicType(j.d) FROM values('j Nullable(JSON)', CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON));
SELECT toString(v), dynamicType(variantElement(v, 'JSON').d) FROM values('v Variant(JSON, String)', CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON));
SELECT toString(v), dynamicType(variantElement(v, 'Array(Dynamic)')[1]) FROM values('v Variant(Array(Dynamic), String)', [CAST(toDate('2020-01-01') AS Dynamic)]);
SELECT toString(a), dynamicType(a[1]) FROM values('a Array(Dynamic)', CAST([CAST(toDate('2020-01-01') AS Dynamic)] AS Dynamic));

DROP TABLE IF EXISTS t_dynamic_values_variant;
CREATE TABLE t_dynamic_values_variant (id UInt8, d Dynamic, v Variant(JSON, String), w Variant(Array(Dynamic), String)) ENGINE = Memory;
INSERT INTO t_dynamic_values_variant SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, CAST(toUInt16(42) AS Variant(Date, UInt16)), CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON), [CAST(toDate('2020-01-01') AS Dynamic)]);
INSERT INTO t_dynamic_values_variant SELECT
    2, CAST(toUInt16(42) AS Variant(Date, UInt16)), CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON), [CAST(toDate('2020-01-01') AS Dynamic)];
SELECT id, toString(d), dynamicType(d), dynamicType(variantElement(v, 'JSON').d), dynamicType(variantElement(w, 'Array(Dynamic)')[1]) FROM t_dynamic_values_variant ORDER BY id;
DROP TABLE t_dynamic_values_variant;
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(toUInt16(42) AS Variant(Date, UInt16)));

-- A `Field` has neither the width of a `FixedString` nor the element names of a tuple: typed reads still find the values.
SELECT dynamicType(d), dynamicElement(d, 'FixedString(3)') = toFixedString('a', 3) FROM values('d Dynamic', toFixedString('a', 3));
SELECT dynamicType(d), dynamicElement(d, 'Tuple(x UInt8)').x FROM values('d Dynamic', CAST(tuple(1) AS Tuple(x UInt8)));
SELECT dynamicType(a[1]) FROM values('a Array(Dynamic)', [toFixedString('a', 3)]);

-- The alternative of a `Variant` nested in the source is kept, as by `INSERT SELECT`.
SELECT toString(d), variantType(dynamicElement(d, 'Array(Variant(Date, UInt16))')[1]) FROM values('d Dynamic', [CAST(toUInt16(42) AS Variant(Date, UInt16))]);
DROP TABLE IF EXISTS t_dynamic_values_nested_variant;
CREATE TABLE t_dynamic_values_nested_variant (id UInt8, a Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_nested_variant SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, CAST([CAST(toUInt16(42) AS Variant(Date, UInt16))] AS Array(Variant(Date, UInt16))));
INSERT INTO t_dynamic_values_nested_variant SELECT 2, CAST([CAST(toUInt16(42) AS Variant(Date, UInt16))] AS Array(Variant(Date, UInt16)));
SELECT id, toString(a), dynamicType(a[1]) FROM t_dynamic_values_nested_variant ORDER BY id;
DROP TABLE t_dynamic_values_nested_variant;

-- `JSON` carried by a `Dynamic` or `Variant` source keeps the types inside, as without the carrier.
SELECT toString(j), dynamicType(j.d) FROM values('j JSON', CAST(CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON) AS Dynamic));
SELECT toString(j), dynamicType(j.d) FROM values('j JSON', CAST(CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON) AS Variant(JSON, String)));
SELECT toString(j), dynamicType(j.d) FROM values('j Nullable(JSON)', CAST(CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON) AS Variant(JSON, String)));
SELECT toString(a), dynamicType(a[1].d) FROM values('a Array(JSON)', CAST([CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON)] AS Dynamic));

-- `input_format_null_as_default` replaces the nulls inside a `Dynamic` or `Variant` source too, keeping the other types.
DROP TABLE IF EXISTS t_dynamic_values_carrier_null;
CREATE TABLE t_dynamic_values_carrier_null (id UInt8, x Tuple(Int32, Dynamic), a Array(Tuple(Int32, Dynamic))) ENGINE = Memory;
INSERT INTO t_dynamic_values_carrier_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, CAST((CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)) AS Dynamic),
        CAST([(CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic))] AS Dynamic));
INSERT INTO t_dynamic_values_carrier_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (2, CAST((CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)) AS Variant(Tuple(Nullable(Int32), Dynamic), String)), []);
INSERT INTO t_dynamic_values_carrier_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (3, (CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)), []);
SELECT id, x.1, toString(x.2), dynamicType(x.2), toString(a), dynamicType(a[1].2) FROM t_dynamic_values_carrier_null ORDER BY id;
DROP TABLE t_dynamic_values_carrier_null;

-- A `Field` has no custom type names: `Point` and `LineString` are kept, and typed reads find the values.
SELECT dynamicType(d), dynamicElement(d, 'Point') FROM values('d Dynamic', CAST((1., 2.) AS Point));
SELECT dynamicType(d), dynamicElement(d, 'LineString') FROM values('d Dynamic', CAST([(1., 2.), (3., 4.)] AS LineString));
SELECT dynamicType(d), d FROM values('d Dynamic', true);

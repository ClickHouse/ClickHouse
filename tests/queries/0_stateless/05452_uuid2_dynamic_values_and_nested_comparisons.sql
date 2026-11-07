-- `UUID` and `UUID2` share the `Field` representation and differ in the layout of the value. The `Field` of a `Dynamic`
-- value keeps its UUID values in the layout of `UUID`, so the value survives a round trip through a `Field`.

SET allow_suspicious_variant_types = 1;

-- Text conversion of a value of a `Variant` alternative that is a collection, and of a `Dynamic` value.
SELECT s FROM values('s String', [CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001')) AS Variant(Tuple(UUID2), String))]);
SELECT s FROM values('s String', CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic));
SELECT s FROM values('s UUID', CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic));

DROP TABLE IF EXISTS t_uuid2_dynamic_to_string;
CREATE TABLE t_uuid2_dynamic_to_string (s String) ENGINE = Memory;
INSERT INTO t_uuid2_dynamic_to_string SETTINGS input_format_values_deduce_templates_of_expressions = 0
    VALUES (CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic));
SELECT s FROM t_uuid2_dynamic_to_string;
DROP TABLE t_uuid2_dynamic_to_string;

-- A `Dynamic` parameter and a `Dynamic` argument of an aggregate function.
SELECT groupArrayInsertAt(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic), 2)(toUUID('00000000-0000-0000-0000-000000000001'), toUInt64(1));
SELECT groupArrayInsertAt(CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Dynamic), 2)(toUUID2('00000000-0000-0000-0000-000000000001'), toUInt64(1));
SELECT groupArrayInsertAt(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic), toUInt64(0));
SELECT groupArrayInsertAt(CAST([toUUID2('00000000-0000-0000-0000-000000000001')] AS Dynamic), toUInt64(0));

-- `optimize_redundant_comparisons` does not fold comparisons of nested values in the order of the other UUID type.
SELECT count()
FROM values('u Tuple(UUID1)', tuple(toUUID('00000000-0000-0000-0000-000000000002')))
WHERE u = tuple(toUUID('00000000-0000-0000-0000-000000000002'))
  AND u < tuple(toUUID2('00000000-0000-0001-0000-000000000001'))
SETTINGS optimize_redundant_comparisons = 1;
SELECT count()
FROM values('u Tuple(UUID1)', tuple(toUUID('00000000-0000-0000-0000-000000000002')))
WHERE u = tuple(toUUID('00000000-0000-0000-0000-000000000002'))
  AND u < tuple(toUUID2('00000000-0000-0001-0000-000000000001'))
SETTINGS optimize_redundant_comparisons = 0;
SELECT count()
FROM values('u Array(UUID1)', [toUUID('00000000-0000-0000-0000-000000000002')])
WHERE u = [toUUID('00000000-0000-0000-0000-000000000002')]
  AND u < [toUUID2('00000000-0000-0001-0000-000000000001')]
SETTINGS optimize_redundant_comparisons = 1;

-- `transform` and simple `CASE` keep the mappings of nested values of the other UUID type.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    transform(tuple(toUUID2(s)), [tuple(toUUID(s))], [7], 0),
    transform(tuple(toUUID(s)), [tuple(toUUID2(s))], [7], 0),
    transform([toUUID2(s)], [[toUUID(s)]], [7], 0),
    CASE tuple(toUUID2(s)) WHEN tuple(toUUID(s)) THEN 7 ELSE 0 END,
    CASE tuple(toUUID(s)) WHEN tuple(toUUID2(s)) THEN 7 ELSE 0 END;

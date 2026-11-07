-- Accessing named alternatives on Variant expressions should work like Variant subcolumns.
SET enable_variant_type = 1;
SET optimize_functions_to_subcolumns = 0;

-- Geometry expression, as in #126436.
SELECT readWKB(wkb(readWKT('POLYGON((0 0,1 0,1 1,0 0))'))).Polygon
    = variantElement(readWKB(wkb(readWKT('POLYGON((0 0,1 0,1 1,0 0))'))), 'Polygon');

-- A nonmatching alternative returns the same default as a Variant subcolumn.
SELECT empty(readWKB(wkb(readWKT('POINT(1 2)'))).Polygon);

-- Nonconstant Variant expressions support dot access too.
SELECT (number::Variant(String, UInt64)).UInt64 FROM numbers(2);

SELECT tupleElement(v, 'UInt64'), tupleElement(v, 'String')
FROM
(
    SELECT
        number,
        if(
            number % 2 = 0,
            number::Variant(String, UInt64),
            concat('s', toString(number))::Variant(String, UInt64)) AS v
    FROM numbers(4)
)
ORDER BY number;

-- Match variantElement for nested arrays of Variants, including missing alternatives.
SELECT tupleElement([[(number % 2 ? NULL : number)::Variant(String, UInt64)]], 'UInt64') FROM numbers(4);

-- Missing alternatives support tupleElement's optional default argument.
SELECT tupleElement(number::Variant(String, UInt64), 'Int64', -1) FROM numbers(1);
SELECT tupleElement(number::Variant(String, UInt64), 'Int64') FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tupleElement(number::Variant(String, UInt64), 1) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Primitive alternatives get a Nullable result; complex alternatives use their default.
SELECT toTypeName(tupleElement(number::Variant(String, UInt64), 'UInt64')) FROM numbers(1);
SELECT toTypeName(tupleElement(number::Variant(Array(UInt64), UInt64), 'Array(UInt64)')),
    empty(tupleElement(number::Variant(Array(UInt64), UInt64), 'Array(UInt64)')) FROM numbers(1);

-- The optional default can be an expression.
SELECT tupleElement(number::Variant(String, UInt64), 'Int64', toInt64(number)) FROM numbers(2);

-- Check stored Variant columns with subcolumn optimization disabled and enabled.
DROP TABLE IF EXISTS test_variant_expression_subcolumns;
CREATE TABLE test_variant_expression_subcolumns (id UInt8, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_variant_expression_subcolumns
SELECT toUInt8(number),
    if(number % 2 = 0, number::Variant(String, UInt64), concat('s', toString(number))::Variant(String, UInt64))
FROM numbers(4);

SELECT id, tupleElement(v, 'UInt64') FROM test_variant_expression_subcolumns ORDER BY id;

SET optimize_functions_to_subcolumns = 1;
SELECT tupleElement(number::Variant(String, UInt64), 'UInt64') FROM numbers(2);
SELECT id, tupleElement(v, 'UInt64') FROM test_variant_expression_subcolumns ORDER BY id;
SELECT id, variantType(v) FROM test_variant_expression_subcolumns
WHERE tupleElement(v, 'UInt64') = 2 ORDER BY id;
DROP TABLE test_variant_expression_subcolumns;

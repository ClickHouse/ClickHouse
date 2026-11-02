-- The typed paths of `JSON` keep the layout of their types: converting them to a string keeps the identifier.
SELECT s FROM values('s String',
    CAST('{"u":"00000000-0000-0000-0000-000000000001"}' AS JSON(u UUID2)));

-- The same for a typed `JSON` value carried by `Dynamic` through an aggregate state.
SELECT toJSONString(groupArrayInsertAt(
    CAST(CAST('{"u":"00000000-0000-0000-0000-000000000001"}' AS JSON(u UUID2)) AS Dynamic),
    toUInt64(0)));

-- Converting between typed `JSON` schemas with a different UUID layout keeps the identifier, in both directions.
SELECT j FROM values('j JSON(u UUID1)',
    CAST('{"u":"00000000-0000-0000-0000-000000000001"}' AS JSON(u UUID2)));
SELECT j FROM values('j JSON(u UUID2)',
    CAST('{"u":"00000000-0000-0000-0000-000000000001"}' AS JSON(u UUID1)));
-- And to a dynamic path.
SELECT j FROM values('j JSON',
    CAST('{"u":"00000000-0000-0000-0000-000000000001"}' AS JSON(u UUID2)));

-- A callable formal parameter of a lambda shadows a function, so its argument is an ordinary string.
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda ENGINE = Memory AS
WITH (defaultValueOfTypeName -> defaultValueOfTypeName('UUID')) AS invoke,
     (x -> x) AS identity_lambda
SELECT invoke(identity_lambda) AS s;
SELECT s FROM t_lambda;
DROP TABLE t_lambda;
SET uuid_type_version = 1;

-- A `Variant` value loses its discriminator in a constant array: different identifiers whose bits coincide
-- in the opposite layouts must not match, whether the array is constant or not.
SET allow_suspicious_variant_types = 1;
WITH '00000000-0000-0000-0000-000000000001' AS s,
     '00000000-0000-0001-0000-000000000000' AS t
SELECT
    has([CAST(toUUID2(s) AS Variant(UUID, UUID2))], CAST(toUUID(t) AS Variant(UUID, UUID2))),
    has(materialize([CAST(toUUID2(s) AS Variant(UUID, UUID2))]), CAST(toUUID(t) AS Variant(UUID, UUID2))),
    has([CAST(toUUID2(s) AS Variant(UUID, UUID2))], CAST(toUUID2(s) AS Variant(UUID, UUID2))),
    has(materialize([CAST(toUUID2(s) AS Variant(UUID, UUID2))]), CAST(toUUID2(s) AS Variant(UUID, UUID2)));

-- `UUID2` can be obfuscated; the version and the variant of the identifier are kept.
DROP TABLE IF EXISTS t_obfuscate;
CREATE TABLE t_obfuscate (u UUID2) ENGINE = Memory;
INSERT INTO t_obfuscate VALUES ('01234567-89ab-4cde-8f01-23456789abcd');
SELECT toTypeName(u), u != toUUID2('01234567-89ab-4cde-8f01-23456789abcd'), substring(toString(u), 15, 1), substring(toString(u), 20, 1) IN ('8', '9', 'a', 'b')
FROM obfuscate(SELECT u FROM t_obfuscate) LIMIT 1 SETTINGS obfuscate_seed = 'default-test-seed';
DROP TABLE t_obfuscate;

SET in_null_as_unknown = 1;

SELECT [toUInt8(1)] IN ([CAST(toUInt64(1) AS Dynamic)], NULL),
    [toUInt8(1)] IN (materialize([CAST(toUInt64(1) AS Dynamic)]), NULL);
SELECT map('a', '1') IN (map('a', CAST(toUInt64(1) AS Variant(UInt64, String))), NULL),
    map('a', '1') IN (materialize(map('a', CAST(toUInt64(1) AS Variant(UInt64, String)))), NULL);

DROP TABLE IF EXISTS squality_nested_carrier_members;
CREATE TABLE squality_nested_carrier_members
(
    a Array(UInt8),
    b Array(Dynamic),
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;

INSERT INTO squality_nested_carrier_members SELECT [toUInt8(1)], [CAST(toUInt64(1) AS Dynamic)];
SELECT count() FROM squality_nested_carrier_members;
SELECT count() FROM squality_nested_carrier_members
WHERE a IN ([CAST(toUInt64(1) AS Dynamic)], NULL);
SELECT count() FROM squality_nested_carrier_members
WHERE a IN (materialize([CAST(toUInt64(1) AS Dynamic)]), NULL);
DROP TABLE squality_nested_carrier_members;

SELECT map('a', '1') IN
    (SELECT map('a', CAST(toUInt64(1) AS Variant(UInt64, String)))); -- { serverError NOT_IMPLEMENTED }

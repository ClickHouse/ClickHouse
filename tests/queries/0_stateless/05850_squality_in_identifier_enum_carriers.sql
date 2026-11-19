SET in_null_as_unknown = 1;

WITH toUUID('00000000-0000-0000-0000-000000000001') AS u,
     CAST('00000000-0000-0000-0000-000000000001' AS Dynamic) AS d
SELECT u IN [d], u IN materialize([d]), u IN materialize(d);

SELECT
    CAST('a' AS Enum8('a' = 1)) IN (CAST('a' AS Dynamic), NULL),
    CAST('a' AS Enum8('a' = 1)) IN (materialize(CAST('a' AS Dynamic)), NULL),
    CAST('a' AS Enum8('a' = 1)) IN materialize([CAST('a' AS Dynamic), NULL]);

DROP TABLE IF EXISTS squality_enum_carrier_check;
CREATE TABLE squality_enum_carrier_check
(
    x Enum8('a' = 1),
    y Dynamic,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;
INSERT INTO squality_enum_carrier_check SELECT CAST('a' AS Enum8('a' = 1)), CAST('a' AS Dynamic);
SELECT count() FROM squality_enum_carrier_check WHERE x IN (y, NULL);
DROP TABLE squality_enum_carrier_check;

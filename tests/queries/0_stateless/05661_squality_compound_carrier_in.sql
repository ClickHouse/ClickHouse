SELECT
    map('a', toUInt8(1)) IN CAST(map('a', toUInt8(1)) AS Dynamic),
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Dynamic)),
    map('a', toUInt8(1)) IN (materialize(CAST(map('a', toUInt8(1)) AS Dynamic)), NULL),
    map('a', toUInt8(1)) NOT IN (materialize(CAST(map('a', toUInt8(1)) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN materialize(CAST('other' AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN (materialize(CAST('other' AS Variant(Map(String, UInt8), String))), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toUInt8(1)) IN [CAST(map('a', toUInt8(1)) AS Dynamic)],
    map('a', toUInt8(1)) IN [CAST('other' AS Dynamic)],
    map('a', toUInt8(1)) IN [CAST(map(toUInt8(1), toUInt8(1)) AS Dynamic)],
    [toUInt8(1)] IN [CAST([toUInt8(1)] AS Dynamic)]
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM numbers(1)
WHERE map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Dynamic))
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Dynamic)),
    map('a', toUInt8(1)) IN materialize(CAST(map(toUInt8(1), toUInt8(1)) AS Dynamic)),
    map('a', toUInt8(1)) IN materialize(CAST('other' AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN materialize(CAST('other' AS Variant(Map(UInt8, UInt8), String)))
SETTINGS in_null_as_unknown = 1, short_circuit_function_evaluation = 'disable';

SELECT
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt16(1)) AS Dynamic)),
    map('a', toUInt8(1)) IN materialize(CAST(map('a', '1') AS Dynamic)),
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt16(1)) AS Variant(Map(String, UInt16), String)))
SETTINGS in_null_as_unknown = 1, short_circuit_function_evaluation = 'disable';

SET in_null_as_unknown = 1;
CREATE TABLE squality_compound_carrier_check
(
    m Map(String, UInt8),
    v Dynamic,
    CONSTRAINT c CHECK m IN (v, NULL)
) ENGINE = Memory;
INSERT INTO squality_compound_carrier_check SELECT map('a', toUInt8(1)), CAST(map('a', toUInt8(1)) AS Dynamic);
SELECT m IN (v, NULL) FROM squality_compound_carrier_check;
DROP TABLE squality_compound_carrier_check;

-- A polygon dictionary fills its result column with one branch per attribute type, and with a mask
-- branch when dictGetOrDefault leaves the default expression to be evaluated only for the rows that
-- found no polygon. Behaviour: docs/en/sql-reference/dictionaries/index.md (polygon dictionaries)
-- and docs/en/sql-reference/functions/ext-dict-functions.md (dictGet, dictGetOrDefault).

SET short_circuit_function_evaluation = 'enable';
-- The randomizer varies the threshold; the test needs the default lazy execution.
SET short_circuit_function_evaluation_for_nulls_threshold = 1;

DROP DICTIONARY IF EXISTS polygon_typed_dict;
DROP TABLE IF EXISTS polygon_typed_source;
DROP TABLE IF EXISTS polygon_typed_points;

CREATE TABLE polygon_typed_source
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    s String,
    n UInt64
) ENGINE = Memory;

-- Two disjoint rectangles: one east of x = 0, one west of it.
INSERT INTO polygon_typed_source VALUES
    ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], [1, 2], map('east', 1), '{"side":"east"}', 'east', 10),
    ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], [3], map('west', 2), '{"side":"west"}', 'west', 20);

CREATE DICTIONARY polygon_typed_dict
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    arr Array(UInt64),
    m Map(String, UInt64),
    j JSON,
    s String,
    n UInt64
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygon_typed_source' DB currentDatabase()))
LIFETIME(0)
LAYOUT(POLYGON());

-- `miss` is 1 on the only point that lies outside both rectangles.
CREATE TABLE polygon_typed_points (x Float64, y Float64, miss UInt64) ENGINE = Memory;
INSERT INTO polygon_typed_points VALUES (0.5, 0, 0), (-0.5, 0, 0), (10, 10, 1);

-- dictGet: the point outside every polygon gets the null value of the attribute type,
-- the empty Map and the empty JSON object.
SELECT 'dictGet';
SELECT x, dictGet('polygon_typed_dict', 'm', (x, y)), dictGet('polygon_typed_dict', 'j', (x, y))
FROM polygon_typed_points ORDER BY x;

-- dictGetOrDefault with a default that cannot be evaluated eagerly: intDiv(1, miss) divides by zero
-- on the two rows that do find a polygon, so getting rows back at all proves that the default was
-- computed for the third row only.
SELECT 'dictGetOrDefault';
SELECT x,
    dictGetOrDefault('polygon_typed_dict', 'arr', (x, y), [intDiv(1, miss)]),
    dictGetOrDefault('polygon_typed_dict', 'm', (x, y), map('d', intDiv(1, miss))),
    dictGetOrDefault('polygon_typed_dict', 'j', (x, y), concat('{"d":', toString(intDiv(1, miss)), '}')::JSON),
    dictGetOrDefault('polygon_typed_dict', 's', (x, y), toString(intDiv(1, miss))),
    dictGetOrDefault('polygon_typed_dict', 'n', (x, y), intDiv(1, miss))
FROM polygon_typed_points ORDER BY x;

-- Without short circuit the very same default is evaluated for every row.
SELECT x, dictGetOrDefault('polygon_typed_dict', 'n', (x, y), intDiv(1, miss))
FROM polygon_typed_points ORDER BY x
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }

DROP DICTIONARY polygon_typed_dict;
DROP TABLE polygon_typed_points;
DROP TABLE polygon_typed_source;

SELECT
    [toDate('1970-01-01')] IN ([toUInt32(65536)], NULL),
    [toDate('1970-01-01')] IN (materialize([toUInt32(65536)]), NULL),
    [toDate('1970-01-01')] NOT IN (materialize([toUInt32(65536)]), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toDate('1970-01-01')) IN (materialize(map('a', toUInt32(65536))), NULL)
SETTINGS in_null_as_unknown = 1;

SET in_null_as_unknown = 1;
CREATE TABLE squality_compound_temporal_check
(
    x DateTime('UTC'),
    y UInt64,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;
INSERT INTO squality_compound_temporal_check VALUES ('2106-02-07 06:28:15', 4294967296); -- { serverError VIOLATED_CONSTRAINT }
SELECT x IN (y, NULL)
FROM (SELECT toDateTime('2106-02-07 06:28:15', 'UTC') AS x, materialize(toUInt64(4294967296)) AS y);
DROP TABLE squality_compound_temporal_check;

CREATE TABLE squality_temporal_string_check
(
    x DateTime('UTC'),
    y String,
    CONSTRAINT c CHECK x NOT IN [y]
) ENGINE = Memory;
INSERT INTO squality_temporal_string_check VALUES ('2020-01-01 00:00:00', 'not-a-date');
SELECT x NOT IN [y] FROM squality_temporal_string_check;
DROP TABLE squality_temporal_string_check;

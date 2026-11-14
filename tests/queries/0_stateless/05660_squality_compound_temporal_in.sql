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
INSERT INTO squality_compound_temporal_check VALUES ('2106-02-07 06:28:15', 4294967296);
SELECT x IN (y, NULL) FROM squality_compound_temporal_check;
DROP TABLE squality_compound_temporal_check;

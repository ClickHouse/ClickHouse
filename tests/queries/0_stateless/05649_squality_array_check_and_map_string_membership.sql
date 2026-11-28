SET in_null_as_unknown = 1;

SELECT m IN (s, NULL), m NOT IN (s, NULL)
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toString(map('a', toUInt8(1)))) AS s);

SELECT m IN (s, NULL), m NOT IN (s, NULL)
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toString(map('b', toUInt8(1)))) AS s);

SELECT count()
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toString(map('a', toUInt8(1)))) AS s)
WHERE m IN (s, NULL);

DROP TABLE IF EXISTS squality_check_array_datetime_membership;
CREATE TABLE squality_check_array_datetime_membership
(
    x Date,
    y DateTime,
    CONSTRAINT c CHECK x NOT IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_datetime_membership VALUES ('2000-01-01', '2000-01-01 12:00:00'); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_check_array_datetime_membership;

DROP TABLE IF EXISTS squality_check_array_datetime_match;
CREATE TABLE squality_check_array_datetime_match
(
    x Date,
    y DateTime,
    CONSTRAINT c CHECK x IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_datetime_match VALUES ('2000-01-01', '2000-01-01 12:00:00');
SELECT x IN [y] FROM squality_check_array_datetime_match;
DROP TABLE squality_check_array_datetime_match;

DROP TABLE IF EXISTS squality_check_array_datetime64_match;
CREATE TABLE squality_check_array_datetime64_match
(
    x Date32,
    y DateTime64(3),
    CONSTRAINT c CHECK x IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_datetime64_match VALUES ('2000-01-01', '2000-01-01 12:00:00.123');
SELECT x IN [y] FROM squality_check_array_datetime64_match;
DROP TABLE squality_check_array_datetime64_match;

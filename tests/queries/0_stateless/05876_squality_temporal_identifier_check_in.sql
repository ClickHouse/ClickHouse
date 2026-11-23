SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_temporal_check_in;
CREATE TABLE squality_temporal_check_in
(
    x Date,
    y Date,
    z UInt32,
    CONSTRAINT c CHECK x IN (y, z, NULL)
) ENGINE = Memory;

INSERT INTO squality_temporal_check_in VALUES ('1970-01-01', '1970-01-02', 65536); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM squality_temporal_check_in;
DROP TABLE squality_temporal_check_in;

CREATE TABLE squality_temporal_check_in
(
    x Date,
    y Date,
    s String,
    CONSTRAINT c CHECK x NOT IN (y, s)
) ENGINE = Memory;

INSERT INTO squality_temporal_check_in VALUES ('2000-01-02', '2000-01-01', 'bad-date');
SELECT x NOT IN (y, s) FROM squality_temporal_check_in;
DROP TABLE squality_temporal_check_in;

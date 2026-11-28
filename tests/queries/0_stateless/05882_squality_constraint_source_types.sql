SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_constraint_temporal_sources;
CREATE TABLE squality_constraint_temporal_sources
(
    x Date,
    y Date32,
    z UInt32,
    CONSTRAINT c CHECK x IN (y, z, NULL)
) ENGINE = Memory;

INSERT INTO squality_constraint_temporal_sources VALUES ('1970-01-01', '1970-01-02', 65536); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM squality_constraint_temporal_sources;
DROP TABLE squality_constraint_temporal_sources;

DROP TABLE IF EXISTS squality_constraint_enum_sources;
CREATE TABLE squality_constraint_enum_sources
(
    x Enum8('a' = 1, 'b' = 2),
    y Enum8('a' = 1, 'b' = 2),
    s String,
    CONSTRAINT c CHECK x IN (y, s)
) ENGINE = Memory;

INSERT INTO squality_constraint_enum_sources VALUES ('a', 'b', '1');
SELECT x IN (y, s), count() FROM squality_constraint_enum_sources GROUP BY x, y, s;
DROP TABLE squality_constraint_enum_sources;

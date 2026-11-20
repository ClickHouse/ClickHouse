SET in_null_as_unknown = 1;

SELECT
    toDate('2000-01-01') IN
        (toDateTime('2000-01-01 12:00:00'), CAST(toUInt8(1) AS Dynamic), NULL),
    toDate('2000-01-01') IN
        (materialize(toDateTime('2000-01-01 12:00:00')),
         materialize(CAST(toUInt8(1) AS Dynamic)), NULL);

SELECT isNull(toDate('2000-01-01') IN
    (materialize(toUInt64(1)), materialize(CAST(toUInt8(2) AS Dynamic)), NULL));

DROP TABLE IF EXISTS squality_mixed_temporal_list_check;
CREATE TABLE squality_mixed_temporal_list_check
(
    x Date,
    y DateTime,
    z Dynamic,
    CONSTRAINT c CHECK x IN (y, z, NULL)
) ENGINE = Memory;
INSERT INTO squality_mixed_temporal_list_check
SELECT toDate('2000-01-01'), toDateTime('2000-01-01 12:00:00'), CAST(toUInt8(1) AS Dynamic);
SELECT count() FROM squality_mixed_temporal_list_check WHERE x IN (y, z, NULL);
DROP TABLE squality_mixed_temporal_list_check;

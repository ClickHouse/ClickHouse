SET in_null_as_unknown = 1;

SELECT
    isNull(toUInt8(0) IN (toDate(256), CAST(toUInt8(1) AS Dynamic), NULL)),
    isNull(toUInt8(0) IN
        (materialize(toDate(256)), materialize(CAST(toUInt8(1) AS Dynamic)), NULL));

SELECT
    toDate('2000-01-01') IN
        (materialize(CAST(toDateTime('2000-01-01 12:00:00') AS Dynamic)), NULL),
    toDate('2000-01-01') IN
        materialize([CAST(toDateTime('2000-01-01 12:00:00') AS Dynamic), NULL]);

DROP TABLE IF EXISTS squality_temporal_carrier_array_check;
CREATE TABLE squality_temporal_carrier_array_check
(
    x Date,
    y Dynamic,
    CONSTRAINT c CHECK x IN [y, NULL]
) ENGINE = Memory;

INSERT INTO squality_temporal_carrier_array_check
SELECT toDate('2000-01-01'), CAST(toDateTime('2000-01-01 12:00:00') AS Dynamic);
SELECT count() FROM squality_temporal_carrier_array_check;

DROP TABLE squality_temporal_carrier_array_check;

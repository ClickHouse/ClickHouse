SET in_null_as_unknown = 1;

SELECT isNull(toDate('1970-01-01') IN (materialize(CAST(toUInt64(65536) AS Dynamic)), NULL));

DROP TABLE IF EXISTS squality_temporal_dynamic_check;
CREATE TABLE squality_temporal_dynamic_check
(
    x Date,
    y Dynamic,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;

INSERT INTO squality_temporal_dynamic_check
SELECT toDate('1970-01-01'), CAST(toUInt64(65536) AS Dynamic); -- { serverError VIOLATED_CONSTRAINT }

DROP TABLE squality_temporal_dynamic_check;

SET in_null_as_unknown = 1;

SELECT
    isNull([toUInt8(0)] IN ([toDate(256)], NULL)),
    isNull([toUInt8(0)] IN (materialize([toDate(256)]), NULL));

DROP TABLE IF EXISTS squality_compound_temporal_source_filter;
CREATE TABLE squality_compound_temporal_source_filter (a Array(UInt8)) ENGINE = Memory;
INSERT INTO squality_compound_temporal_source_filter VALUES ([0]);

SELECT count() FROM squality_compound_temporal_source_filter
WHERE a IN ([toDate(256)], NULL);
SELECT count() FROM squality_compound_temporal_source_filter
WHERE a IN (materialize([toDate(256)]), NULL);

SELECT isNull(map('a', toUInt8(0)) IN (materialize(map('a', toDate(256))), NULL));

DROP TABLE IF EXISTS squality_compound_temporal_source_check;
CREATE TABLE squality_compound_temporal_source_check
(
    a Array(UInt8),
    b Array(Date),
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;

INSERT INTO squality_compound_temporal_source_check
SELECT [toUInt8(0)], [toDate(256)]; -- { serverError VIOLATED_CONSTRAINT }

DROP TABLE squality_compound_temporal_source_check;
DROP TABLE squality_compound_temporal_source_filter;

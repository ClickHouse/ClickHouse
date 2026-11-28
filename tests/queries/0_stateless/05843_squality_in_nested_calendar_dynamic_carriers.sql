SET in_null_as_unknown = 1;
SET allow_experimental_variant_type = 1;

SELECT [tuple(toDate('2000-01-01'))] IN
    (materialize(CAST([tuple(toDateTime('2000-01-01 00:00:00'))] AS Dynamic)), NULL); -- { serverError NOT_IMPLEMENTED }
SELECT map('a', toDate('2000-01-01')) IN
    (materialize(CAST(map('a', toDateTime('2000-01-01 00:00:00')) AS Dynamic)), NULL); -- { serverError NOT_IMPLEMENTED }
SELECT map('a', toDate('2000-01-01')) IN
    (materialize(CAST(map('a', toDateTime('2000-01-01 00:00:00')), 'Variant(Map(String, DateTime), String)')), NULL); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_map_calendar_carrier_check;
CREATE TABLE squality_map_calendar_carrier_check
(
    a Map(String, Date),
    b Dynamic,
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;

INSERT INTO squality_map_calendar_carrier_check
SELECT map('a', toDate('2000-01-01')), CAST(map('a', toDateTime('2000-01-01 00:00:00')) AS Dynamic); -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_map_calendar_carrier_check;

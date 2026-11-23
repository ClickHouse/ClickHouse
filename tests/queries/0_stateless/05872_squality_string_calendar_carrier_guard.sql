SET in_null_as_unknown = 1;

SELECT [toDate('2000-01-01')] IN
    (materialize(toString([toDateTime('2000-01-01 00:00:00')])), NULL); -- { serverError NOT_IMPLEMENTED }

SELECT map('a', toDate('2000-01-01')) IN
    (materialize(toString(map('a', toDateTime('2000-01-01 00:00:00')))), NULL); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_string_calendar_carrier_check;
CREATE TABLE squality_string_calendar_carrier_check
(
    a Array(Date),
    b String,
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;

INSERT INTO squality_string_calendar_carrier_check
SELECT [toDate('2000-01-01')], toString([toDateTime('2000-01-01 00:00:00')]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_string_calendar_carrier_check;

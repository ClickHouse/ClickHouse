SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_check_nested_calendar_guard;
CREATE TABLE squality_check_nested_calendar_guard
(
    a Array(Date),
    b Array(DateTime),
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;

INSERT INTO squality_check_nested_calendar_guard VALUES (['2000-01-01'], ['2000-01-01 00:00:00']); -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_check_nested_calendar_guard;

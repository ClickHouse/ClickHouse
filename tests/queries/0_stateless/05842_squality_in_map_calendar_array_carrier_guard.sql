SET in_null_as_unknown = 1;

SELECT map('a', toDate('2000-01-01')) IN
    [map('a', toDateTime('2000-01-01 00:00:00'))]; -- { serverError NOT_IMPLEMENTED }
SELECT map('a', toDate('2000-01-01')) IN
    materialize([map('a', toDateTime('2000-01-01 00:00:00'))]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_map_calendar_filter;
CREATE TABLE squality_map_calendar_filter (m Map(String, Date)) ENGINE = Memory;
INSERT INTO squality_map_calendar_filter SELECT map('a', toDate('2000-01-01'));

SELECT count() FROM squality_map_calendar_filter
WHERE m IN [map('a', toDateTime('2000-01-01 00:00:00'))]; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM squality_map_calendar_filter
WHERE m IN materialize([map('a', toDateTime('2000-01-01 00:00:00'))]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_map_calendar_filter;

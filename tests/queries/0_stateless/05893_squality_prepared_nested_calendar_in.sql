SET in_null_as_unknown = 1;

SELECT [toDateTime('2000-01-01 12:00:00')] IN
    (SELECT [toDate('2000-01-01')]); -- { serverError NOT_IMPLEMENTED }

SELECT [toDate('2000-01-01')] IN
    (SELECT [toDateTime('2000-01-01 12:00:00')]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_nested_calendar_set;
CREATE TABLE squality_nested_calendar_set (k Array(Date)) ENGINE = Set;
INSERT INTO squality_nested_calendar_set SELECT [toDate('2000-01-01')];

SELECT [toDateTime('2000-01-01 12:00:00')] IN squality_nested_calendar_set; -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_nested_calendar_set;

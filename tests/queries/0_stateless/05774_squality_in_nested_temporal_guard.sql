SELECT [toDate('2000-01-01')] IN [toDateTime('2000-01-01 00:00:00')]
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT [toDate('2000-01-01')] IN materialize([toDateTime('2000-01-01 00:00:00')])
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT count()
FROM (SELECT [toDate('2000-01-01')] AS a)
WHERE a IN [toDateTime('2000-01-01 00:00:00')]
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT count()
FROM (SELECT [toDate('2000-01-01')] AS a)
WHERE a IN materialize([toDateTime('2000-01-01 00:00:00')])
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

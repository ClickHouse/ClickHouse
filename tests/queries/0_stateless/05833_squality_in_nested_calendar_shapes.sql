SELECT [tuple(toDate('2000-01-01'))] IN [tuple(toDateTime('2000-01-01 00:00:00'))]
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT [tuple(toDate('2000-01-01'))] IN materialize([tuple(toDateTime('2000-01-01 00:00:00'))])
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT map('a', toDate('2000-01-01')) IN map('a', toDateTime('2000-01-01 00:00:00'))
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT map('a', toDate('2000-01-01')) IN materialize(map('a', toDateTime('2000-01-01 00:00:00')))
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

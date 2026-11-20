SELECT
    isNull(toDate('2000-01-01') IN (SELECT NULL)),
    isNull(toDate('2000-01-01') NOT IN (SELECT NULL))
SETTINGS in_null_as_unknown = 1;

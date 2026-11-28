SET in_null_as_unknown = 1;

SELECT toDate('2000-01-01') IN (toDateTime('2000-01-01 12:00:00'), NULL),
       toDate('2000-01-01') IN (materialize(toDateTime('2000-01-01 12:00:00')), NULL);
SELECT toDate32('2000-01-01') IN (toDateTime('2000-01-01 12:00:00'), NULL),
       toDate32('2000-01-01') IN (materialize(toDateTime('2000-01-01 12:00:00')), NULL);
SELECT toDate('2000-01-01') NOT IN (toDateTime('2000-01-01 12:00:00'), NULL),
       toDate('2000-01-01') NOT IN (materialize(toDateTime('2000-01-01 12:00:00')), NULL);
SELECT count() FROM VALUES('x Date', ('2000-01-01'))
WHERE x IN (materialize(toDateTime('2000-01-01 12:00:00')), NULL);

SELECT toDate('2000-01-01') IN materialize([toDateTime('2000-01-01 12:00:00'), NULL]);
SELECT toDate('2000-01-01') IN
       plus(materialize(toDateTime('2000-01-01 12:00:00')), toIntervalHour(1));

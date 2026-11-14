SET in_null_as_unknown = 1;

SELECT toDate('1970-01-01') IN (65536, NULL),
       toDate('1970-01-01') IN (materialize(65536), NULL);
SELECT toDate('1970-01-01') NOT IN (65536, NULL),
       toDate('1970-01-01') NOT IN (materialize(65536), NULL);
SELECT count() FROM VALUES('x Date', ('1970-01-01'))
WHERE x IN (materialize(65536), NULL);

SELECT toDate('1970-01-01') IN materialize([toUInt32(65536), NULL]);
SELECT toDate('1970-01-01') IN plus(materialize(65536), 0);
SELECT toDate('1970-01-01') IN (materialize(0), NULL),
       toDate('1970-01-01') IN materialize([toUInt32(0), NULL]);

SELECT true IN materialize([toUInt8(2), NULL]);
SELECT true IN plus(materialize(2), 0);

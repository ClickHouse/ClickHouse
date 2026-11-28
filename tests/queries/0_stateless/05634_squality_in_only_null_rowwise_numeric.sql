SET in_null_as_unknown = 1;

SELECT NULL IN (number, 0.5), NULL NOT IN (number, 0.5),
       materialize(NULL) IN (number, 0.5), materialize(NULL) NOT IN (number, 0.5)
FROM numbers(1);

SELECT materialize(NULL) IN (), materialize(NULL) NOT IN ();

SET in_null_as_unknown = 0;

SELECT materialize(NULL) IN (number, 0.5), materialize(NULL) NOT IN (number, 0.5)
FROM numbers(1);

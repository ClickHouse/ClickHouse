-- The TO of a fill key limits only the values generated for that key: in the groups of the data at or past it,
-- the later keys are filled as without TO.

SELECT '--- later key with FROM';
SELECT * FROM values('b Int64, c Int64', (2, 2), (3, 4))
ORDER BY b WITH FILL TO 1, c WITH FILL FROM 0 TO 5;

SELECT '--- three keys, sorted and without duplicates';
SELECT * FROM values('a Int64, b Int64, c Int64', (1, 2, 2), (2, 3, 4))
ORDER BY a WITH FILL, b WITH FILL TO 1, c WITH FILL FROM 0 TO 5;
SELECT DISTINCT a, b, c FROM
(
    SELECT * FROM values('a Int64, b Int64, c Int64', (1, 2, 2), (2, 3, 4))
    ORDER BY a WITH FILL, b WITH FILL TO 1, c WITH FILL FROM 0 TO 5
);
SELECT * FROM
(
    SELECT * FROM values('a Int64, b Int64, c Int64', (1, 2, 2), (2, 3, 4))
    ORDER BY a WITH FILL, b WITH FILL TO 1, c WITH FILL FROM 0 TO 5
)
ORDER BY a, b, c;

SELECT '--- later key with FROM and without TO';
SELECT * FROM values('a Int64, b Int64, c Int64', (1, 2, 2), (2, 3, 4))
ORDER BY a WITH FILL, b WITH FILL TO 1, c WITH FILL FROM 0;

SELECT '--- data at TO';
SELECT * FROM values('b Int64, c Int64', (1, 2), (3, 4), (3, 7))
ORDER BY b WITH FILL TO 1, c WITH FILL FROM 0 TO 9;

SELECT '--- next group at TO';
SELECT * FROM values('b Int64, c Int64', (2, 2), (3, 4))
ORDER BY b WITH FILL TO 3, c WITH FILL FROM 0 TO 5;

SELECT '--- gap past TO';
SELECT * FROM values('b Int64, c Int64', (2, 2), (5, 1))
ORDER BY b WITH FILL TO 1, c WITH FILL FROM 0 TO 3;

SELECT '--- STEP 2, a group off the steps is not entered';
SELECT * FROM values('b Int64, c Int64', (2, 2), (3, 1))
ORDER BY b WITH FILL TO 1 STEP 2, c WITH FILL FROM 0 TO 3;

SELECT '--- DESC';
SELECT * FROM values('b Int64, c Int64', (3, 2), (2, 4))
ORDER BY b DESC WITH FILL TO 4, c WITH FILL FROM 0 TO 5;

SELECT '--- later key without FROM';
SELECT * FROM values('i Int64, k Int64', (1, 0), (3, 5), (3, 8))
ORDER BY i WITH FILL TO 1, k WITH FILL;
SELECT * FROM values('i Int64, j Int64, k Int64', (0, 2, 0), (0, 3, 5), (0, 3, 8))
ORDER BY i WITH FILL, j WITH FILL TO 2, k WITH FILL;

SELECT '--- Date';
SELECT * FROM values('d Date, k Int64', ('2020-01-31', 1), ('2020-03-31', 1))
ORDER BY d WITH FILL TO toDate('2020-01-01'), k WITH FILL FROM 0 TO 2;
SELECT * FROM values('d Date, k Int64', ('2020-01-03', 0), ('2020-01-05', 3), ('2020-01-05', 6))
ORDER BY d WITH FILL TO toDate('2020-01-03'), k WITH FILL;

SELECT '--- sorting prefix';
SELECT * FROM values('p Int64, b Int64, c Int64', (0, 2, 2), (1, 2, 4), (1, 3, 1))
ORDER BY p, b WITH FILL TO 1, c WITH FILL FROM 0 TO 5;
SELECT * FROM values('p Int64, b Int64, c Int64', (0, 2, 2), (1, 2, 4), (1, 3, 1))
ORDER BY p, b WITH FILL TO 1, c WITH FILL FROM 0 TO 5
SETTINGS max_block_size = 1;

SELECT '--- last group';
SELECT * FROM values('b Int64, c Int64', (2, 2), (3, 1))
ORDER BY b WITH FILL TO 1, c WITH FILL FROM 0 TO 5;

SELECT '--- FROM row after the first row of its group';
SELECT * FROM values('b Int64, c Int64', (2, 2), (3, -5), (3, 4))
ORDER BY b WITH FILL TO 1, c WITH FILL FROM 0 TO 5;

SELECT '--- STEP 2, a group on the steps is entered';
SELECT * FROM values('b Int64, c Int64', (2, 2), (4, 4))
ORDER BY b WITH FILL TO 1 STEP 2, c WITH FILL FROM 0 TO 5;
SELECT * FROM values('b Int64, c Int64', (4, 2), (2, 4))
ORDER BY b DESC WITH FILL TO 5 STEP -2, c WITH FILL FROM 0 TO 5;

SELECT '--- DateTime64';
SELECT * FROM values('d DateTime64(3, ''UTC''), c Int64', ('2020-01-01 00:00:05', 1), ('2020-01-01 00:00:06', 1))
ORDER BY d WITH FILL TO toDateTime64('2020-01-01 00:00:00', 3, 'UTC') STEP INTERVAL 1 SECOND, c WITH FILL FROM 0 TO 3;

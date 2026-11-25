-- The columns of the subquery of `UNIQUE` are renamed by their positions. Its `INTERPOLATE` items
-- still refer to the columns by their own names, so they must be kept, and the interpolated rows tested.
-- The same holds for the column aliases of a CTE or of a subquery in `FROM`.
-- See https://github.com/ClickHouse/ClickHouse/pull/99877

SET enable_analyzer = 1;

SELECT UNIQUE(
    SELECT number + 10 AS x
    FROM numbers(2)
    ORDER BY number WITH FILL TO 4
    INTERPOLATE (x AS x + 1)
);

SELECT UNIQUE(
    SELECT number + 10 AS x
    FROM numbers(2)
    ORDER BY number WITH FILL TO 4
    INTERPOLATE (x AS x)
);

SELECT '--';
WITH t(y) AS (SELECT number + 10 AS x FROM numbers(2) ORDER BY number WITH FILL TO 4 INTERPOLATE (x AS x + 1))
SELECT * FROM t;

SELECT '--';
SELECT y FROM (SELECT number + 10 AS x FROM numbers(2) ORDER BY number WITH FILL TO 4 INTERPOLATE (x AS x + 1)) AS t(y);

SELECT '--';
SELECT z FROM (SELECT number + 10 AS x, number AS n FROM numbers(2) ORDER BY n WITH FILL TO 4 INTERPOLATE (x AS x + 1)) AS t(z, m);

-- The names are renamed once more when `IN` is rewritten to a join: the `INTERPOLATE` item must survive it.
SELECT '--';
WITH t(y) AS (SELECT number + 10 AS x FROM numbers(2) ORDER BY number WITH FILL TO 4 INTERPOLATE (x AS x + 1))
SELECT number, number IN t FROM numbers(12, 2) ORDER BY number SETTINGS rewrite_in_to_join = 1;

-- The query sent to a shard is written with the new names, so its `INTERPOLATE` items are renamed as well.
SELECT '--';
SELECT UNIQUE(
    SELECT number + 10 AS x
    FROM remote('127.0.0.2', numbers(2))
    ORDER BY number WITH FILL TO 4
    INTERPOLATE (x AS x + 1)
);

WITH t(y) AS (SELECT number + 10 AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY number WITH FILL TO 4 INTERPOLATE (x AS x + 1))
SELECT * FROM t;

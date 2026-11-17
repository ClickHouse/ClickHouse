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

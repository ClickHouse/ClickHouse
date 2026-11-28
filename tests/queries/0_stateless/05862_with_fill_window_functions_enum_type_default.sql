-- Rows that WITH FILL, INTERPOLATE, lag/lead, nth_value and neighbor pad get the type default, which for an `Enum` is
-- its smallest value and not the column's zero (issue #132404).

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (n UInt8, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum16('x' = 5, 'y' = 7)), ne Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = Memory;
INSERT INTO tab VALUES (1, 'b', ('p', 'y'), 'b'), (3, 'b', ('q', 'y'), 'b');

SELECT 'with fill';
SELECT n, e, CAST(e, 'Int8'), t, ne FROM (SELECT n, e, t, ne FROM tab ORDER BY n WITH FILL) ORDER BY n;
SELECT count() FROM (SELECT n, e FROM tab ORDER BY n WITH FILL) WHERE e = 'a';
SELECT 'interpolate before the first row';
SELECT n, e, t FROM tab ORDER BY n WITH FILL FROM 0 INTERPOLATE (e, t);
SELECT 'sorting prefix of an empty input';
SELECT e, t, n FROM tab WHERE n > 10 ORDER BY e, t, n WITH FILL FROM 0 TO 2 SETTINGS use_with_fill_by_sorting_prefix = 1;
SELECT 'lag and lead';
SELECT n, lag(e) OVER w, lead(e) OVER w, lagInFrame(e) OVER w, leadInFrame(e) OVER w, lagInFrame(t) OVER w, lagInFrame(ne) OVER w
FROM tab WINDOW w AS (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY n;
SELECT n, lagInFrame(e) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM tab ORDER BY n;
SELECT count() FROM (SELECT lag(e) OVER (ORDER BY n) AS l FROM tab) WHERE l = 'a';
SELECT 'nth_value';
SELECT n, nth_value(e, 2) OVER w, nth_value(t, 2) OVER w FROM tab WINDOW w AS (ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW) ORDER BY n;
SELECT 'neighbor';
SET allow_deprecated_error_prone_window_functions = 1;
-- One source row, so the offset is outside the block whatever max_block_size is.
SELECT neighbor(e, 1), neighbor(t, -1) FROM tab WHERE n = 1;
SELECT neighbor(e, toInt64(n)), neighbor(t, -toInt64(n)) FROM tab WHERE n = 1;
SELECT 'Nothing';
-- A `Nothing` value cannot be printed, so this only has to run without an exception.
SELECT ignore(lag([][1]) OVER (ORDER BY n)), ignore(nth_value([][1], 2) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW)),
    lag(tuple([][1])) OVER (ORDER BY n), lagInFrame(tuple(e, [][1])) OVER (ORDER BY n)
FROM tab FORMAT Null;
SELECT n, x.1 FROM (SELECT n, tuple(e, [][1]) AS x FROM tab ORDER BY n WITH FILL);

DROP TABLE tab;

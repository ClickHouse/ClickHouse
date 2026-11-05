-- The set built by query_plan_convert_join_to_in must not be cached by the query condition cache:
-- its contents differ between queries and change with the right table.

SET use_query_condition_cache = 1;
SET query_plan_convert_join_to_in = 1;

CREATE TABLE l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO l SELECT number, number % 7 FROM numbers(1000000);

CREATE TABLE r (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r VALUES (5, 1), (900003, 2);

-- Same filter on `l`, different sets.
SELECT count() FROM l ANY INNER JOIN r ON l.k = r.k WHERE r.c = 1 AND l.v >= 5;
SELECT count() FROM l ANY INNER JOIN r ON l.k = r.k WHERE r.c = 2 AND l.v >= 5;
SELECT count() FROM l ANY INNER JOIN r ON l.k = r.k WHERE r.c = 1 AND l.v >= 5;

-- Same query after the right table changes.
INSERT INTO r VALUES (900002, 1);
SELECT count() FROM l ANY INNER JOIN r ON l.k = r.k WHERE r.c = 1 AND l.v >= 5;

DROP TABLE l;
DROP TABLE r;

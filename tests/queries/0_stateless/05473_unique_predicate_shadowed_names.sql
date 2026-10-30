-- A lambda named like one of the functions that implement the `UNIQUE` predicate must not change its result.
-- The predicate is rewritten into a scalar subquery with `count`, `uniqExact`, `equals`, `and`, `isNotNull`
-- and `ifNull`, which are resolved in the scope of the query, where such a lambda is visible.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;

WITH ((x, y) -> 42) AS ifNull SELECT UNIQUE(SELECT number FROM numbers(2));
WITH (x -> 0) AS isNotNull SELECT UNIQUE(SELECT 1 FROM numbers(2));
WITH (x -> 0) AS isNotNull SELECT UNIQUE(SELECT NULL FROM numbers(2));
WITH ((x, y) -> 7) AS equals, (x -> 5) AS uniqExact, (() -> 9) AS count
SELECT UNIQUE(SELECT 1 FROM numbers(2)), UNIQUE(SELECT number FROM numbers(2));
WITH ((x, y) -> 0) AS and SELECT UNIQUE(SELECT number FROM numbers(2));

-- The predicate itself cannot be replaced by a lambda named after its internal function.
WITH (x -> 42) AS __unique SELECT UNIQUE(SELECT 1);

-- A correlated subquery.
WITH (x -> 0) AS isNotNull
SELECT n, UNIQUE(SELECT 1 FROM numbers(5) WHERE number < n) FROM (SELECT arrayJoin([0, 1, 2]) AS n) ORDER BY n;

-- Only a single subquery as the whole argument is the predicate. Other shapes are ordinary function calls,
-- also when the argument starts with a keyword that can start a subquery.
WITH (x -> x) AS unique SELECT unique(from + 1) FROM (SELECT 1 AS from);
WITH (x, y) -> x + y AS unique SELECT unique((SELECT 1), 2), unique((SELECT 1) + 1, 3);

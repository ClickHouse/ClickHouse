SET explain_query_plan_default = 'legacy';

-- A window function referenced many times through a chain of aliases must be computed only once.
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT ntile(2) OVER (ORDER BY number) AS w, w + 1 AS a, a * a AS b, b + b + b AS c FROM numbers(10)
)
WHERE explain LIKE '%ntile(%' AND explain NOT LIKE '%->%';

SELECT number, w, c FROM
(
    SELECT number, ntile(2) OVER (ORDER BY number) AS w, w + 1 AS a, a * a AS b, b + b + b AS c FROM numbers(10)
)
ORDER BY number;

SELECT '--';

-- The same function over different windows and different functions over the same window are kept apart.
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT
        ntile(2) OVER (ORDER BY number) AS w1,
        ntile(2) OVER (ORDER BY number DESC) AS w2,
        ntile(3) OVER (ORDER BY number) AS w3,
        w1 + w1 + w2 + w2 + w3 + w3 AS s
    FROM numbers(10)
)
WHERE explain LIKE '%ntile(%' AND explain NOT LIKE '%->%';

SELECT number, w1, w2, w3, s FROM
(
    SELECT
        number,
        ntile(2) OVER (ORDER BY number) AS w1,
        ntile(2) OVER (ORDER BY number DESC) AS w2,
        ntile(3) OVER (ORDER BY number) AS w3,
        w1 + w1 + w2 + w2 + w3 + w3 AS s
    FROM numbers(10)
)
ORDER BY number;

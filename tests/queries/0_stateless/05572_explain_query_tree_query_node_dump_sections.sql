-- EXPLAIN QUERY TREE prints one line per query tree node. For a QUERY node that line
-- carries the clause flags of the SELECT, and it is followed by one section header per
-- non-empty clause. The format is documented in docs/reference/statements/explain.mdx,
-- section "EXPLAIN QUERY TREE"; the LIMIT AFTER / UNTIL / ALL syntax used below is
-- documented in the FunctionDocumentation of src/Parsers/ParserSelectQuery.cpp.
--
-- run_passes = 0 is what makes the flags observable: the resolution passes clear
-- is_group_by_all, is_order_by_all and is_limit_by_all and replace the clauses they
-- stand for by the expanded ones, so the query as written only survives unresolved.
--
-- Every query asserts two decoded values of the dump: the root QUERY line with its node
-- id replaced by a constant (that is, the set of printed flags), and the sorted set of
-- top-level section headers, i.e. the lines indented by exactly two spaces. Sorting the
-- headers keeps both values independent of the row order of the EXPLAIN result.

-- A recursive CTE sets is_recursive_with and fills the WITH section.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t);

-- LIMIT WITH TIES sets is_limit_with_ties, ORDER BY ALL sets is_order_by_all.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number FROM numbers(10) ORDER BY ALL LIMIT 2 WITH TIES);

-- GROUP BY ALL sets is_group_by_all; the unresolved tree has no GROUP BY section yet.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number, count() FROM numbers(3) GROUP BY ALL);

-- LIMIT n BY ALL sets is_limit_by_all and keeps only the LIMIT BY LIMIT section.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number FROM numbers(10) LIMIT 1 BY ALL);

-- A named window adds the WINDOW section.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number, sum(number) OVER w FROM numbers(3) WINDOW w AS (ORDER BY number));

-- LIMIT n, m BY expr plus LIMIT n OFFSET m adds LIMIT BY OFFSET and OFFSET.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number FROM numbers(10) ORDER BY number LIMIT 1, 1 BY number % 2 LIMIT 2 OFFSET 1);

-- A LIMIT range with ALL sets is_limit_after_all and adds LIMIT AFTER and LIMIT UNTIL.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number IN (2, 6) ALL UNTIL number = 4);

-- A reset and a query parameter are not settings changes: they are printed as
-- name=DEFAULT and param_name='value' at the end of the SETTINGS section.
SELECT replaceRegexpOne(maxIf(explain, explain NOT LIKE ' %'), '^QUERY id: [0-9]+', 'QUERY') AS root,
       arraySort(groupArrayIf(trimBoth(explain), explain LIKE '  %' AND explain NOT LIKE '   %')) AS sections
FROM (EXPLAIN QUERY TREE run_passes = 0
      SELECT 1 SETTINGS max_threads = DEFAULT, param_p = 'v');

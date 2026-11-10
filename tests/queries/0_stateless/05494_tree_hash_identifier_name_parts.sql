-- Identifiers that differ only in where the dot falls must not share an AST tree hash.
-- `t.x` is column `x` of `t`, while `` `t.x` `` is column `t.x` of `u`.

SELECT * FROM
(
    SELECT * FROM view(SELECT t.x FROM (SELECT 1 AS x) AS t, (SELECT 2 AS `t.x`) AS u)
    UNION ALL
    SELECT * FROM view(SELECT `t.x` FROM (SELECT 1 AS x) AS t, (SELECT 2 AS `t.x`) AS u)
)
ORDER BY ALL;

SELECT t.x FROM (SELECT 1 AS x) AS t, (SELECT 2 AS `t.x`) AS u SETTINGS use_query_cache = 1;
SELECT `t.x` FROM (SELECT 1 AS x) AS t, (SELECT 2 AS `t.x`) AS u SETTINGS use_query_cache = 1;

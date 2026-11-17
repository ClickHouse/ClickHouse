-- The `UNIQUE` predicate (the operator `__unique`) and an ordinary call `__unique(...)` are different expressions,
-- so they must have different AST hashes and must not share an entry of the query cache.
-- See https://github.com/ClickHouse/ClickHouse/pull/99877

SET enable_analyzer = 1;

WITH x -> 42 AS __unique SELECT UNIQUE(SELECT 1) AS u_05817 SETTINGS use_query_cache = 1;
WITH x -> 42 AS __unique SELECT __unique((SELECT 1)) AS u_05817 SETTINGS use_query_cache = 1;
WITH x -> 42 AS __unique SELECT UNIQUE(SELECT 1) AS u_05817 SETTINGS use_query_cache = 1;

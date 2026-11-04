-- The identifier resolve cache must not reuse a result across the window in which a lambda argument is hidden
-- while an outer alias is resolved inside the lambda: https://github.com/ClickHouse/ClickHouse/issues/121046

-- `x` in `n` is resolved inside the window (the column), the direct `x` outside of it (the argument).
SELECT x + 1 AS n, arrayMap(x -> arrayMap(y -> n + x, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 0;
SELECT x + 1 AS n, arrayMap(x -> arrayMap(y -> n + x, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 1;

-- The same with the direct `x` resolved first.
SELECT x + 1 AS n, arrayMap(x -> arrayMap(y -> x + n, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 0;
SELECT x + 1 AS n, arrayMap(x -> arrayMap(y -> x + n, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 1;

-- Both `x` in `n` are resolved inside the same window.
SELECT x + x AS n, arrayMap(x -> arrayMap(y -> n + x, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 0;
SELECT x + x AS n, arrayMap(x -> arrayMap(y -> n + x, [1])[1], [10]) FROM (SELECT 5 AS x) SETTINGS enable_identifier_resolve_cache = 1;

-- The lambda and the parameters of an `APPLY` column transformer must be seen by every pass over the query AST.

-- Set operations are normalized.
SELECT * APPLY (x -> x + (SELECT count() FROM (SELECT 1 UNION ALL SELECT 2))) FROM (SELECT 1 AS i);
SELECT * APPLY (x -> x + (SELECT count() FROM (SELECT 1 UNION DISTINCT SELECT 1))) FROM (SELECT 1 AS i);
SELECT * APPLY (x -> x + (SELECT count() FROM (SELECT 1 UNION SELECT 1))) FROM (SELECT 1 AS i) SETTINGS union_default_mode = 'DISTINCT';
SELECT * APPLY (x -> x + (SELECT count() FROM (SELECT 1 INTERSECT SELECT 1))) FROM (SELECT 1 AS i);
SELECT * APPLY (x -> x + (SELECT count() FROM (SELECT 1 EXCEPT SELECT 2))) FROM (SELECT 1 AS i);
SELECT * APPLY (x -> x IN (SELECT 1 UNION ALL SELECT 2)) FROM (SELECT 1 AS i);
SELECT COLUMNS('i') APPLY (x -> x + (SELECT count() FROM (SELECT 1 UNION ALL SELECT 2))) FROM (SELECT 1 AS i);

-- Query parameters are substituted, in the lambda and in the parameters of the function.
SET param_p = 2;
SELECT * APPLY (x -> x + {p:UInt8}) FROM (SELECT 1 AS i);
SET param_q = 0.5;
SELECT * APPLY (quantile({q:Float64})) FROM (SELECT number AS i FROM numbers(11));

-- Integer division follows `exact_division_for_integers`.
SELECT * APPLY (x -> x / 2) FROM (SELECT 3 AS i) SETTINGS exact_division_for_integers = 1;

-- The query cache refuses a non-deterministic function and a system table.
SELECT * APPLY (x -> x + rand()) FROM (SELECT 1 AS i) SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * APPLY (x -> x + (SELECT count() FROM system.one)) FROM (SELECT 1 AS i) SETTINGS use_query_cache = 1, query_cache_system_table_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }

-- A table function's secret arguments are masked in the query log. The empty address is rejected after the query is logged.
SELECT * APPLY (x -> x + (SELECT count() FROM remote('', system.one, 'usr', 'pass_05405_secret'))) FROM (SELECT 1 AS i) SETTINGS log_comment = '05405_apply_masking'; -- { serverError BAD_ARGUMENTS }
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0, countIf(position(query, 'pass_05405_secret') > 0), countIf(query LIKE '%[HIDDEN]%') = count()
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05405_apply_masking' AND event_date >= yesterday();

-- A view stores the tables of the lambda qualified with the database of its definition.
CREATE TABLE t (a UInt8) ENGINE = Memory;
INSERT INTO t VALUES (1), (2), (3), (4), (5);
CREATE VIEW v AS SELECT tupleElement(tuple(*), 1) AS r FROM (SELECT * APPLY (x -> x + (SELECT count() FROM t)) FROM (SELECT 1 AS i));
USE system;
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.v;

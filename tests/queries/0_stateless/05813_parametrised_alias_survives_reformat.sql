-- An alias that is a query parameter, `AS {name:Identifier}`, is kept when a definition is formatted and parsed back.

SELECT formatQuerySingleLine('SELECT number AS {a:Identifier}, (number + 1) AS {b:Identifier} FROM numbers(2) AS {t:Identifier}') AS f, formatQuerySingleLine(f) = f;
SELECT formatQuerySingleLine('SELECT 1 LIMIT AFTER `settings` = 1 AS {p:Identifier}') AS f, formatQuerySingleLine(f) = f;

DROP VIEW IF EXISTS v_alias_param;
DROP VIEW IF EXISTS v_alias_only;
CREATE VIEW v_alias_param AS SELECT number AS {a:Identifier} FROM numbers(2) WHERE number < {n:UInt64};
CREATE VIEW v_alias_only AS SELECT (number + 1) AS {a:Identifier} FROM numbers(2);

SELECT name, arraySort(parameterized_view_parameters), position(create_table_query, '{a:Identifier}') > 0
FROM system.tables WHERE database = currentDatabase() AND name IN ('v_alias_only', 'v_alias_param') ORDER BY name;

SELECT * FROM v_alias_param(a = 'x', n = 5) ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM v_alias_only(a = 'y') ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM v_alias_param(n = 5); -- { serverError UNKNOWN_QUERY_PARAMETER }

DETACH TABLE v_alias_param;
ATTACH TABLE v_alias_param;
DETACH TABLE v_alias_only;
ATTACH TABLE v_alias_only;

SELECT * FROM v_alias_param(a = 'x', n = 5) ORDER BY ALL FORMAT TSVWithNames;
SELECT x FROM v_alias_param(a = 'x', n = 5) ORDER BY ALL;
SELECT * FROM v_alias_only(a = 'y') ORDER BY ALL FORMAT TSVWithNames;

-- A materialized view substitutes the parameter when it is created and stores the resolved alias.
SET param_m = 'z';
DROP TABLE IF EXISTS src_alias;
DROP VIEW IF EXISTS mv_alias;
CREATE TABLE src_alias (number UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_alias ENGINE = Memory AS SELECT number AS {m:Identifier} FROM src_alias;
SELECT position(create_table_query, '{m:Identifier}') = 0, position(create_table_query, 'AS z') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'mv_alias';
DETACH TABLE mv_alias;
ATTACH TABLE mv_alias;
INSERT INTO src_alias VALUES (7);
SELECT z FROM mv_alias;

DROP VIEW mv_alias;
DROP TABLE src_alias;
DROP VIEW v_alias_only;
DROP VIEW v_alias_param;

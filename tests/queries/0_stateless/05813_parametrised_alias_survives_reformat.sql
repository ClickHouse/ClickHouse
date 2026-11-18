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
SELECT * FROM v_alias_param(a = 'y', n = 5) ORDER BY ALL FORMAT TSVWithNames;
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

-- A FILTER condition becomes an argument of the aggregate, where a parametrised alias is not kept.
SELECT formatQuerySingleLine('SELECT count() FILTER (WHERE number > 0 AS {p:Identifier}) FROM numbers(3)') AS f, formatQuerySingleLine(f) = f;
DROP VIEW IF EXISTS v_alias_filter;
CREATE VIEW v_alias_filter AS SELECT count() FILTER (WHERE number > 0 AS {p:Identifier}) AS c FROM numbers(3);
SELECT parameterized_view_parameters FROM system.tables WHERE database = currentDatabase() AND name = 'v_alias_filter';
DETACH TABLE v_alias_filter;
ATTACH TABLE v_alias_filter;
SELECT c FROM v_alias_filter;
DROP VIEW v_alias_filter;

-- CREATE qualifies the table name and expands a common table expression, keeping the alias of the table.
DROP VIEW IF EXISTS v_alias_table;
DROP VIEW IF EXISTS v_alias_cte;
CREATE VIEW v_alias_table AS SELECT t.number FROM src_alias AS {a:Identifier};
CREATE VIEW v_alias_cte AS WITH c AS (SELECT number FROM src_alias) SELECT t.number FROM c AS {a:Identifier};
SELECT name, parameterized_view_parameters, position(create_table_query, '{a:Identifier}') > 0
FROM system.tables WHERE database = currentDatabase() AND name IN ('v_alias_cte', 'v_alias_table') ORDER BY name;
DETACH TABLE v_alias_table;
ATTACH TABLE v_alias_table;
DETACH TABLE v_alias_cte;
ATTACH TABLE v_alias_cte;
SELECT * FROM v_alias_table(a = 't');
SELECT * FROM v_alias_cte(a = 't');
DROP VIEW v_alias_cte;
DROP VIEW v_alias_table;

-- CREATE qualifies an operand of IN with the database, as in any parameterized view, so it names the table even if the call names the alias after it.
DROP TABLE IF EXISTS vals;
DROP VIEW IF EXISTS v_alias_in;
CREATE TABLE vals (x UInt8) ENGINE = Memory;
INSERT INTO vals VALUES (3);
CREATE VIEW v_alias_in AS WITH [1, 2] AS {a:Identifier} SELECT 1 IN vals AS hit, 3 GLOBAL IN vals AS global_hit;
SELECT hit, global_hit FROM v_alias_in(a = 'vals');
DROP VIEW v_alias_in;
DROP TABLE vals;

DROP VIEW mv_alias;
DROP TABLE src_alias;
DROP VIEW v_alias_only;
DROP VIEW v_alias_param;

-- With an older `compatibility`, a view whose only parameters are aliases is not parameterized: CREATE substitutes them.
SET param_a = 'from_session';
SET compatibility = '26.10';
DROP VIEW IF EXISTS v_alias_compatibility;
DROP VIEW IF EXISTS v_alias_compatibility_in;
CREATE VIEW v_alias_compatibility AS SELECT 7 AS {a:Identifier};
SELECT parameterized_view_parameters, position(create_table_query, '{a:Identifier}') FROM system.tables WHERE database = currentDatabase() AND name = 'v_alias_compatibility';
SELECT from_session FROM v_alias_compatibility;
CREATE VIEW v_alias_compatibility_in AS WITH [1, 2] AS {a:Identifier} SELECT 1 IN from_session AS hit;
SELECT hit FROM v_alias_compatibility_in;
DROP VIEW v_alias_compatibility_in;
DROP VIEW v_alias_compatibility;

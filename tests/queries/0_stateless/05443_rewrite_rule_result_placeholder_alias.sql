-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The alias written on a placeholder in the result template names the substituted expression,
-- so it can be referenced elsewhere in the rewritten query.

DROP RULE IF EXISTS rule_05443;
CREATE RULE rule_05443 AS (SELECT {x:Int}) REWRITE TO (SELECT {x:Int} AS y, y + 1);

SET query_rules = 'rule_05443';
SELECT 42;

SET query_rules = '';
DROP RULE rule_05443;

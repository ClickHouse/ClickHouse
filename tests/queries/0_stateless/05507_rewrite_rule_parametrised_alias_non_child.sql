-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A query parameter used as an alias (`expr AS {name:Type}`) is rejected in rewrite rule templates
-- also when the alias is reachable only through an AST member kept outside `IAST::children`, such
-- as the `WHERE` expression of `SHOW TABLES`.

CREATE RULE rule_alias_non_child AS (SELECT 12345) REWRITE TO (SHOW TABLES WHERE (SELECT 1 AS {a:Identifier} WHERE a = 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
CREATE RULE rule_alias_non_child AS (SHOW TABLES WHERE (SELECT 1 AS {a:Identifier} WHERE a = 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

CREATE RULE rule_alias_non_child AS (SELECT 12345) REWRITE TO (SELECT 1);
ALTER RULE rule_alias_non_child AS (SELECT 12345) REWRITE TO (SHOW TABLES WHERE (SELECT 1 AS {a:Identifier} WHERE a = 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- A literal alias in the same place is fine.
ALTER RULE rule_alias_non_child AS (SELECT 12345) REWRITE TO (SHOW TABLES WHERE (SELECT 1 AS a WHERE a = 1));
SELECT count() FROM system.query_rules WHERE name = 'rule_alias_non_child';
DROP RULE rule_alias_non_child;

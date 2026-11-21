-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A placeholder inside a nested rule template is unreachable by the matcher, so it is rejected at
-- DDL time (see `05053_rewrite_rule_nested_template_non_child_placeholder`). An AST-valued value of
-- `CREATE` / `ALTER NAMED COLLECTION` (e.g. `disk(...)`) is kept inside a `Field`, outside
-- `children`; the screening of nested templates must reach it as well, otherwise the outer rule is
-- persisted and only fails once it produces the nested DDL.

-- Nested rule DDL on the result side of the outer rule.
CREATE RULE rule_05453_outer AS (SELECT 1) REWRITE TO (CREATE RULE rule_05453_inner AS (CREATE NAMED COLLECTION c AS storage = disk(path = {p:String})) REWRITE TO (SELECT 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- Nested rule DDL on the source side of the outer rule.
CREATE RULE rule_05453_outer AS (CREATE RULE rule_05453_inner AS (ALTER NAMED COLLECTION c SET storage = disk(path = {p:String})) REWRITE TO (SELECT 1)) REWRITE TO (SELECT 1); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }

-- `ALTER RULE` performs the same screening.
CREATE RULE rule_05453_outer AS (SELECT 1) REWRITE TO (SELECT 2);
ALTER RULE rule_05453_outer AS (SELECT 1) REWRITE TO (CREATE RULE rule_05453_inner AS (CREATE NAMED COLLECTION c AS storage = disk(path = {p:String})) REWRITE TO (SELECT 1)); -- { serverError REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE }
DROP RULE rule_05453_outer;

-- The counterfactual: a nested template without a placeholder in the named-collection value is accepted.
CREATE RULE rule_05453_outer AS (SELECT 1) REWRITE TO (CREATE RULE rule_05453_inner AS (CREATE NAMED COLLECTION c AS storage = disk(path = 'x')) REWRITE TO (SELECT 1));
SELECT count() FROM system.query_rules WHERE name = 'rule_05453_outer';
DROP RULE rule_05453_outer;

-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The replacement expression of `REPLACE (expr AS name)` is the only child of
-- `ASTColumnsReplaceTransformer::Replacement`, and must not be folded into the hash of that node
-- itself: the matcher compares the own hashes of the nodes before descending into children, so a
-- placeholder there (or a typed query parameter, substituted as `_CAST`) would never be reached.

DROP RULE IF EXISTS rule_05457_placeholder;
DROP RULE IF EXISTS rule_05457_literal;

CREATE RULE rule_05457_placeholder AS (SELECT * REPLACE ({p:Expression} AS dummy) FROM system.one) REJECT WITH 'blocked';
SET query_rules = 'rule_05457_placeholder';
SELECT * REPLACE (1 AS dummy) FROM system.one; -- { serverError REWRITE_RULE_REJECTION }
SELECT * REPLACE (dummy + 1 AS dummy) FROM system.one; -- { serverError REWRITE_RULE_REJECTION }
SELECT * REPLACE (2 AS other) FROM system.one;
SET query_rules = '';
DROP RULE rule_05457_placeholder;

-- A literal in the template matches the same value supplied through a typed query parameter.
CREATE RULE rule_05457_literal AS (SELECT * REPLACE (1 AS dummy) FROM system.one) REJECT WITH 'blocked';
SET query_rules = 'rule_05457_literal';
SET param_v = 1;
SELECT * REPLACE ({v:UInt8} AS dummy) FROM system.one; -- { serverError REWRITE_RULE_REJECTION }
SELECT * REPLACE (2 AS dummy) FROM system.one;
SET query_rules = '';
DROP RULE rule_05457_literal;

-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- `ASTKillQueryQuery::getID` used to include the id of the `WHERE` predicate, so the current-node
-- hash of the `KILL` node depended on its predicate child. The matcher then stopped at the `KILL`
-- node (`QueryParameter` vs `Literal`) instead of descending to bind a predicate placeholder, and a
-- predicate supplied through a typed query parameter (wrapped into `_CAST`) never matched a rule
-- written against the bare literal.

-- The `KILL ... WHERE 0` statements below match nothing; they must not throw `NOTHING_TO_KILL`.
SET kill_throw_if_noop = 0;

CREATE RULE rule_05454_placeholder AS (KILL QUERY WHERE {p:Expression} TEST) REJECT WITH 'blocked';
SET query_rules = 'rule_05454_placeholder';
KILL QUERY WHERE 0 TEST; -- { serverError REWRITE_RULE_REJECTION }
KILL QUERY WHERE query_id = 'nonexistent' TEST; -- { serverError REWRITE_RULE_REJECTION }
-- A different `KILL` variant is still not matched.
KILL MUTATION WHERE 0 TEST;
SELECT 'placeholder: a different KILL variant is not matched';
SET query_rules = '';
DROP RULE rule_05454_placeholder;

-- Typed-parameter transparency: a literal rule source matches the same predicate supplied as `{p:UInt64}`.
CREATE RULE rule_05454_literal AS (KILL QUERY WHERE 0 TEST) REJECT WITH 'blocked';
SET query_rules = 'rule_05454_literal';
SET param_p = 0;
KILL QUERY WHERE {p:UInt64} TEST; -- { serverError REWRITE_RULE_REJECTION }
SET param_p = 1;
KILL QUERY WHERE {p:UInt64} TEST;
SELECT 'literal: a different parameter value is not matched';
SET query_rules = '';
DROP RULE rule_05454_literal;

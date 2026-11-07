-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- Regression: a placeholder used directly as the `WHERE` predicate of a `KILL` result template is
-- replaced in `children`, and `ASTKillQueryQuery::where_expression` must follow it. Previously that
-- member kept pointing at the template's `{p:Expression}`, so the rewritten `KILL` formatted the
-- unresolved parameter into its internal `SELECT` instead of the captured predicate.

SET kill_throw_if_noop = 0;

DROP RULE IF EXISTS rule_05493_kill_predicate;
CREATE RULE rule_05493_kill_predicate AS (SELECT {p:Expression}) REWRITE TO (KILL QUERY WHERE {p:Expression} TEST);

SET query_rules = 'rule_05493_kill_predicate';
-- Rewritten to `KILL QUERY WHERE query_id = '...' TEST`, which matches no query and returns no rows.
SELECT query_id = 'nonexistent_query_id_05493';
SET query_rules = '';

SELECT 'ok';

DROP RULE rule_05493_kill_predicate;

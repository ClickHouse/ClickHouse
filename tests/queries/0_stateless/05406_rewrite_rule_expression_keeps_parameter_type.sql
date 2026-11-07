-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- An `Expression` placeholder must capture a typed query parameter together with its type:
-- an identity rewrite must not change the result.

CREATE RULE rule_keep_param_type AS (SELECT toTypeName({x:Expression})) REWRITE TO (SELECT toTypeName({x:Expression}));

SET param_p = '2026-01-01';
SELECT toTypeName({p:Date});

SET query_rules = 'rule_keep_param_type';
SELECT toTypeName({p:Date});

SET query_rules = '';
DROP RULE rule_keep_param_type;

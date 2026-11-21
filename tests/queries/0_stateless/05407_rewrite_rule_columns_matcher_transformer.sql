-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A rule source with a column matcher plus a transformer is stored as a clone of the parsed AST.
-- The clone must keep the children order of the parser, otherwise the rule does not match the
-- identical query.

CREATE RULE rule_columns_list AS (SELECT COLUMNS(number) APPLY sum FROM numbers(1)) REJECT WITH 'blocked list';
CREATE RULE rule_columns_regexp AS (SELECT t.COLUMNS('num') APPLY sum FROM numbers(1) AS t) REJECT WITH 'blocked regexp';
CREATE RULE rule_qualified_list AS (SELECT t.COLUMNS(number) APPLY sum FROM numbers(1) AS t) REJECT WITH 'blocked qualified list';
CREATE RULE rule_qualified_asterisk AS (SELECT t.* APPLY sum FROM numbers(1) AS t) REJECT WITH 'blocked qualified asterisk';

SET query_rules = 'rule_columns_list,rule_columns_regexp,rule_qualified_list,rule_qualified_asterisk';

SELECT COLUMNS(number) APPLY sum FROM numbers(1); -- { serverError REWRITE_RULE_REJECTION }
SELECT t.COLUMNS('num') APPLY sum FROM numbers(1) AS t; -- { serverError REWRITE_RULE_REJECTION }
SELECT t.COLUMNS(number) APPLY sum FROM numbers(1) AS t; -- { serverError REWRITE_RULE_REJECTION }
SELECT t.* APPLY sum FROM numbers(1) AS t; -- { serverError REWRITE_RULE_REJECTION }

-- Different queries are not rejected.
SELECT COLUMNS(number) APPLY max FROM numbers(1);
SELECT t.* APPLY max FROM numbers(1) AS t;

SET query_rules = '';
DROP RULE rule_columns_list;
DROP RULE rule_columns_regexp;
DROP RULE rule_qualified_list;
DROP RULE rule_qualified_asterisk;

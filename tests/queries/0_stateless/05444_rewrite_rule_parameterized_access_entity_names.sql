-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- A user or role name passed as a query parameter is matched by the rewrite rules as the name it
-- stands for: a `REJECT` rule for an access-entity statement must not be bypassable by writing the
-- name as `{u:Identifier}`.

DROP RULE IF EXISTS rule_05444_create_user;
DROP RULE IF EXISTS rule_05444_create_role;
DROP RULE IF EXISTS rule_05444_drop_user;
DROP RULE IF EXISTS rule_05444_grant;
DROP RULE IF EXISTS rule_05444_rename;

CREATE RULE rule_05444_create_user AS (CREATE USER u_05444_blocked) REJECT WITH 'blocked';
CREATE RULE rule_05444_create_role AS (CREATE ROLE r_05444_blocked) REJECT WITH 'blocked';
CREATE RULE rule_05444_drop_user AS (DROP USER u_05444_blocked) REJECT WITH 'blocked';
CREATE RULE rule_05444_grant AS (GRANT SELECT ON db_05444.t TO u_05444_blocked) REJECT WITH 'blocked';
CREATE RULE rule_05444_rename AS (ALTER USER u_05444_other RENAME TO u_05444_blocked) REJECT WITH 'blocked';

SET query_rules = 'rule_05444_create_user,rule_05444_create_role,rule_05444_drop_user,rule_05444_grant,rule_05444_rename';
SET param_u = 'u_05444_blocked';
SET param_r = 'r_05444_blocked';

CREATE USER u_05444_blocked; -- { serverError REWRITE_RULE_REJECTION }
CREATE USER {u:Identifier}; -- { serverError REWRITE_RULE_REJECTION }
CREATE ROLE r_05444_blocked; -- { serverError REWRITE_RULE_REJECTION }
CREATE ROLE {r:Identifier}; -- { serverError REWRITE_RULE_REJECTION }
DROP USER u_05444_blocked; -- { serverError REWRITE_RULE_REJECTION }
DROP USER {u:Identifier}; -- { serverError REWRITE_RULE_REJECTION }
GRANT SELECT ON db_05444.t TO u_05444_blocked; -- { serverError REWRITE_RULE_REJECTION }
GRANT SELECT ON db_05444.t TO {u:Identifier}; -- { serverError REWRITE_RULE_REJECTION }
ALTER USER u_05444_other RENAME TO u_05444_blocked; -- { serverError REWRITE_RULE_REJECTION }
ALTER USER u_05444_other RENAME TO {u:Identifier}; -- { serverError REWRITE_RULE_REJECTION }
SELECT 'ok';

SET query_rules = '';
DROP RULE rule_05444_create_user;
DROP RULE rule_05444_create_role;
DROP RULE rule_05444_drop_user;
DROP RULE rule_05444_grant;
DROP RULE rule_05444_rename;

-- Tags: no-fasttest
-- no-fasttest: the fast test does not build `HMAC`, so its key is not masked there

-- `system.row_policies` lists a policy only to a user who may SHOW it, and masks the secrets of its filter as `SHOW CREATE ROW POLICY` does.

DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE USER {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE decoy (x UInt8) ENGINE = Memory;
CREATE TABLE protected (x UInt8) ENGINE = Memory;
CREATE ROW POLICY table_policy_05875 ON protected FOR SELECT USING x = 1 OR queryID() = hex(HMAC('sha256', toString(x), 'secret_05875'));
CREATE ROW POLICY database_policy_05875 ON * FOR SELECT USING 1;

-- SELECT on the system table without SHOW shows none of them
GRANT SELECT ON system.row_policies TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SELECT count() FROM system.row_policies WHERE database = currentDatabase();
REVOKE SELECT ON system.row_policies FROM {CLICKHOUSE_DATABASE:Identifier};

-- SHOW on another table shows none of them, also to SHOW ROW POLICIES
GRANT SHOW ROW POLICIES ON decoy TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SELECT count() FROM system.row_policies WHERE database = currentDatabase();
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SHOW ROW POLICIES ON protected;

-- SHOW on a table shows its policy
GRANT SHOW ROW POLICIES ON protected TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SELECT short_name, select_filter FROM system.row_policies WHERE database = currentDatabase() ORDER BY short_name;

-- SHOW on the database shows the policies of the database
GRANT SHOW ROW POLICIES ON * TO {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SELECT short_name, select_filter FROM system.row_policies WHERE database = currentDatabase() ORDER BY short_name;

-- a revoke on a table hides the policies of that table and of the database
REVOKE SHOW ROW POLICIES ON protected FROM {CLICKHOUSE_DATABASE:Identifier};
EXECUTE AS {CLICKHOUSE_DATABASE:Identifier} SELECT count() FROM system.row_policies WHERE database = currentDatabase();

DROP ROW POLICY table_policy_05875 ON protected;
DROP ROW POLICY database_policy_05875 ON *;
DROP USER {CLICKHOUSE_DATABASE:Identifier};

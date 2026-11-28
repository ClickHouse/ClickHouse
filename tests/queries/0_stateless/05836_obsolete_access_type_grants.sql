-- Obsolete access types are marked in `system.privileges` and `system.grants`,
-- and they remain members of their parent groups, so revoking a group removes them.

SELECT privilege, is_obsolete FROM system.privileges WHERE is_obsolete ORDER BY privilege;

DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE USER {CLICKHOUSE_DATABASE:Identifier};

GRANT SYSTEM RELOAD MODEL ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = currentDatabase();
REVOKE SYSTEM ON *.* FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM system.grants WHERE user_name = currentDatabase();

GRANT SYSTEM RELOAD MODEL ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
REVOKE ALL ON *.* FROM {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM system.grants WHERE user_name = currentDatabase();

GRANT SELECT ON *.* TO {CLICKHOUSE_DATABASE:Identifier};
SELECT access_type, is_obsolete FROM system.grants WHERE user_name = currentDatabase();

DROP USER {CLICKHOUSE_DATABASE:Identifier};

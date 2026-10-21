-- The SQL cluster catalog is not configured in the test server, so the statements fail after
-- the access check; that is enough to check `readonly` / `allow_ddl` enforcement and masking of
-- secrets in `system.query_log`.

CREATE ENDPOINT e_05441 PROPERTIES (host = '127.0.0.1', port = 9000, user = 'default', password = 'secret_endpoint_05441'); -- { serverError INVALID_CONFIG_PARAMETER }
ALTER ENDPOINT e_05441 MODIFY PROPERTIES (password = 'secret_alter_endpoint_05441'); -- { serverError INVALID_CONFIG_PARAMETER }
CREATE CLUSTER c_05441 (s_05441) PROPERTIES (secret = 'secret_cluster_05441'); -- { serverError INVALID_CONFIG_PARAMETER }

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%secret\_%\_05441%';
SELECT count() >= 3 FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%_05441%' AND query LIKE '%[HIDDEN]%';

SET allow_ddl = 0;
CREATE ENDPOINT e_05441 PROPERTIES (host = '127.0.0.1', port = 9000); -- { serverError QUERY_IS_PROHIBITED }
ALTER ENDPOINT e_05441 MODIFY PROPERTIES (port = 9001); -- { serverError QUERY_IS_PROHIBITED }
DROP ENDPOINT e_05441; -- { serverError QUERY_IS_PROHIBITED }
CREATE SHARD s_05441 REPLICA (e_05441); -- { serverError QUERY_IS_PROHIBITED }
ALTER SHARD s_05441 MODIFY PROPERTIES (weight = 2); -- { serverError QUERY_IS_PROHIBITED }
DROP SHARD s_05441; -- { serverError QUERY_IS_PROHIBITED }
CREATE CLUSTER c_05441 (s_05441); -- { serverError QUERY_IS_PROHIBITED }
ALTER CLUSTER c_05441 ADD SHARD s_05441; -- { serverError QUERY_IS_PROHIBITED }
DROP CLUSTER c_05441; -- { serverError QUERY_IS_PROHIBITED }

-- `readonly` is checked before `allow_ddl`.
SET readonly = 1;
CREATE ENDPOINT e_05441 PROPERTIES (host = '127.0.0.1', port = 9000); -- { serverError READONLY }
ALTER ENDPOINT e_05441 MODIFY PROPERTIES (port = 9001); -- { serverError READONLY }
DROP ENDPOINT e_05441; -- { serverError READONLY }
CREATE SHARD s_05441 REPLICA (e_05441); -- { serverError READONLY }
ALTER SHARD s_05441 MODIFY PROPERTIES (weight = 2); -- { serverError READONLY }
DROP SHARD s_05441; -- { serverError READONLY }
CREATE CLUSTER c_05441 (s_05441); -- { serverError READONLY }
ALTER CLUSTER c_05441 ADD SHARD s_05441; -- { serverError READONLY }
DROP CLUSTER c_05441; -- { serverError READONLY }

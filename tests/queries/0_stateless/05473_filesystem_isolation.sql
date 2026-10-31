-- The configuration of the tests sets `filesystem_isolation` to `enforce`: see `tests/config/config.d/filesystem_isolation.xml`.
-- The server reports the mode in force, which cannot be changed without a restart.
SELECT value, changeable_without_restart FROM system.server_settings WHERE name = 'filesystem_isolation';

-- A program the server runs sees the isolated filesystem: its root is a read-only `tmpfs`, and the original root
-- is not reachable.
SELECT test_function_filesystem_isolation(1);

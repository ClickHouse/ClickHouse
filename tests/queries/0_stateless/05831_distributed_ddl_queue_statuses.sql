-- Tags: no-parallel, zookeeper
-- Tag no-parallel: the entries are system-global Keeper nodes under the distributed DDL queue path, so a concurrent reader of system.distributed_ddl_queue sees them too
-- Tag zookeeper: the entries are planted directly in Keeper

-- `system.distributed_ddl_queue` (documented in the DOCS_MD block of
-- src/Storages/System/attachSystemTables.cpp) reports one row per (entry, host). The per-host
-- status comes from the `active` and `finished` children of the entry node, and the status names
-- are the enum of `getStatusEnumsAndValues` in src/Storages/System/StorageSystemDDLWorkerQueue.cpp:
-- Inactive, Active, Finished, Removing, Unknown. A real ON CLUSTER query on a one-replica cluster
-- finishes immediately, so only a planted entry shows the other states.
--
-- `allow_zookeeper_write` is on for the stateless suite, and the DDL worker ignores every child of
-- the queue that does not start with `query-`, so these entries are never scheduled, executed or
-- cleaned up. The node names are fixed rather than database-qualified on purpose: the test is
-- sequential, so re-setting the same four entries on every run leaves no Keeper nodes behind and
-- also repairs a run that was interrupted while the last entry held an unparsable value.

INSERT INTO system.zookeeper (name, path, value) VALUES
    -- No status dir at all: both hosts of the entry are Inactive.
    ('covgap-ddl-queue-inactive', '/clickhouse/task_queue/ddl', 'version: 1\nquery: SELECT 1\nhosts: [\'h1:9001\',\'h2:9002\']\n'),
    -- h1 is in `finished` and still in `active` (it is reported once, as Finished), h4 is in
    -- `active` without being in `hosts`, h3 is in `hosts` without having started.
    ('covgap-ddl-queue-mixed', '/clickhouse/task_queue/ddl', 'version: 1\nquery: SELECT 1\nhosts: [\'h1:9001\',\'h3:9003\']\n'),
    ('h1:9001', '/clickhouse/task_queue/ddl/covgap-ddl-queue-mixed/finished', '0\n'),
    ('h1:9001', '/clickhouse/task_queue/ddl/covgap-ddl-queue-mixed/active', ''),
    ('h4:9004', '/clickhouse/task_queue/ddl/covgap-ddl-queue-mixed/active', ''),
    -- `finished` is there and `active` is gone: that is how an entry being removed looks, and its
    -- hosts must not be reported as Inactive.
    ('covgap-ddl-queue-removing', '/clickhouse/task_queue/ddl', 'version: 1\nquery: SELECT 1\nhosts: [\'h5:9005\']\n'),
    ('finished', '/clickhouse/task_queue/ddl/covgap-ddl-queue-removing', ''),
    -- No host is known for this entry at all.
    ('covgap-ddl-queue-hostless', '/clickhouse/task_queue/ddl', 'version: 1\nquery: SELECT 1\nhosts: []\n');

SELECT entry, entry_version, host, port, status, exception_code
FROM system.distributed_ddl_queue
WHERE startsWith(entry, 'covgap-ddl-queue-')
ORDER BY entry, host, port;

-- An entry whose format version this server does not support fails the whole read, not just its
-- own rows. The next statement puts the supported version back, so the table is readable again.
INSERT INTO system.zookeeper (name, path, value) VALUES ('covgap-ddl-queue-hostless', '/clickhouse/task_queue/ddl', 'version: 99\nquery: SELECT 1\nhosts: []\n');
SELECT count() FROM system.distributed_ddl_queue WHERE entry != ''; -- { serverError UNKNOWN_FORMAT_VERSION }
INSERT INTO system.zookeeper (name, path, value) VALUES ('covgap-ddl-queue-hostless', '/clickhouse/task_queue/ddl', 'version: 1\nquery: SELECT 1\nhosts: []\n');
SELECT count() FROM system.distributed_ddl_queue WHERE startsWith(entry, 'covgap-ddl-queue-');

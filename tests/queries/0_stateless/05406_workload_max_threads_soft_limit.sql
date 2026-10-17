-- Tags: no-parallel
-- no-parallel: workloads and resources are server-wide state (other tests list all workloads), and the server
--   allows only one MASTER THREAD and one WORKER THREAD resource at a time.

DROP WORKLOAD IF EXISTS 05406_child;
DROP WORKLOAD IF EXISTS 05406_root;
DROP RESOURCE IF EXISTS 05406_cpu;

CREATE RESOURCE 05406_cpu (WORKER THREAD, MASTER THREAD);

-- The workload setting `max_threads_soft_limit`, also per resource with `FOR`
CREATE WORKLOAD 05406_root SETTINGS max_concurrent_threads = 2, max_threads_soft_limit = 4;
CREATE WORKLOAD 05406_child IN 05406_root SETTINGS max_threads_soft_limit = 1 FOR 05406_cpu;
SELECT name, create_query FROM system.workloads WHERE startsWith(name, '05406_') ORDER BY name;

-- `0` disables the limit
CREATE OR REPLACE WORKLOAD 05406_root SETTINGS max_concurrent_threads = 2, max_threads_soft_limit = 0;
SELECT create_query FROM system.workloads WHERE name = '05406_root';

CREATE WORKLOAD 05406_invalid IN 05406_root SETTINGS max_threads_soft_limit = -1; -- { serverError BAD_ARGUMENTS }
CREATE WORKLOAD 05406_invalid IN 05406_root SETTINGS max_threads_soft_limit = 1.5; -- { serverError BAD_GET }

-- A query above the soft limit still runs to completion: its demoted slot requests are served when nothing else waits
SELECT sum(number) FROM numbers_mt(1000000) SETTINGS workload = '05406_child', max_threads = 4;
-- The query setting defaults to `-1` (use the workload value); a non-negative value overrides the workload value,
-- and `0` disables the limit for the query
SELECT value, changed FROM system.settings WHERE name = 'max_threads_soft_limit';
SELECT sum(number) FROM numbers_mt(1000000) SETTINGS workload = '05406_child', max_threads = 4, max_threads_soft_limit = -1;
SELECT sum(number) FROM numbers_mt(1000000) SETTINGS workload = '05406_child', max_threads = 4, max_threads_soft_limit = 2;
SELECT sum(number) FROM numbers_mt(1000000) SETTINGS workload = '05406_child', max_threads = 4, max_threads_soft_limit = 0;
SELECT sum(number) FROM numbers_mt(1000000) SETTINGS workload = '05406_child', max_threads = 4, max_threads_soft_limit = -2; -- { serverError BAD_ARGUMENTS }

DROP WORKLOAD 05406_child;
DROP WORKLOAD 05406_root;
DROP RESOURCE 05406_cpu;

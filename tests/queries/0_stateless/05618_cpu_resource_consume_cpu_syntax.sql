-- Tags: no-parallel
-- no-parallel: the server allows only one CPU resource at a time

DROP RESOURCE IF EXISTS 05618_cpu;
DROP RESOURCE IF EXISTS 05618_master;
DROP RESOURCE IF EXISTS 05618_second;

CREATE RESOURCE 05618_cpu (CONSUME CPU);
SELECT create_query FROM system.resources WHERE name = '05618_cpu';

-- One resource schedules the CPU of all query threads
CREATE RESOURCE 05618_second (CONSUME CPU); -- { serverError BAD_ARGUMENTS }
CREATE RESOURCE 05618_second (WORKER THREAD); -- { serverError BAD_ARGUMENTS }
CREATE RESOURCE 05618_second (CONSUME CPU, READ DISK d); -- { clientError BAD_ARGUMENTS }

-- The deprecated syntax is accepted, keeps its spelling and is reported in `system.warnings`
CREATE RESOURCE 05618_master (MASTER THREAD);
SELECT create_query FROM system.resources WHERE name = '05618_master';
SELECT count() FROM system.warnings WHERE message LIKE '%deprecated `MASTER THREAD` / `WORKER THREAD` syntax: `05618_master`%';
DROP RESOURCE 05618_master;
SELECT count() FROM system.warnings WHERE message LIKE '%deprecated `MASTER THREAD` / `WORKER THREAD` syntax%';

CREATE OR REPLACE RESOURCE 05618_cpu (MASTER THREAD, WORKER THREAD);
SELECT create_query FROM system.resources WHERE name = '05618_cpu';
CREATE OR REPLACE RESOURCE 05618_cpu (CONSUME CPU);
SELECT count() FROM system.warnings WHERE message LIKE '%deprecated `MASTER THREAD` / `WORKER THREAD` syntax%';

DROP RESOURCE 05618_cpu;

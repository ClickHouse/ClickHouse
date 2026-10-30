-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates a global user with a fixed name
-- no-replicated-database: DDL in a `Replicated` database is executed with a context without the user, so it is not checked

-- Expressions stored in a table definition are checked against the user who defines the table.
-- AI functions are executed only in queries run by a user, so they are rejected there even with the privilege.

DROP USER IF EXISTS u_05442;
CREATE USER u_05442 IDENTIFIED WITH no_password;
GRANT CREATE TABLE, INSERT, SELECT, ALTER ON * TO u_05442;
GRANT TABLE ENGINE ON MergeTree TO u_05442;

EXECUTE AS u_05442 CREATE TABLE t1 (x String, d DateTime) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t2 (x String) ENGINE = MergeTree ORDER BY length(aiEmbed(x, 'm')); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t3 (x String) ENGINE = MergeTree PARTITION BY length(aiEmbed(x, 'm')) ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t4 (x String, INDEX i length(aiEmbed(x, 'm')) TYPE minmax) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t5 (x String, PROJECTION p (SELECT aiGenerate(x) ORDER BY x)) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t8 (k String, d DateTime, x String) ENGINE = MergeTree ORDER BY (k, d) TTL d + INTERVAL 1 DAY GROUP BY k SET x = any(aiGenerate(x)); -- { serverError ACCESS_DENIED }

EXECUTE AS u_05442 CREATE TABLE t7 (x String, d DateTime) ENGINE = MergeTree ORDER BY x;
EXECUTE AS u_05442 ALTER TABLE t7 MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD INDEX i length(aiEmbed(x, 'm')) TYPE minmax; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD PROJECTION p (SELECT aiGenerate(x) ORDER BY x); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD COLUMN y String, MODIFY ORDER BY (x, length(aiEmbed(y, 'm'))); -- { serverError ACCESS_DENIED }

GRANT AI FUNCTIONS ON *.* TO u_05442;
EXECUTE AS u_05442 CREATE TABLE t1 (x String, d DateTime) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 CREATE TABLE t2 (x String) ENGINE = MergeTree ORDER BY length(aiEmbed(x, 'm')); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 CREATE TABLE t8 (k String, d DateTime, x String) ENGINE = MergeTree ORDER BY (k, d) TTL d + INTERVAL 1 DAY GROUP BY k SET x = any(aiGenerate(x)); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 ALTER TABLE t7 MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD PROJECTION p (SELECT aiGenerate(x) ORDER BY x); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 CREATE TABLE t3 (x String) ENGINE = MergeTree PARTITION BY length(aiEmbed(x, 'm')) ORDER BY x; -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 CREATE TABLE t4 (x String, INDEX i length(aiEmbed(x, 'm')) TYPE minmax) ENGINE = MergeTree ORDER BY x; -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD INDEX i length(aiEmbed(x, 'm')) TYPE minmax; -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD COLUMN y String, MODIFY ORDER BY (x, length(aiEmbed(y, 'm'))); -- { serverError NOT_IMPLEMENTED }
-- A `DEFAULT` or `MATERIALIZED` column added by `ALTER` is computed in the background for the existing parts.
EXECUTE AS u_05442 ALTER TABLE t7 ADD COLUMN c String DEFAULT aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD COLUMN m String MATERIALIZED aiGenerate(x); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't7';


DROP TABLE t7;
DROP USER u_05442;

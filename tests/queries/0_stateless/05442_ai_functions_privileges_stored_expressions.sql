-- Tags: no-parallel
-- no-parallel: creates a global user with a fixed name

-- Expressions stored in a table definition are checked against the user who defines the table.

DROP USER IF EXISTS u_05442;
CREATE USER u_05442 IDENTIFIED WITH no_password;
GRANT CREATE TABLE, INSERT, SELECT, ALTER ON * TO u_05442;

CREATE TABLE src (x String, d DateTime) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c');

EXECUTE AS u_05442 CREATE TABLE t1 (x String, d DateTime) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t2 (x String) ENGINE = MergeTree ORDER BY length(aiEmbed(x, 'm')); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t3 (x String) ENGINE = MergeTree PARTITION BY length(aiEmbed(x, 'm')) ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t4 (x String, INDEX i length(aiEmbed(x, 'm')) TYPE minmax) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t5 (x String, PROJECTION p (SELECT aiGenerate(x) ORDER BY x)) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 CREATE TABLE t6 AS src; -- { serverError ACCESS_DENIED }

EXECUTE AS u_05442 CREATE TABLE t7 (x String, d DateTime) ENGINE = MergeTree ORDER BY x;
EXECUTE AS u_05442 ALTER TABLE t7 MODIFY TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c'); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD INDEX i length(aiEmbed(x, 'm')) TYPE minmax; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD PROJECTION p (SELECT aiGenerate(x) ORDER BY x); -- { serverError ACCESS_DENIED }
EXECUTE AS u_05442 ALTER TABLE t7 ADD COLUMN y String, MODIFY ORDER BY (x, length(aiEmbed(y, 'm'))); -- { serverError ACCESS_DENIED }

GRANT AI FUNCTIONS ON *.* TO u_05442;
EXECUTE AS u_05442 CREATE TABLE t1 (x String, d DateTime) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY DELETE WHERE aiFilter(x, 'c');
EXECUTE AS u_05442 CREATE TABLE t6 AS src;

DROP TABLE src;
DROP TABLE t1;
DROP TABLE t6;
DROP TABLE t7;
DROP USER u_05442;

-- A key, skip index or projection sorting key expression that has the name of a stored column was used in place of that
-- column when the table was written or queried:
-- https://github.com/ClickHouse/ClickHouse/issues/133178

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_alter;
DROP TABLE IF EXISTS t_ok;
DROP TABLE IF EXISTS t_legacy;

-- Refused at CREATE.
CREATE TABLE t_bad (x String, `lower(x)` String) ENGINE = MergeTree ORDER BY lower(x); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x String, `lower(x)` String) ENGINE = MergeTree ORDER BY (x, lower(x)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x String, `lower(x)` String MATERIALIZED upper(x)) ENGINE = MergeTree ORDER BY lower(x); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (name String, `length(name)` UInt64, INDEX il length(name) TYPE minmax) ENGINE = MergeTree ORDER BY name; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x Int64, `negate(x)` Int64) ENGINE = MergeTree ORDER BY `negate(x)` PRIMARY KEY negate(x); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x Int64, `negate(x)` Int64) ENGINE = MergeTree ORDER BY (negate(x), `negate(x)`); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x String, `cityHash64(x)` UInt64) ENGINE = MergeTree ORDER BY `cityHash64(x)` SAMPLE BY cityHash64(x); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x String, `lower(x)` String, PROJECTION p (SELECT * ORDER BY lower(x))) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_src (s String) ENGINE = Memory;
INSERT INTO t_src VALUES ('HeLLo');
CREATE TABLE t_bad ENGINE = MergeTree ORDER BY lower(s) EMPTY AS SELECT s, lower(s) FROM t_src; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_bad';

-- Refused at ALTER, when the ALTER introduces the name.
CREATE TABLE t_alter (x String, y String) ENGINE = MergeTree ORDER BY lower(x);
ALTER TABLE t_alter ADD COLUMN `lower(x)` String; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter RENAME COLUMN y TO `lower(x)`; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter ADD COLUMN z String, ADD COLUMN `lower(z)` String, MODIFY ORDER BY (lower(x), lower(z)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter ADD INDEX iy lower(y) TYPE minmax;
ALTER TABLE t_alter ADD COLUMN `lower(y)` String; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_alter' ORDER BY position;
DROP TABLE t_alter;

CREATE TABLE t_alter (x String, `lower(x)` String) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter ADD INDEX il lower(x) TYPE minmax; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alter ADD PROJECTION p (SELECT * ORDER BY lower(x)); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_alter';
DROP TABLE t_alter;

-- Accepted: the key reads the column itself, or the column is not stored, or it is not a key element, or the projection
-- does not store it.
CREATE TABLE t_ok (x String, `lower(x)` String ALIAS upper(x)) ENGINE = MergeTree ORDER BY lower(x);
INSERT INTO t_ok VALUES ('B'), ('A');
SELECT x, `lower(x)` FROM t_ok ORDER BY x;
DROP TABLE t_ok;

CREATE TABLE t_ok (x String, `lower(x)` String, INDEX ic (x, `lower(x)`) TYPE minmax) ENGINE = MergeTree ORDER BY `lower(x)`;
INSERT INTO t_ok VALUES ('B', 'q'), ('A', 'r');
SELECT * FROM t_ok ORDER BY x;
DROP TABLE t_ok;

CREATE TABLE t_ok (x String, `lower(x)` String, PROJECTION p (SELECT x ORDER BY lower(x))) ENGINE = MergeTree ORDER BY length(lower(x));
INSERT INTO t_ok VALUES ('B', 'q'), ('A', 'r');
SELECT * FROM t_ok ORDER BY x;
DROP TABLE t_ok;

CREATE TABLE t_ok (d Date, `toYYYYMM(d)` UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d;
INSERT INTO t_ok VALUES ('2024-01-01', 7);
SELECT * FROM t_ok;
DROP TABLE t_ok;

-- A table created with the setting keeps working, and only an ALTER that adds another such element is refused.
CREATE TABLE t_legacy (x String, `lower(x)` String) ENGINE = MergeTree ORDER BY lower(x) SETTINGS allow_suspicious_indices = 1;
INSERT INTO t_legacy VALUES ('B', 'q');
ALTER TABLE t_legacy ADD COLUMN z UInt8;
ALTER TABLE t_legacy ADD COLUMN y String, MODIFY ORDER BY (lower(x), y);
ALTER TABLE t_legacy ADD INDEX il lower(x) TYPE minmax; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_legacy ADD INDEX il lower(x) TYPE minmax SETTINGS allow_suspicious_indices = 1;
ALTER TABLE t_legacy RESET SETTING allow_suspicious_indices;
DETACH TABLE t_legacy;
ATTACH TABLE t_legacy;
SELECT count() FROM t_legacy;
DROP TABLE t_legacy;

DROP TABLE t_src;

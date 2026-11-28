-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS t_keeper_map_version;
DROP TABLE IF EXISTS t_keeper_map_version_alias;

CREATE TABLE t_keeper_map_version (key UInt64, _version UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_keeper_map_version (_version UInt64, value UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY _version; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_keeper_map_version (key UInt64, _version Int32) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_keeper_map_version (key UInt64, _version UInt64 MATERIALIZED key) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_keeper_map_version ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key AS SELECT 1 AS key, 2 AS _version; -- { serverError ILLEGAL_COLUMN }
SET allow_reserved_column_names = 1;
CREATE TABLE t_keeper_map_version (key UInt64, _version UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_keeper_map_version_alias (key UInt64, value UInt64, _version UInt64 ALIAS value + 1) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version_alias') PRIMARY KEY key;
INSERT INTO t_keeper_map_version_alias VALUES (1, 10), (2, 20);
SELECT key, value, _version FROM t_keeper_map_version_alias ORDER BY key;
DROP TABLE t_keeper_map_version_alias;
CREATE TABLE t_keeper_map_version (key UInt64, version UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/t_keeper_map_version') PRIMARY KEY key;
INSERT INTO t_keeper_map_version SELECT number, 1 FROM numbers(1000);
SELECT count(), sum(version), max(_version) FROM t_keeper_map_version;
DROP TABLE t_keeper_map_version;

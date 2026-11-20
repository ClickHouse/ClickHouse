-- Tags: no-fasttest, need-query-parameters
-- Tag no-fasttest: needs S3 (MinIO) and Iceberg (Avro)

-- An object storage table renamed by RENAME TABLE or EXCHANGE TABLES is read and altered under its new name.

SET allow_insert_into_iceberg = 1, iceberg_delete_data_on_drop = 0;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_a2;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_target;
DROP TABLE IF EXISTS t_ice;
DROP TABLE IF EXISTS t_ice2;

CREATE TABLE t_a (c0 Int32) ENGINE = S3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05837/a.native', format = Native);
INSERT INTO t_a SETTINGS s3_truncate_on_insert = 1 VALUES (1);
RENAME TABLE t_a TO t_a2;
SELECT DISTINCT _table FROM t_a2;

ALTER TABLE t_a2 COMMENT COLUMN c0 'renamed';
SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_a2';
DETACH TABLE t_a2;
ATTACH TABLE t_a2;
SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_a2';

-- The stored definition of an `Alias` table has no columns.
CREATE TABLE t_target (c0 Int32) ENGINE = Memory;
CREATE TABLE t_alias ENGINE = Alias(t_target);
CREATE TABLE t_c (c0 Int32) ENGINE = S3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05837/c.native', format = Native);
EXCHANGE TABLES t_c AND t_alias;
ALTER TABLE t_alias COMMENT COLUMN c0 'exchanged';
SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name IN ('t_c', 't_alias') ORDER BY name;
SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alias';

CREATE TABLE t_ice (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05837/ice/');
RENAME TABLE t_ice TO t_ice2;
ALTER TABLE t_ice2 ADD COLUMN c1 Nullable(Int32);
DESCRIBE t_ice2;
ALTER TABLE t_ice2 MODIFY SETTING iceberg_use_version_hint = 1;
SELECT engine_full LIKE '%iceberg_use_version_hint = 1%' FROM system.tables WHERE database = currentDatabase() AND name = 't_ice2';

DROP TABLE t_a2;
DROP TABLE t_alias;
DROP TABLE t_c;
DROP TABLE t_target;
DROP TABLE t_ice2;

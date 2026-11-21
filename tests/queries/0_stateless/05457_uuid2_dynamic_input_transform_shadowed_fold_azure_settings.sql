-- Tags: no-fasttest
-- no-fasttest: uses `azureBlobStorage`

-- 1. `transform` keeps a mapping when its input (also nested) is a `Dynamic` holding a `UUID2` value.
-- 2. A lambda shadowing a string function is not folded inside a type name argument by `uuid_type_version = 2`.
-- 3. The structure of `azureBlobStorage` with a trailing `SETTINGS` clause or `extra_credentials` is materialized.

SELECT '-- transform';
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    transform(toUUID2(s), [toUUID2(s)], [7], 0),
    transform(CAST(toUUID2(s) AS Dynamic), [toUUID2(s)], [7], 0),
    transform(tuple(CAST(toUUID2(s) AS Dynamic)), [tuple(toUUID2(s))], [7], 0),
    CASE CAST(toUUID2(s) AS Dynamic) WHEN toUUID2(s) THEN 7 ELSE 0 END;

SELECT '-- shadowed fold';
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_shadowed_fold;
CREATE TABLE t_shadowed_fold ENGINE = Memory AS
WITH ((x, y) -> 'String') AS concat
SELECT CAST('x', concat('U', 'UID')) AS s;
SELECT s, toTypeName(s) FROM t_shadowed_fold;
DROP TABLE t_shadowed_fold;

SELECT '-- azure';
-- `extra_credentials` without an account key resolves to the server's own identity, which user queries must opt into.
SET s3_allow_server_credentials_in_user_queries = 1;
DROP VIEW IF EXISTS v_azure_settings;
DROP VIEW IF EXISTS v_azure_credentials;
DROP VIEW IF EXISTS v_azure_cluster;
CREATE VIEW v_azure_settings AS SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 'path', 'CSV', 'auto', 'id UUID', SETTINGS format_csv_delimiter = ';');
CREATE VIEW v_azure_credentials AS SELECT * FROM azureBlobStorage('http://127.0.0.1:1/devstoreaccount1', 'cont', 'path', 'CSV', 'auto', 'id UUID', extra_credentials(client_id = 'c', tenant_id = 't'));
CREATE VIEW v_azure_cluster AS SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'http://127.0.0.1:1/devstoreaccount1', 'cont', 'path', 'CSV', 'auto', 'id UUID', extra_credentials(client_id = 'c', tenant_id = 't'), SETTINGS format_csv_delimiter = ';');
SELECT name, position(create_table_query, '\'`id` UUID2\'') > 0 FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'v_azure_%' ORDER BY name;
DROP VIEW v_azure_settings;
DROP VIEW v_azure_credentials;
DROP VIEW v_azure_cluster;

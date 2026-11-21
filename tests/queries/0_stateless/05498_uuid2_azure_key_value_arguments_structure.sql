-- Tags: no-fasttest
-- no-fasttest: uses `azureBlobStorage`

-- Under `uuid_type_version = 2`, the structure of `azureBlobStorage` is materialized also when the call has
-- trailing key-value arguments, which do not take a positional slot.

SET uuid_type_version = 2;
-- `extra_credentials` without an account key resolves to the server's own identity, which user queries must opt into.
SET s3_allow_server_credentials_in_user_queries = 1;

DROP VIEW IF EXISTS v_azure_kv_strategy;
DROP VIEW IF EXISTS v_azure_kv_both;
DROP VIEW IF EXISTS v_azure_kv_credentials;
DROP VIEW IF EXISTS v_azure_kv_cluster;

CREATE VIEW v_azure_kv_strategy AS SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 'path', 'CSV', 'auto', 'id UUID', partition_strategy = 'wildcard');
CREATE VIEW v_azure_kv_both AS SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 'path', 'CSV', 'auto', 'id UUID', partition_strategy = 'wildcard', partition_columns_in_data_file = 1, SETTINGS format_csv_delimiter = ';');
CREATE VIEW v_azure_kv_credentials AS SELECT * FROM azureBlobStorage('http://127.0.0.1:1/devstoreaccount1', 'cont', 'path', 'CSV', 'auto', 'id UUID', partition_strategy = 'wildcard', extra_credentials(client_id = 'c', tenant_id = 't'));
CREATE VIEW v_azure_kv_cluster AS SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:1/devstoreaccount1;', 'cont', 'path', 'CSV', 'auto', 'id UUID', partition_strategy = 'wildcard');

SELECT name, position(create_table_query, '\'`id` UUID2\'') > 0 FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'v_azure_kv_%' ORDER BY name;

DROP VIEW v_azure_kv_strategy;
DROP VIEW v_azure_kv_both;
DROP VIEW v_azure_kv_credentials;
DROP VIEW v_azure_kv_cluster;

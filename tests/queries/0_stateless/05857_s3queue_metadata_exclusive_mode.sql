-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` and `AzureQueue` engines are not built in the fast test.
-- zookeeper: the queue metadata lives in keeper.

-- A queue in `exclusive` mode keeps the state of its files only in memory, so it has no
-- `processing`, `failed` or `processed` directories in keeper.

CREATE TABLE q_s3 (x String) ENGINE = S3Queue('http://localhost:11111/test/05857_s3/*', NOSIGN, 'CSV')
SETTINGS mode = 'exclusive', keeper_path = '/clickhouse/{database}/05857_s3';

CREATE TABLE q_azure (x String) ENGINE = AzureQueue('http://localhost:11111', 'test', '05857_azure/*', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV')
SETTINGS mode = 'exclusive', keeper_path = '/clickhouse/{database}/05857_azure';

SELECT processing_nodes_count, failed_nodes_count FROM system.s3_queue_metadata
WHERE zookeeper_path = '/clickhouse/' || currentDatabase() || '/05857_s3';

SELECT processed_nodes_count, processing_nodes_count, failed_nodes_count, processed_nodes, processing_nodes, failed_nodes, processed_path
FROM system.s3_queue_metadata
WHERE zookeeper_path = '/clickhouse/' || currentDatabase() || '/05857_s3';

SELECT processed_nodes_count, processing_nodes_count, failed_nodes_count, processed_nodes, processing_nodes, failed_nodes, processed_path
FROM system.azure_queue_metadata
WHERE zookeeper_path = '/clickhouse/' || currentDatabase() || '/05857_azure';

DROP TABLE q_s3;
DROP TABLE q_azure;

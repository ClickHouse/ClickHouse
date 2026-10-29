-- Tags: no-fasttest
-- Tag no-fasttest: the Kafka, NATS, RabbitMQ, MySQL and PostgreSQL engines are not built in the fast test.

-- `system.engine_settings` lists the settings a table takes from elsewhere unless its definition states them, with
-- `inherited` = 1, and the settings the engine sets itself, with the value the engine uses and `inherited` = 0.

-- The engines whose settings are all inherited are listed too.
SELECT name FROM system.table_engines
WHERE name IN ('S3', 'AzureBlobStorage', 'HDFS', 'COSN', 'OSS', 'GCS', 'PostgreSQL')
    AND name NOT IN (SELECT DISTINCT engine_name FROM system.engine_settings);

-- Settings taken from the query or server settings, the server configuration or the creating session.
SELECT engine_name, name, inherited FROM system.engine_settings
WHERE (engine_name = 'Kafka' AND name IN ('kafka_max_block_size', 'format_csv_delimiter'))
    OR (engine_name = 'PostgreSQL' AND name = 'postgresql_connection_pool_size')
    OR (engine_name = 'MySQL' AND name = 'mysql_datatypes_support_level')
    OR (engine_name = 'S3Queue' AND name = 'keeper_path')
    OR (engine_name IN ('S3Queue', 'AzureQueue') AND name IN ('min_insert_block_size_rows_for_materialized_views',
        'min_insert_block_size_bytes_for_materialized_views'))
    OR (engine_name IN ('Executable', 'ExecutablePool') AND name = 'max_command_execution_time')
    OR (engine_name = 'RabbitMQ' AND name = 'rabbitmq_vhost')
ORDER BY engine_name, name;

-- The format settings `Kafka`, `NATS` and `RabbitMQ` set themselves when a table does not state them.
SELECT engine_name, name, value, changed, readonly, inherited FROM system.engine_settings
WHERE engine_name IN ('Kafka', 'NATS', 'RabbitMQ')
    AND name IN ('input_format_skip_unknown_fields', 'input_format_allow_errors_ratio', 'input_format_allow_errors_num')
ORDER BY engine_name, name;

-- The header detection the streaming engines always disable.
SELECT engine_name, name, value, changed, readonly, inherited FROM system.engine_settings
WHERE engine_name IN ('Kafka', 'FileLog', 'NATS', 'RabbitMQ')
    AND name IN ('input_format_csv_detect_header', 'input_format_tsv_detect_header', 'input_format_custom_detect_header')
ORDER BY engine_name, name;

-- Credentials show only their compiled default, never the server configuration's.
SELECT engine_name, name, value = '' FROM system.engine_settings
WHERE name IN ('kafka_sasl_password', 'nats_password', 'rabbitmq_password')
ORDER BY engine_name;

-- No `MergeTree` row is inherited: they match `system.merge_tree_settings`.
SELECT countIf(inherited) FROM system.engine_settings WHERE endsWith(engine_name, 'MergeTree');

-- `Set`, `Hive` and `FileLog` do not list the format settings they never read; `FileLog` lists only the header detection.
SELECT count() FROM system.engine_settings WHERE engine_name = 'Set' AND name LIKE '%format%';
SELECT count() FROM system.engine_settings WHERE engine_name = 'Hive' AND name LIKE '%format%';
SELECT count() FROM system.engine_settings WHERE engine_name = 'FileLog' AND name LIKE '%format%' AND name NOT LIKE '%detect\_header';

-- `YTsaurus` does not list the settings only the YTsaurus dictionary sources take, which the table engine rejects.
SELECT count() > 0, countIf(name IN ('lookup_throttler_max_requests_per_second', 'lookup_max_rows_per_query'))
FROM system.engine_settings WHERE engine_name = 'YTsaurus';

-- The `Iceberg` engines take their compaction settings from the query settings of the same names; the other data lake
-- engines, which do not read them, do not.
SELECT engine_name, countIf(inherited) FROM system.engine_settings
WHERE engine_name IN ('IcebergS3', 'PaimonS3')
    AND name IN ('allow_experimental_iceberg_compaction', 'allow_experimental_cleanup_old_data_files_compaction',
        'iceberg_compaction_delay_bias', 'iceberg_compaction_data_cleanup', 'iceberg_compaction_commit_batch_size',
        'iceberg_compaction_max_rows_in_data_file', 'iceberg_compaction_max_bytes_in_data_file',
        'iceberg_max_number_datafiles_to_compact', 'iceberg_data_file_size_lower_threshold_compaction',
        'iceberg_data_file_size_upper_threshold_compaction')
GROUP BY engine_name ORDER BY engine_name;

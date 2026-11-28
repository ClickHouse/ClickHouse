-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.

-- `system.s3_queue_settings` hides the secrets that `SHOW CREATE TABLE` hides.

DROP TABLE IF EXISTS t_secrets;

CREATE TABLE t_secrets (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_secrets/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered',
    after_processing_move_access_key_id = 'key_id_05493',
    after_processing_move_secret_access_key = 'SEKRIT_05493_1',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SEKRIT_05493_2;';

SELECT name, value
FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 't_secrets'
    AND name IN ('after_processing_move_access_key_id', 'after_processing_move_secret_access_key',
        'after_processing_move_connection_string', 'mode')
ORDER BY name;

DROP TABLE t_secrets;

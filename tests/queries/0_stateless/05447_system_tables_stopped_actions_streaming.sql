-- Tags: no-fasttest
-- `Kafka` is not built in fast tests. Inspect local locks without a broker or dependent view.
SET send_logs_level = 'fatal';

CREATE TABLE stopped_actions_stream (x UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'stopped_actions',
         kafka_group_name = 'stopped_actions', kafka_format = 'JSONEachRow';

SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM PAUSE stopped_actions_stream;
SYSTEM PAUSE stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM STOP stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM START stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';

DROP TABLE stopped_actions_stream;

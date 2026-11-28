-- Tags: no-fasttest
-- Tag no-fasttest: the Kafka engine is not built in the fast test.

-- `system.table_settings` has the rows `system.engine_settings` has for the table's engine. A setting the table takes
-- from elsewhere has `inherited` = 1 and the engine's own value while the definition does not state it, and
-- `inherited` = 0 with the stated value once it does.

CREATE TABLE kafka_plain (a Int32) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'CSV';

CREATE TABLE kafka_stated (a Int32) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'CSV',
    kafka_max_block_size = 100, format_csv_delimiter = ';';

-- The same names, and the same inherited ones, as the engine's rows.
SELECT
    (SELECT arraySort(groupArray(name)) FROM system.table_settings WHERE database = currentDatabase() AND table = 'kafka_plain')
    = (SELECT arraySort(groupArray(name)) FROM system.engine_settings WHERE engine_name = 'Kafka'),
    (SELECT arraySort(groupArray(name)) FROM system.table_settings WHERE database = currentDatabase() AND table = 'kafka_plain' AND inherited)
    = (SELECT arraySort(groupArray(name)) FROM system.engine_settings WHERE engine_name = 'Kafka' AND inherited);

SELECT table, name, value, changed, inherited FROM system.table_settings
WHERE database = currentDatabase() AND table LIKE 'kafka%' AND name IN ('kafka_max_block_size', 'format_csv_delimiter')
ORDER BY table, name;

DROP TABLE kafka_plain;
DROP TABLE kafka_stated;

-- Tags: no-fasttest
-- Tag no-fasttest: Kafka is not built in the fast test

-- An INSERT with no rows per message for a row-based format must fail instead of producing empty messages forever.
CREATE TABLE kafka_key (`_key` String, x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow',
        kafka_max_rows_per_message = 0;
INSERT INTO kafka_key VALUES ('k', 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE kafka_avro (x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'Avro';
INSERT INTO kafka_avro SETTINGS output_format_avro_rows_in_file = 0 VALUES (1); -- { serverError BAD_ARGUMENTS }

DROP TABLE kafka_key;
DROP TABLE kafka_avro;

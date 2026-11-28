-- Tags: no-fasttest
-- Tag no-fasttest: the NATS, RabbitMQ and Kafka engines are not built in the fast test

-- Invalid settings are rejected before connecting to the broker, so the hosts below do not need to exist.
SET send_logs_level = 'fatal';

-- Zero consumers, or a number that does not fit an `int`, failed an assertion while creating the consumer semaphore.
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_num_consumers = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_num_consumers = 2147483648; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_num_consumers = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_num_consumers = 4294967296; -- { serverError BAD_ARGUMENTS }

-- The reconnect wait is passed to the NATS client as an `int`.
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_reconnect_wait = 2147483648; -- { serverError BAD_ARGUMENTS }

-- An INSERT with no rows per message for a row-based format must fail instead of producing empty messages forever.
CREATE TABLE kafka_key (`_key` String, x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow',
        kafka_max_rows_per_message = 0;
INSERT INTO kafka_key VALUES ('k', 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE kafka_avro (x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'Avro';
INSERT INTO kafka_avro SETTINGS output_format_avro_rows_in_file = 0 VALUES (1); -- { serverError BAD_ARGUMENTS }

-- A block format writes the whole chunk as one message and ignores the limit, so 0 stays accepted.
CREATE TABLE kafka_native (x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'Native',
        kafka_max_rows_per_message = 0;
INSERT INTO kafka_native SELECT number FROM numbers(0);

DROP TABLE kafka_key;
DROP TABLE kafka_avro;
DROP TABLE kafka_native;

SELECT count() FROM system.tables WHERE database = currentDatabase();

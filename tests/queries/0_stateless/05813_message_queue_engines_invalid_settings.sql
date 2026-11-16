-- Tags: no-fasttest
-- Tag no-fasttest: the NATS, RabbitMQ and Kafka engines are not built in the fast test.

-- Invalid settings are rejected before connecting to the broker, so the hosts below do not need to exist.

-- Suppress expected connection errors from reaching client stderr.
SET send_logs_level = 'fatal';

-- Zero consumers used to fail an assertion while creating the consumer semaphore, which made
-- the `DDLWorker` retry the `ON CLUSTER` query forever. A value that does not fit an `int` failed the same way.
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_num_consumers = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_num_consumers = 2147483648; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_num_consumers = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_num_consumers = 4294967296; -- { serverError BAD_ARGUMENTS }

-- Zero rows per message made `INSERT` produce empty messages forever, without reacting to `KILL QUERY`.
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_max_rows_per_message = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_max_rows_per_message = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_kafka (x UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow', kafka_max_rows_per_message = 0; -- { serverError BAD_ARGUMENTS }

-- A RabbitMQ table without queues consumes nothing.
CREATE TABLE t_rmq (x UInt64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = 'localhost:5672', rabbitmq_username = 'u', rabbitmq_password = 'p', rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow', rabbitmq_num_queues = 0; -- { serverError BAD_ARGUMENTS }

-- The reconnect wait is passed to the NATS client as an `int`.
CREATE TABLE t_nats (x UInt64) ENGINE = NATS
SETTINGS nats_url = 'localhost:4222', nats_subjects = 's', nats_format = 'JSONEachRow', nats_reconnect_wait = 2147483648; -- { serverError BAD_ARGUMENTS }

-- `output_format_avro_rows_in_file` overrides the rows per message for `Avro`, so the sink checks it too.
CREATE TABLE t_kafka_avro (x UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'Avro';
INSERT INTO t_kafka_avro SETTINGS output_format_avro_rows_in_file = 0 VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_kafka_avro;

SELECT count() FROM system.tables WHERE database = currentDatabase();

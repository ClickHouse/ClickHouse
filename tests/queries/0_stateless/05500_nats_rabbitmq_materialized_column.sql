-- Tags: no-fasttest
-- Tag no-fasttest: the NATS and RabbitMQ engines are not built in the fast test.

-- MATERIALIZED columns are rejected at CREATE time (before any connection is made),
-- otherwise every INSERT fails with a block structure mismatch.
CREATE TABLE t_nats (c0 Int32, c1 UInt64 MATERIALIZED 7)
ENGINE = NATS SETTINGS nats_url = '127.0.0.1:4222', nats_subjects = 's', nats_format = 'JSONEachRow'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_rmq (c0 Int32, c1 UInt64 MATERIALIZED 7)
ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = '127.0.0.1:5672', rabbitmq_username = 'u', rabbitmq_password = 'p',
    rabbitmq_exchange_name = 'e', rabbitmq_format = 'JSONEachRow'; -- { serverError BAD_ARGUMENTS }

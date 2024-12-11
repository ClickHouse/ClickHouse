SET allow_kafka_use_default_column = 1;
CREATE TABLE queue1 (
                        timestamp UInt64 default 0,
                        level String,
                        message String
) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');
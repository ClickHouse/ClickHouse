-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- The streaming exchange compresses with `distributed_plan_exchange_compression_method`, `LZ4` by default,
-- whatever `network_compression_method` is. An empty value uses `network_compression_method`.
-- A run compresses its packets when the exchange writes fewer bytes to the sockets than the Native
-- blocks it serializes.

DROP TABLE IF EXISTS t_exchange_codec_default;
CREATE TABLE t_exchange_codec_default (k String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_exchange_codec_default SELECT concat('key ', toString(number % 1000), ' with some repeated words'), number FROM numbers(100000);

-- The checks of the default need the defaults of this version: with an older `compatibility`, which the stress
-- test can pass to the client, the exchange uses the network codec.
SET compatibility = '';
SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2;
-- The shuffle carries the input rows, which compress well: aggregating before the shuffle would leave
-- one small state per key to send.
SET distributed_plan_force_shuffle_aggregation = 1;

SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS network_compression_method = 'NONE', log_comment = '05407_default';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = '', network_compression_method = 'NONE', log_comment = '05407_empty_none';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = '', network_compression_method = 'LZ4', log_comment = '05407_empty_lz4';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = 'NONE', network_compression_method = 'ZSTD', log_comment = '05407_none_zstd';
-- `ZSTD` of the exchange takes its level from `distributed_plan_exchange_zstd_compression_level`, not from
-- `network_zstd_compression_level`: only the level of the exchange makes the codec reject a level above 22.
-- `network_compression_method` is `LZ4`, so that the connection to the client does not use the network level.
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = 'ZSTD', distributed_plan_exchange_zstd_compression_level = 1,
    network_compression_method = 'LZ4', network_zstd_compression_level = 23, log_comment = '05407_zstd_exchange_level';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = 'ZSTD', distributed_plan_exchange_zstd_compression_level = 23,
    network_compression_method = 'LZ4', network_zstd_compression_level = 1; -- { serverError ILLEGAL_CODEC_PARAMETER }
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS compatibility = '26.10', network_compression_method = 'NONE', log_comment = '05407_compatibility_26_10';

SELECT count() FROM (SELECT k, count() FROM t_exchange_codec_default GROUP BY k)
  SETTINGS distributed_plan_exchange_compression_method = 'Delta'; -- { serverError BAD_ARGUMENTS }

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

-- The tasks of a run are the `query_log` rows whose `initial_query_id` is the query of that run.
-- The runs are listed by name, because other queries of the test also have a `log_comment` that starts with `05407_`.
CREATE VIEW v_exchange_codec_default AS
SELECT runs.log_comment AS run,
    sum(tasks.ProfileEvents['StreamingExchangeSerializedBytes']) AS serialized_bytes,
    sum(tasks.ProfileEvents['StreamingExchangeSendBytes']) AS sent_bytes
FROM system.query_log AS tasks
INNER JOIN (
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment IN ('05407_default', '05407_empty_none', '05407_empty_lz4', '05407_none_zstd', '05407_zstd_exchange_level', '05407_compatibility_26_10')
      AND type = 'QueryFinish') AS runs ON tasks.initial_query_id = runs.query_id
WHERE tasks.event_date >= yesterday() AND tasks.event_time >= now() - INTERVAL 10 MINUTE
  AND tasks.type = 'QueryFinish' AND tasks.query_id != runs.query_id
GROUP BY run;

SELECT run, serialized_bytes > 0 AS measured, sent_bytes < serialized_bytes AS compressed
FROM v_exchange_codec_default
ORDER BY run;

DROP VIEW v_exchange_codec_default;
DROP TABLE t_exchange_codec_default;

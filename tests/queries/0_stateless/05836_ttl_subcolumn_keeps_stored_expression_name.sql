-- A TTL on a subcolumn is evaluated through getSubcolumn internally, but the TTL infos stored in a part are
-- keyed by the original expression (`ts.d + toIntervalDay(1)`), so parts written by older versions keep
-- matching their TTL rule: the recompression codec is applied and DELETE WHERE removes the matching rows.

DROP TABLE IF EXISTS t_ttl_subcolumn_key;

CREATE TABLE t_ttl_subcolumn_key (id UInt64, ts Tuple(d DateTime), v String)
ENGINE = MergeTree ORDER BY id
TTL ts.d + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1)), ts.d + INTERVAL 1 DAY DELETE WHERE id < 3
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_ttl_subcolumn_key SELECT number, tuple(toDateTime('2000-01-01 00:00:00', 'UTC')), 'x' FROM numbers(10);

OPTIMIZE TABLE t_ttl_subcolumn_key FINAL;

SELECT recompression_ttl_info.expression, default_compression_codec
FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_subcolumn_key' AND active;

SELECT count() FROM t_ttl_subcolumn_key;

DROP TABLE t_ttl_subcolumn_key;

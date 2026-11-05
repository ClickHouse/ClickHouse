-- A mutation that only recalculates TTL infos evaluates the WHERE part of a ROWS WHERE rule,
-- so it must read the column owning a subcolumn used there (`j.flag`).

DROP TABLE IF EXISTS t_where_subcolumn;

CREATE TABLE t_where_subcolumn (k UInt64, d1 DateTime('UTC'), d2 DateTime('UTC'), j Tuple(flag UInt8), v String)
ENGINE = MergeTree ORDER BY k
TTL d1 + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)), d2 + INTERVAL 10 YEAR DELETE WHERE j.flag = 1
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_where_subcolumn SELECT number, toDateTime('2090-01-01 00:00:00', 'UTC'), toDateTime('2090-01-01 00:00:00', 'UTC'), tuple(toUInt8(1)), toString(number) FROM numbers(10);

ALTER TABLE t_where_subcolumn UPDATE d1 = toDateTime('2095-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 1;

SELECT count(), min(d1), max(d1), min(d2), max(d2) FROM t_where_subcolumn;

DROP TABLE t_where_subcolumn;

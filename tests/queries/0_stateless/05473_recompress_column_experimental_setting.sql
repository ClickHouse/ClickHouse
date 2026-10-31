-- `ALTER TABLE ... RECOMPRESS COLUMN` requires `allow_experimental_recompress_column`.

DROP TABLE IF EXISTS t_recompress_experimental;
CREATE TABLE t_recompress_experimental (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_recompress_experimental SELECT number FROM numbers(100);

SET mutations_sync = 2;

SET allow_experimental_recompress_column = 0;
ALTER TABLE t_recompress_experimental RECOMPRESS COLUMN a; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_recompress_experimental';

SET allow_experimental_recompress_column = 1;
ALTER TABLE t_recompress_experimental MODIFY COLUMN a UInt64 CODEC(ZSTD(1));
ALTER TABLE t_recompress_experimental RECOMPRESS COLUMN a;
SELECT count(), sum(a) FROM t_recompress_experimental;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_recompress_experimental' AND is_done;

DROP TABLE t_recompress_experimental;

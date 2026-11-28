-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL and Parquet

-- A projection part inherits the codec of its parent part, which may be an encryption codec from the settings
-- the parent part was written with, even if the current settings of the table don't have it.
-- Such a projection part is not written in `Parquet` format.
-- The columns `_block_number` and `_block_offset` are disabled: with them `MATERIALIZE PROJECTION` rewrites
-- the parent part with the current codec, so the projection part does not inherit the encryption codec.

SET allow_experimental_parquet_parts = 1;

DROP TABLE IF EXISTS t_parquet_projection_inherited_codec;

CREATE TABLE t_parquet_projection_inherited_codec (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'AES_128_GCM_SIV',
    enable_block_number_column = 0, enable_block_offset_column = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_parquet_projection_inherited_codec VALUES (1, 'a');

ALTER TABLE t_parquet_projection_inherited_codec MODIFY SETTING default_compression_codec = 'LZ4';
ALTER TABLE t_parquet_projection_inherited_codec ADD PROJECTION p (SELECT * ORDER BY s) WITH SETTINGS (min_rows_for_parquet_part = 1);
ALTER TABLE t_parquet_projection_inherited_codec MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT name, part_type != 'Parquet' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_parquet_projection_inherited_codec' AND active;

SELECT * FROM t_parquet_projection_inherited_codec ORDER BY s SETTINGS force_optimize_projection = 1;

DROP TABLE t_parquet_projection_inherited_codec;

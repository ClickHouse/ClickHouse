SET enable_subcolumn_codecs = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_subcolumn_codecs_alter;

CREATE TABLE t_subcolumn_codecs_alter
(
    id UInt64,
    payload Tuple(ts DateTime64(3), value Float64, meta Tuple(seq UInt64, score Float32)),
    attrs Map(UInt64, Float64) CODEC(ZSTD(3)),
    CODEC payload.ts (DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4',
    replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_subcolumn_codecs_alter;
INSERT INTO t_subcolumn_codecs_alter SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, (number, number / 3)), map(number, number) FROM numbers(100);

-- Set, replace and remove, with the three forms of a name.
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta.seq CODEC(T64, LZ4);
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN `payload`.`value` CODEC(Gorilla, ZSTD(1));
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN `payload.ts` CODEC(Delta, ZSTD(2));
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta.seq REMOVE CODEC;
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN IF EXISTS payload.missing CODEC(ZSTD);

SELECT 'after alter';
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_alter';

-- A codec change applies to the parts written afterwards.
INSERT INTO t_subcolumn_codecs_alter SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, (number, number / 3)), map(number, number) FROM numbers(100);
SELECT 'streams of the old and the new part';
SELECT part_name, substream, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_subcolumn_codecs_alter)
WHERE column = 'payload' ORDER BY part_name, substream;

-- A column switches between its own codec and codecs of its subcolumns in one statement.
SELECT 'switch';
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN attrs.values CODEC(Gorilla, LZ4), MODIFY COLUMN attrs REMOVE CODEC, MODIFY COLUMN attrs.keys CODEC(Delta, LZ4);
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_alter';
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN attrs CODEC(ZSTD(3)), MODIFY COLUMN attrs.keys REMOVE CODEC, MODIFY COLUMN attrs.values REMOVE CODEC;
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_alter';

-- Only the codec of a subcolumn can be changed.
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.value Float32; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta.seq REMOVE CODEC; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.missing CODEC(ZSTD); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.value CODEC(Gorilla), RENAME COLUMN payload TO p; -- { serverError NOT_IMPLEMENTED }

-- Declarations of one column never overlap.
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload CODEC(ZSTD); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta CODEC(ZSTD), MODIFY COLUMN payload.meta.seq CODEC(ZSTD); -- { serverError BAD_ARGUMENTS }

-- An ALIAS column has no streams, so a column with codecs cannot become ALIAS.
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload Tuple(ts DateTime64(3), value Float64, meta Tuple(seq UInt64, score Float32)) ALIAS (toDateTime64('2026-01-01 00:00:00', 3), 0., (0, 0.)); -- { serverError BAD_ARGUMENTS }

-- A type change re-validates the declarations: a declared subcolumn must stay, and its codec must still fit its type.
SYSTEM START MERGES t_subcolumn_codecs_alter;
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta.seq CODEC(T64, LZ4);
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload Tuple(ts DateTime64(3), value Float64, meta Tuple(seq String, score Float32)); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload Tuple(ts DateTime64(3), meta Tuple(seq UInt64, score Float32)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload Tuple(ts DateTime64(3), value Float64, meta Tuple(seq UInt64, score Float32, extra UInt8));
SELECT 'after type change';
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_alter' AND name = 'payload';

-- RENAME COLUMN keeps the declarations, DROP COLUMN drops them.
SELECT 'rename and drop';
ALTER TABLE t_subcolumn_codecs_alter RENAME COLUMN payload TO p;
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_alter' AND name = 'p';
ALTER TABLE t_subcolumn_codecs_alter RENAME COLUMN p TO payload;

-- A subcolumn of a column that the same statement adds or changes the type of.
ALTER TABLE t_subcolumn_codecs_alter ADD COLUMN extra Tuple(x UInt8, y UInt8), MODIFY COLUMN extra.x CODEC(ZSTD);
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_alter' AND name = 'extra';
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN extra.x REMOVE CODEC, MODIFY COLUMN extra Tuple(x UInt8, y UInt8, z UInt8), MODIFY COLUMN extra.z CODEC(LZ4HC(4));
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_alter' AND name = 'extra';
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN extra Tuple(x UInt8, y UInt8), MODIFY COLUMN extra.z CODEC(LZ4HC(4)); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_subcolumn_codecs_alter DROP COLUMN extra;
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_alter';

-- A column may not get the name of a declared subcolumn, also when the same statement declares it.
ALTER TABLE t_subcolumn_codecs_alter ADD COLUMN `payload.ts` UInt8; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.meta.score CODEC(ZSTD), ADD COLUMN `payload.meta.score` UInt8; -- { serverError BAD_ARGUMENTS }

-- Without the setting a declaration can be removed but not added.
SET enable_subcolumn_codecs = 0;
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.value CODEC(ZSTD); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_subcolumn_codecs_alter MODIFY COLUMN payload.value REMOVE CODEC;
SELECT 'after remove without the setting';
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_alter';
SELECT count(), sum(payload.meta.seq) FROM t_subcolumn_codecs_alter;

DROP TABLE t_subcolumn_codecs_alter;

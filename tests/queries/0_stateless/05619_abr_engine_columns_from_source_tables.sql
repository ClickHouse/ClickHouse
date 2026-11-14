-- The columns of an `ABR` table are taken from its source tables: they cannot be specified, and are not stored in its definition.

SET allow_experimental_abr = 1;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;

CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;

CREATE TABLE abr (ts DateTime, value UInt64) ENGINE = ABR('src', [1, 10], ('ts')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE abr AS src_1 ENGINE = ABR('src', [1, 10], ('ts')); -- { serverError BAD_ARGUMENTS }

CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'abr' ORDER BY position;
SELECT create_table_query NOT LIKE '%UInt64%' FROM system.tables WHERE database = currentDatabase() AND name = 'abr';

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;

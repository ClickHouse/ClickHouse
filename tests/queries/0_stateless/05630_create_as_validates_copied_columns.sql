-- CREATE TABLE ... AS copies the columns of another table, and they are checked as if they were written in the query.
DROP TABLE IF EXISTS src_codec;
DROP TABLE IF EXISTS src_statistics;
DROP TABLE IF EXISTS src_type;
DROP TABLE IF EXISTS copy;

CREATE TABLE src_codec (x UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_codecs = 1;
CREATE TABLE src_statistics (x UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_statistics = 1;
CREATE TABLE src_type (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_low_cardinality_types = 1;

SET allow_suspicious_codecs = 0, allow_statistics = 0, allow_suspicious_low_cardinality_types = 0;

CREATE TABLE copy (x UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE copy AS src_codec; -- { serverError BAD_ARGUMENTS }
CREATE TABLE copy (x UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE copy AS src_statistics; -- { serverError INCORRECT_QUERY }
CREATE TABLE copy (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
CREATE TABLE copy AS src_type; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

-- With the settings enabled, the copy is created.
CREATE TABLE copy AS src_codec SETTINGS allow_suspicious_codecs = 1;
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'copy' AND compression_codec != '';
DROP TABLE copy;

DROP TABLE src_codec;
DROP TABLE src_statistics;
DROP TABLE src_type;

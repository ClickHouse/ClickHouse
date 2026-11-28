-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- A lossy alternative of `FirstApplicable` is not allowed where the data type is unknown: the combinator would be
-- selected again for the type of every column, including a sorting key, the keys of a `Map` or a `LowCardinality`.

SET enable_sz3_codec = 1;
SET allow_suspicious_codecs = 0;

DROP TABLE IF EXISTS t_first_applicable_lossy;

CREATE TABLE t_first_applicable_lossy (k Float64, m Map(Float64, Float64)) ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'FirstApplicable(SZ3, ZSTD)'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_first_applicable_lossy (k Float64, m Map(Float64, Float64)) ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'FirstApplicable(Multiple(SZ3, ZSTD), ZSTD)'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_first_applicable_lossy (k Float64, t DateTime) ENGINE = MergeTree ORDER BY k
TTL t RECOMPRESS CODEC(FirstApplicable(SZ3, ZSTD)); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_first_applicable_lossy (k Float64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_first_applicable_lossy MODIFY SETTING default_compression_codec = 'FirstApplicable(SZ3, ZSTD)'; -- { serverError BAD_ARGUMENTS }

-- With a data type, a lossy alternative is checked for the column as usual.
CREATE TABLE t_first_applicable_lossy_map (m Map(Float64, Float64) CODEC(FirstApplicable(SZ3, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A `Map` nested in a `Variant`, or one that a `Dynamic` or `JSON` column may hold, is protected as well.
SET allow_experimental_variant_type = 1, allow_experimental_dynamic_type = 1, enable_json_type = 1;
CREATE TABLE t_first_applicable_lossy_map (v Variant(Map(Float64, Float64), String) CODEC(FirstApplicable(SZ3, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_lossy_map (d Dynamic CODEC(FirstApplicable(SZ3, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_lossy_map (j JSON CODEC(FirstApplicable(SZ3, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_lossy_map (t Tuple(a Float64, v Variant(Map(Float64, Float64), String)) CODEC(FirstApplicable(SZ3, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_first_applicable_lossy ADD COLUMN v Float64 CODEC(FirstApplicable(SZ3, ZSTD));
INSERT INTO t_first_applicable_lossy SELECT number, number / 7 FROM numbers(1000);
SELECT count(), sum(k) FROM t_first_applicable_lossy;

DROP TABLE t_first_applicable_lossy;

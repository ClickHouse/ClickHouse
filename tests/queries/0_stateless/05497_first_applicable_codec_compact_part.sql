-- Compact parts write the serialization prefixes of `Variant` and `LowCardinality` into separate untyped streams.
-- They must keep only the generic codecs of `FirstApplicable`, as in wide parts, where the prefixes are inside typed streams.
-- So a `FirstApplicable` without a generic alternative, accepted at creation, must also be accepted at insertion.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_first_applicable_variant;
DROP TABLE IF EXISTS t_first_applicable_lc;

CREATE TABLE t_first_applicable_variant
(
    v Variant(Float64, Int64) CODEC(FirstApplicable(FPC, T64))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_first_applicable_variant SELECT toInt64(number) FROM numbers(1000);
INSERT INTO t_first_applicable_variant SELECT toFloat64(number) / 4 FROM numbers(1000);

SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_first_applicable_variant' AND active GROUP BY part_type;
SELECT count(), sum(v.Int64), sum(v.Float64) FROM t_first_applicable_variant;

OPTIMIZE TABLE t_first_applicable_variant FINAL;
SELECT count(), sum(v.Int64), sum(v.Float64) FROM t_first_applicable_variant;

CREATE TABLE t_first_applicable_lc
(
    c LowCardinality(Int64) CODEC(FirstApplicable(FPC, T64))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_first_applicable_lc SELECT number % 10 FROM numbers(1000);
INSERT INTO t_first_applicable_lc SELECT number % 7 FROM numbers(1000);

SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_first_applicable_lc' AND active GROUP BY part_type;
SELECT count(), sum(c), uniqExact(c) FROM t_first_applicable_lc;

OPTIMIZE TABLE t_first_applicable_lc FINAL;
SELECT count(), sum(c), uniqExact(c) FROM t_first_applicable_lc;

DROP TABLE t_first_applicable_variant;
DROP TABLE t_first_applicable_lc;

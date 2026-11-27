-- `force_primary_key` must apply only to the primary key of the table itself, not to the primary
-- keys of its projections. The analysis of a projection candidate whose key is not constrained by
-- the filter must reject the candidate, not fail the query.

DROP TABLE IF EXISTS t_force_pk_proj;

CREATE TABLE t_force_pk_proj
(
    a LowCardinality(String),
    t DateTime,
    c UInt64,
    v UInt64,
    PROJECTION p_index INDEX c TYPE basic,
    PROJECTION p_normal (SELECT a, v ORDER BY v)
)
ENGINE = MergeTree
PARTITION BY toDate(t)
ORDER BY (a, t)
SETTINGS index_granularity = 4;

INSERT INTO t_force_pk_proj SELECT 'x', toDateTime('2026-01-01 00:00:00', 'UTC') + number, number, number FROM numbers(100);

SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET force_primary_key = 1;

-- The filter uses the main table's primary key but no projection key: must succeed.
SELECT count() FROM t_force_pk_proj WHERE a = 'x' AND t >= toDateTime('2026-01-01 00:00:30', 'UTC');

-- The same with the partition-key guardrail enabled on top.
SELECT count() FROM t_force_pk_proj WHERE a = 'x' AND t >= toDateTime('2026-01-01 00:00:30', 'UTC') SETTINGS force_index_by_date = 1;

-- A normal projection candidate that contains all required columns: must succeed as well.
SELECT sum(v) FROM t_force_pk_proj WHERE a = 'x';

-- The filter constrains both the main primary key and the projection index: must succeed.
SELECT count() FROM t_force_pk_proj WHERE a = 'x' AND c = 42 SETTINGS min_table_rows_to_use_projection_index = 0;

-- The guardrail still applies to the main table's primary key.
SELECT count() FROM t_force_pk_proj WHERE c = 42; -- { serverError INDEX_NOT_USED }

-- A forced projection whose key is not constrained is used without the guardrail...
SELECT sum(v) FROM t_force_pk_proj WHERE a = 'x'
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_normal', force_primary_key = 0;

-- ...but under the guardrail it would be a full scan of the projection, so the query throws.
SELECT sum(v) FROM t_force_pk_proj WHERE a = 'x'
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_normal'; -- { serverError PROJECTION_NOT_USED }

-- A forced projection whose key is constrained works under the guardrail.
SELECT sum(v) FROM t_force_pk_proj WHERE a = 'x' AND v >= 30
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_normal';

DROP TABLE t_force_pk_proj;

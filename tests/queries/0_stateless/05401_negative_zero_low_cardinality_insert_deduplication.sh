#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery <<'EOF'
-- The hash of an inserted block identifies it for insert deduplication, so it is persisted and has to be the same
-- as in older versions, which hashed the raw bits of floating point values: `-0.` and `0.` are different blocks.
-- `LowCardinality` columns, also nested ones, must hash the same way as plain ones, so that a retry of an insert
-- written by an older version is still deduplicated.
-- The queries run in `clickhouse-local`: it parses `VALUES` into the `LowCardinality` columns value by value, which
-- keeps the sign of zero. A block that is sent to the server, or cast with `CAST`, is inserted into the dictionary as
-- a range, and that maps `-0.` to the default `0.` of the dictionary, so both inserts would hold the same values.

SET allow_suspicious_low_cardinality_types = 1;
SET insert_deduplicate = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS t_dedup_signed_zero;
CREATE TABLE t_dedup_signed_zero
(
    f Float64,
    lc LowCardinality(Float64),
    lc_nullable LowCardinality(Nullable(Float32)),
    arr Array(LowCardinality(Float64)),
    tup Tuple(LowCardinality(Float64), String)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

-- Only one column holds a zero of a different sign in each pair of inserts, the rest are the same.
INSERT INTO t_dedup_signed_zero VALUES (1, -0., 1, [1], (1, 'a'));
INSERT INTO t_dedup_signed_zero VALUES (1, 0., 1, [1], (1, 'a'));
INSERT INTO t_dedup_signed_zero VALUES (1, 0., 1, [1], (1, 'a'));

INSERT INTO t_dedup_signed_zero VALUES (2, 1, -0., [1], (1, 'a'));
INSERT INTO t_dedup_signed_zero VALUES (2, 1, 0., [1], (1, 'a'));

INSERT INTO t_dedup_signed_zero VALUES (3, 1, 1, [-0.], (1, 'a'));
INSERT INTO t_dedup_signed_zero VALUES (3, 1, 1, [0.], (1, 'a'));

INSERT INTO t_dedup_signed_zero VALUES (4, 1, 1, [1], (-0., 'a'));
INSERT INTO t_dedup_signed_zero VALUES (4, 1, 1, [1], (0., 'a'));

INSERT INTO t_dedup_signed_zero VALUES (-0., 1, 1, [1], (1, 'a'));
INSERT INTO t_dedup_signed_zero VALUES (0., 1, 1, [1], (1, 'a'));

SELECT abs(f) AS a, count() FROM t_dedup_signed_zero GROUP BY a ORDER BY a;
SELECT count() FROM t_dedup_signed_zero;

DROP TABLE t_dedup_signed_zero;
EOF

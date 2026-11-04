#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` keeps zero-width code points: a combining mark, a virama, a joiner take no column of their own
# and are printed with the character before them, so the identifiers are shown unchanged and the boxes still line up.
# The queries are only explained, never executed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The text of a read step carries the database name, which decides the widths of the boxes; see `05404_explain_plan_tree.sh`.
DB="db_$(echo -n "$CLICKHOUSE_DATABASE" | md5sum | cut -c1-8)"
PLACEHOLDER="db_XXXXXXXX"

# `café` with a decomposed `é`: `e` followed by U+0301 COMBINING ACUTE ACCENT, which has display width 0.
TABLE=$'cafe\xcc\x81'
# Devanagari `क्ष`: the virama U+094D between the two consonants has display width 0.
COLUMN='क्ष'

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB"

$CLICKHOUSE_CLIENT --multiquery <<EOF | sed "s/$DB/$PLACEHOLDER/g"
CREATE TABLE $DB.\`$TABLE\`(\`$COLUMN\` UInt64) ENGINE = MergeTree ORDER BY \`$COLUMN\`;
INSERT INTO $DB.\`$TABLE\` SELECT number FROM numbers(100000);

SET max_rows_to_group_by = 0;
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- In ClickHouse Cloud a scan on shared storage is replaced by \`ReadFromMergeTreeAtWorker\`, which prints no table name.
SET distributed_plan_prefer_replicas_over_workers = 1;
SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- the table name and the hash key keep their zero-width code points';
EXPLAIN tree = 1
SELECT \`$COLUMN\` % 10 AS k, count()
FROM $DB.\`$TABLE\`
GROUP BY k;
EOF

# The same output, checked byte for byte: the decomposed table name and the hash key are printed in full.
OUTPUT=$($CLICKHOUSE_CLIENT --multiquery <<EOF
SET max_rows_to_group_by = 0;
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
SET distributed_plan_prefer_replicas_over_workers = 1;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, use_statistics = 0, distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';
EXPLAIN tree = 1 SELECT \`$COLUMN\` % 10 AS k, count() FROM $DB.\`$TABLE\` GROUP BY k;
EOF
)
echo "table name with the combining accent: $(grep -cF "$DB.$TABLE" <<< "$OUTPUT")"
echo "table name without it: $(grep -cF "$DB.cafe)" <<< "$OUTPUT")"
echo "hash key with the virama: $(grep -cF "hash($COLUMN MOD 10)" <<< "$OUTPUT")"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"

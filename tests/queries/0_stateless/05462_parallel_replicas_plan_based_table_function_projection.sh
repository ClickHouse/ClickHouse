#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The plan-based mode ships a read through a `TimeSeries` table function as a read of the inner table, so the
# initiator's own copy announces it under that table's name as well. A projection replaces the read with a new
# one, which must keep that name: otherwise the replicas ask the coordinator for a read it does not know.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Several parts of many small granules, so the replicas get a share each and announce the read before the
# initiator has taken all of it.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
    CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
        SAMPLES INNER ENGINE = AggregatingMergeTree
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, index_granularity = 8, deduplicate_merge_projection_mode = 'rebuild';
"
samples_table=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE \`$samples_table\` ADD PROJECTION normal (SELECT id, bucket, samples ORDER BY samples[1].2);
    ALTER TABLE \`$samples_table\` ADD PROJECTION aggregate (SELECT id, count() GROUP BY id);
"
for i in 0 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO ts (metric_name, tags, samples)
        SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 100))] FROM numbers($((i * 2000)), 2000);
    "
done

settings="enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, enable_analyzer = 1,
    parallel_replicas_plan_based = 1, parallel_replicas_support_projection = 1, parallel_replicas_mark_segment_size = 1,
    max_threads = 1, max_block_size = 8, optimize_use_projections = 1, optimize_use_implicit_projections = 0"

declare -A queries=(
    [normal]="SELECT count(), sum(samples[1].2) FROM timeSeriesSamples(${CLICKHOUSE_DATABASE}.ts) WHERE samples[1].2 < 50"
    [aggregate]="SELECT count(), sum(c) FROM (SELECT id, count() AS c FROM timeSeriesSamples(${CLICKHOUSE_DATABASE}.ts) GROUP BY id)"
)

for projection in normal aggregate; do
    query=${queries[$projection]}
    $CLICKHOUSE_CLIENT -q "$query SETTINGS enable_parallel_replicas = 0"
    for local_plan in 0 1; do
        $CLICKHOUSE_CLIENT -q "$query SETTINGS $settings, parallel_replicas_local_plan = $local_plan"
    done
    # The projection is used, and the read is distributed.
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(explain LIKE '%ReadFromMergeTree ($projection)%') > 0, countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
        FROM (EXPLAIN distributed = 1 $query SETTINGS $settings, parallel_replicas_local_plan = 1)"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"

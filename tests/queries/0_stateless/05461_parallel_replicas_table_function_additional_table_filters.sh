#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A read through a `TimeSeries` table function is sent to the replicas as query text, even with
# `serialize_query_plan`, and a replica matches `additional_table_filters` against its own current
# database, which is not the initiator's. A filter keyed by the short name of the samples table matches
# here and not on the replicas, so such a read must not be distributed: otherwise the replicas return the
# rows the filter excludes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "
    CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
        SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
"
for i in 0 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO ts (metric_name, tags, samples)
        SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers($((i * 10)), 10);
    "
done

samples_table=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
filters="{'$samples_table': 'samples[1].2 < 3'}"

query="SELECT count(), sum(samples[1].2) FROM timeSeriesSamples(${CLICKHOUSE_DATABASE}.ts)"

$CLICKHOUSE_CLIENT --additional_table_filters "$filters" -q "$query SETTINGS enable_parallel_replicas = 0"

for local_plan in 0 1; do
    $CLICKHOUSE_CLIENT --additional_table_filters "$filters" -q "
        $query SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
            parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, enable_analyzer = 1,
            serialize_query_plan = 1, parallel_replicas_local_plan = $local_plan, parallel_replicas_plan_based = 0;
    "
done

# The filters set only for the subquery that reads, so the query around it is the one that gets distributed.
for local_plan in 0 1; do
    $CLICKHOUSE_CLIENT -q "
        SELECT count(), sum(samples[1].2) FROM (
            SELECT * FROM timeSeriesSamples(${CLICKHOUSE_DATABASE}.ts) SETTINGS additional_table_filters = $filters)
        SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
            parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, enable_analyzer = 1,
            serialize_query_plan = 1, parallel_replicas_local_plan = $local_plan, parallel_replicas_plan_based = 0;
    "
done

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"

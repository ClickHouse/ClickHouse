#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: ON CLUSTER is not allowed for a Replicated database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The initiator of an `ON CLUSTER` query authorizes the text index transforms and sends to the hosts
# exactly the expressions it authorized, see also `05482_text_index_transform_on_cluster_binding`.

FUNCTION="f_05497_${CLICKHOUSE_DATABASE}"

function dispatched()
{
    # The entries of this test are told apart by a marker in the name of the index.
    $CLICKHOUSE_CLIENT -q "
        SELECT position(query, 'lower(val)') > 0, position(query, 'upper') > 0, position(query, '$FUNCTION') > 0
        FROM system.distributed_ddl_queue
        WHERE position(query, '${CLICKHOUSE_DATABASE}') > 0 AND position(query, '$1') > 0
        LIMIT 1"
}

$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE tab
    (
        id UInt64,
        val String,
        INDEX idx_kept(val) TYPE text(tokenizer = ngrams(3), preprocessor = upper(val))
    )
    ENGINE = MergeTree ORDER BY id;
"

echo '1. An index kept by IF NOT EXISTS on the initiator is sent as the statement declares it.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        ADD INDEX IF NOT EXISTS idx_kept(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))
"
dispatched 'idx_kept'

echo '2. An ADD INDEX with a transform between commands of different segments is rejected.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        ADD INDEX idx_segments(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)),
        MATERIALIZE COLUMN id,
        DROP INDEX idx_segments
" 2>&1 | grep -o -m1 'QUERY_IS_PROHIBITED'

echo '3. A SQL UDF in a transform is expanded before CREATE ... ON CLUSTER is sent as written.'
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION $FUNCTION AS x -> lower(x)"
$CLICKHOUSE_CLIENT --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost
    (
        val String,
        INDEX idx_udf(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = $FUNCTION(val))
    )
    ENGINE = MergeTree ORDER BY tuple()
"
dispatched 'idx_udf'

echo '4. ADD STATISTICS replaces an implicit statistic in a statement that also adds an index.'
$CLICKHOUSE_CLIENT -n -q "
    SET allow_statistics = 1;
    CREATE TABLE stats (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS auto_statistics_types = 'basic';
    ALTER TABLE stats ADD INDEX idx_minmax(x) TYPE minmax GRANULARITY 1, ADD STATISTICS x TYPE basic;
    SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'stats';
"

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS dst SYNC;
    DROP TABLE tab;
    DROP TABLE stats;
    DROP FUNCTION $FUNCTION;
"

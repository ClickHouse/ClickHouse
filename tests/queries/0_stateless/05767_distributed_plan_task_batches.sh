#!/usr/bin/env bash
# Each case runs a query locally, then distributed with `distributed_plan_task_dispatch_batching` off and on in one dispatch arm:
# all workers at once (`default`), one worker after another (`one_sender`), or batches handed over after every stage
# (`flush_per_stage`). The table shows how the tasks of the run with batching reached the workers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./distributed_plan.lib
. "$CUR_DIR"/distributed_plan.lib

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_task_batches (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_task_batches SELECT number % 1000, number FROM numbers(100000)"

AGGREGATION="SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_task_batches GROUP BY k)"
JOIN="SELECT count(), sum(a.k) FROM t_task_batches AS a INNER JOIN t_task_batches AS b ON a.v = b.v"
IN_SET="SELECT count() FROM t_task_batches WHERE v IN (SELECT number * 2 FROM numbers(50000))"

for kind in Streaming Persisted
do
    for arm in default one_sender flush_per_stage
    do
        case $arm in
            default) dispatch="distributed_plan_task_dispatch_threads = 0, distributed_plan_task_dispatch_stages_per_flush = 0" ;;
            one_sender) dispatch="distributed_plan_task_dispatch_threads = 1, distributed_plan_task_dispatch_stages_per_flush = 0" ;;
            flush_per_stage) dispatch="distributed_plan_task_dispatch_threads = 0, distributed_plan_task_dispatch_stages_per_flush = 1" ;;
        esac
        common="distributed_plan_force_exchange_kind = '$kind', distributed_plan_default_reader_bucket_count = 4,
            distributed_plan_default_shuffle_join_bucket_count = 4, $dispatch"
        run_case "aggregation $kind $arm" "$AGGREGATION" "$common, distributed_plan_force_shuffle_aggregation = 1"
        run_case "join $kind $arm" "$JOIN" "$common, distributed_plan_max_rows_to_broadcast = 0"
        run_case "set $kind $arm" "$IN_SET" "$common"
    done
done

echo "-- Every task is sent and runs (tasks = sent = ran), and both results equal local. Without batching each task has its own"
echo "-- request (batches_without_batching = tasks). With batching and streaming exchanges each plan sends one request per worker"
echo "-- (batches = workers; workers adds up the workers of each plan, and the subquery of query 3 is a plan of its own), more with"
echo "-- flush_per_stage. With persisted exchanges a plan sends one request per worker before each wait for producers and at the end."
echo
echo "-- query 1, a shuffle aggregation: $AGGREGATION"
print_cases aggregation
echo "-- query 2, a shuffle join: $JOIN"
print_cases join
echo "-- query 3, an IN set: $IN_SET"
print_cases set

#!/usr/bin/env bash

QUERIES_FILE=$1
DEBUG_MODE=$2

function random {
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z' | fold -w ${1:-8} | head -n 1
}

while read line; do
    query="$line"
    echo "Query: $query"
    function execute()
    {
        query_id=$(random)
        echo "query id: $query_id"
        result=($(clickhouse-client --time --query_id "$query_id" -q "$query" 2>&1))
        query_result=${result[0]}
        total_time=${result[1]}
        clickhouse-client -q "system flush logs"

        time_executing=$(clickhouse-client -q "select query_duration_ms / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")
        time_reading_from_prefetch=$(clickhouse-client -q "select ProfileEvents['AsynchronousRemoteReadWaitMicroseconds'] / 1000 / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")
        time_reading_without_prefetch=$(clickhouse-client -q "select ProfileEvents['SynchronousRemoteReadWaitMicroseconds'] / 1000 / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")

        function print()
        {
            echo "          $1: $2"
        }

        print "time executing query" $time_executing
        print "time reading data with prefetch" $time_reading_from_prefetch
        print "time reading data without prefetch" $time_reading_without_prefetch

        if (( $DEBUG_MODE == 1 )); then
            remote_profiles=$(clickhouse-client -q "select mapExtractKeyLike(ProfileEvents, '%RemoteFS%') from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            threadpool_profiles=$(clickhouse-client -q "select mapExtractKeyLike(ProfileEvents, '%ThreadpoolReader%') from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            s3_profiles=$(clickhouse-client -q "select mapExtractKeyLike(ProfileEvents, '%S3%') from system.query_log where type='QueryFinish' and query_id = '$query_id'")

            max_parallel_read_tasks=$(clickhouse-client -q "select AsyncReadCounters['max_parallel_read_tasks'] from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            max_parallel_prefetch_tasks=$(clickhouse-client -q "select AsyncReadCounters['max_parallel_prefetch_tasks'] from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            total_prefetch_tasks=$(clickhouse-client -q "select AsyncReadCounters['total_prefetch_tasks'] from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            init=$(clickhouse-client -q "select ProfileEvents['PrefetchedReadBufferInitMS'] / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")
            wait_prefetch_task=$(clickhouse-client -q "select ProfileEvents['WaitPrefetchTaskMicroseconds'] / 1000 / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")

            print "max parallel read tasks" $max_parallel_read_tasks
            print "max parallel prefetch tasks" $max_parallel_prefetch_tasks
            print "total prefetch tasks" $total_prefetch_tasks
            print "init tasks time" $init
            print "wait prefetch task" $wait_prefetch_task
            print "remote reader profile events" $remote_profiles
            print "threadpool profile events" $threadpool_profiles
            print "s3 profile events" $s3_profiles
        fi
    }

    execute
done < $QUERIES_FILE

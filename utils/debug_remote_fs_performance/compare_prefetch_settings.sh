#!/usr/bin/env bash

QUERIES_FILE=$1
DEBUG_MODE=$2

function random {
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z' | fold -w ${1:-8} | head -n 1
}

while read line; do
    query="$line"
    echo "Query: $query"
    for use_prefetched_read_pool in 1 0
    do
        echo "  use_prefetched_read_pool: $use_prefetched_read_pool"

        for max_threads in 8 16 32 64
        do
            echo "    max_threads: $max_threads"

            function execute()
            {
                max_marks_per_part_to_prefetch=$1

                query_id=$(random)
                query_settings="enable_filesystem_cache=0, prefer_prefetched_read_pool=$use_prefetched_read_pool, max_threads=$max_threads, max_marks_per_part_to_prefetch=$max_marks_per_part_to_prefetch"
                query_with_settings="$query"" SETTINGS $query_settings"

                result=($(clickhouse-client --time --query_id "$query_id" -q "$query_with_settings" 2>&1))
                query_result=${result[0]}
                total_time=${result[1]}
                clickhouse-client -q "system flush logs"
                time_reading=$(clickhouse-client -q "select ProfileEvents['AsynchronousReadWaitMicroseconds'] / 1000 / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")
                time_executing=$(clickhouse-client -q "select query_duration_ms / 1000 from system.query_log where type='QueryFinish' and query_id = '$query_id'")

                function print()
                {
                    echo "          $1: $2"
                }

                print "time executing query" $time_executing
                print "time waiting for data" $time_reading

                if (( $DEBUG_MODE == 1 )); then
                    profiles=$(clickhouse-client -q "select mapExtractKeyLike(ProfileEvents, '%RemoteFS%') from system.query_log where type='QueryFinish' and query_id = '$query_id'")
                    read_bytes=$(clickhouse-client -q "select formatReadableSize(ProfileEvents['ThreadpoolReaderReadBytes']) from system.query_log where type='QueryFinish' and query_id = '$query_id'")
                    max_parallel_read_tasks=$(clickhouse-client -q "select AsyncReadCounters['max_parallel_read_tasks'] from system.query_log where type='QueryFinish' and query_id = '$query_id'")
                    max_parallel_prefetch_tasks=$(clickhouse-client -q "select AsyncReadCounters['max_parallel_prefetch_tasks'] from system.query_log where type='QueryFinish' and query_id = '$query_id'")

                    print "read bytes" $read_bytes
                    print "max parallel read tasks" $max_parallel_read_tasks
                    print "max parallel prefetch tasks" $max_parallel_prefetch_tasks
                    print "profile events" $profiles
                fi
            }

            if (( $use_prefetched_read_pool == 1 )); then
                for max_marks_per_part_to_prefetch in 0 50 500
                do
                    echo "      max_marks_per_part_to_prefetch: $max_marks_per_part_to_prefetch"

                    execute $max_marks_per_part_to_prefetch
                done
            else
                execute 0
            fi
        done
    done
done < $QUERIES_FILE

#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# set -e

# All replicas are localhost, disable `prefer_localhost_replica` option to test network interface
# Currently this feature could not work with hedged requests
# Enabling `enable_sample_offset_parallel_processing` feature could lead to intersecting marks, so some of them would be thrown away and it will lead to incorrect result of SELECT query
SETTINGS="--max_parallel_replicas=3 --prefer_localhost_replica=false --use_hedged_requests=false --async_socket_for_remote=false --enable_sample_offset_parallel_processing=false --parallel_reading_from_replicas=false"

# Prepare tables

$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits;
    drop table if exists test.dist_visits;

    create table test.dist_hits as test.hits engine = Distributed('test_cluster_one_shard_three_replicas', test, hits, rand());
    create table test.dist_visits as test.visits engine = Distributed('test_cluster_one_shard_three_replicas', test, visits, rand());
''';


FAILED=()

# PreviouslyFailed=(
#     # "00011_sorting.sql"
#     # "00013_sorting_of_nested.sql"
#     "00014_filtering_arrays.sql"
#     # "00031_array_enumerate_uniq.sql"
#     # "00061_storage_buffer.sql"
#     # "00068_subquery_in_prewhere.sql"
#     # "00075_left_array_join.sql"
#     # "00079_array_join_not_used_joined_column.sql"
# )

# for TESTNAME in "${PreviouslyFailed[@]}"
for TESTNAME in *.sql
do
    echo -n "Testing $TESTNAME ----> "

    # prepare test
    NEW_TESTNAME="/tmp/dist_$TESTNAME"
    cat $TESTNAME | sed -e 's/test.hits/test.dist_hits/'  | sed -e 's/test.visits/test.dist_visits/' > $NEW_TESTNAME

    TESTNAME_RESULT="/tmp/result_$TESTNAME"
    NEW_TESTNAME_RESULT="/tmp/result_dist_$TESTNAME"

    $CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $TESTNAME > $TESTNAME_RESULT
    $CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $NEW_TESTNAME > $NEW_TESTNAME_RESULT

    expected=$(cat $TESTNAME_RESULT < $TESTNAME | md5sum)
    actual=$(cat $NEW_TESTNAME_RESULT | md5sum)

    if [[ $expected != $actual ]]; then
        FAILED+=($TESTNAME)
        echo "Failed! ❌ "
        echo "Plain:"
        cat $TESTNAME_RESULT
        echo "Distributed:"
        cat $NEW_TESTNAME_RESULT
        continue
    fi

    echo "Ok! ✅"
done


echo "Total failed tests: "
# Iterate the loop to read and print each array element
for value in "${FAILED[@]}"
do
    echo "🔺  $value"
done

# Drop tables

$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits;
    drop table if exists test.dist_visits;
''';

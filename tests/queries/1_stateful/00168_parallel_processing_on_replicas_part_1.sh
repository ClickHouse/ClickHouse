#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# All replicas are localhost, disable `prefer_localhost_replica` option to test network interface
# Currently this feature could not work with hedged requests
# Enabling `enable_sample_offset_parallel_processing` feature could lead to intersecting marks, so some of them would be thrown away and it will lead to incorrect result of SELECT query
SETTINGS="--max_parallel_replicas=3 --prefer_localhost_replica=false --use_hedged_requests=false --async_socket_for_remote=false --enable_sample_offset_parallel_processing=false"

# Prepare tables

$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits;
    drop table if exists test.dist_visits;

    create table test.dist_hits as test.hits engine = Distributed('test_cluster_one_shard_three_replicas', test, hits, rand());
    create table test.dist_visits as test.visits engine = Distributed('test_cluster_one_shard_three_replicas', test, visits, rand());
''';


for TESTNAME in *.sql
do
    echo -n "Testing $TESTNAME ----> "

    if [ $TESTNAME = "00011_sorting.sql" ]; then
        echo "🟨🟨🟨"
        continue
    fi

    if [ $TESTNAME = "00061_storage_buffer.sql" ]; then
        echo "Skipped! 🟨🟨🟨"
        continue
    fi

    # prepare test
    NEW_TESTNAME="/tmp/dist_$TESTNAME"
    cat $TESTNAME | sed -e 's/test.hits/test.dist_hits/'  | sed -e 's/test.visits/test.dist_visits/' > $NEW_TESTNAME



    # echo "Original"
    # cat $TESTNAME
    # echo "Modified"
    # cat $NEW_TESTNAME

    expected=$($CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $TESTNAME | md5sum)
    actual=$($CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $NEW_TESTNAME | md5sum)

    if [[ $expected != $actual ]]; then
        echo "Error, results are not equal while processing $TESTNAME"
        exit
    fi

    echo "Ok! ✅"
done


# Drop tables

$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits;
    drop table if exists test.dist_visits;
''';

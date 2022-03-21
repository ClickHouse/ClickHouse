#!/usr/bin/env bash
# Tags: no-tsan, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# set -e

# All replicas are localhost, disable `prefer_localhost_replica` option to test network interface
# Currently this feature could not work with hedged requests
# Enabling `enable_sample_offset_parallel_processing` feature could lead to intersecting marks, so some of them would be thrown away and it will lead to incorrect result of SELECT query
SETTINGS="--max_parallel_replicas=3 --prefer_localhost_replica=false --use_hedged_requests=false --async_socket_for_remote=false  --allow_experimental_parallel_reading_from_replicas=true"

# Prepare tables
$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits SYNC;
    drop table if exists test.dist_visits SYNC;

    create table test.dist_hits as test.hits engine = Distributed("test_cluster_one_shard_three_replicas_localhost", test, hits, rand());
    create table test.dist_visits as test.visits engine = Distributed("test_cluster_one_shard_three_replicas_localhost", test, visits, rand());
''';

FAILED=()

# PreviouslyFailed=(
# )

SkipList=(
    "00013_sorting_of_nested.sql" # It contains FINAL, which is not allowed together with parallel reading

    "00061_storage_buffer.sql"
    "00095_hyperscan_profiler.sql" # too long in debug (there is a --no-debug tag inside a test)

    "00140_rename.sql" # Multiple renames are not allowed with DatabaseReplicated and tags are not forwarded through this test

    "00154_avro.sql" # Plain select * with limit with Distributed table is not deterministic
    "00151_replace_partition_with_different_granularity.sql" # Replace partition from Distributed is not allowed
    "00152_insert_different_granularity.sql" # The same as above

    "00157_cache_dictionary.sql" # Too long in debug mode, but result is correct
    "00158_cache_dictionary_has.sql" # The same as above

    "00166_explain_estimate.sql" # Distributed table returns nothing
)

# for TESTPATH in "${PreviouslyFailed[@]}"
for TESTPATH in "$CURDIR"/*.sql;
do
    TESTNAME=$(basename $TESTPATH)
    NUM=$(echo "${TESTNAME}" | grep -o -P '^\d+' | sed 's/^0*//')
    if [[ "${NUM}" -ge 168 ]]; then
        continue
    fi

    if [[ " ${SkipList[*]} " =~ ${TESTNAME} ]]; then
        echo  "Skipping $TESTNAME "
        continue
    fi

    echo -n "Testing $TESTNAME ----> "

    # prepare test
    NEW_TESTNAME="/tmp/dist_$TESTNAME"
    # Added g to sed command to replace all tables, not the first
    cat $TESTPATH | sed -e 's/test.hits/test.dist_hits/g'  | sed -e 's/test.visits/test.dist_visits/g' > $NEW_TESTNAME

    TESTNAME_RESULT="/tmp/result_$TESTNAME"
    NEW_TESTNAME_RESULT="/tmp/result_dist_$TESTNAME"

    $CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $TESTPATH > $TESTNAME_RESULT
    $CLICKHOUSE_CLIENT $SETTINGS -nm --testmode < $NEW_TESTNAME > $NEW_TESTNAME_RESULT

    expected=$(cat $TESTNAME_RESULT | md5sum)
    actual=$(cat $NEW_TESTNAME_RESULT | md5sum)

    if [[ "$expected" != "$actual" ]]; then
        FAILED+=("$TESTNAME")
        echo "Failed! ❌"
        echo "Plain:"
        cat $TESTNAME_RESULT
        echo "Distributed:"
        cat $NEW_TESTNAME_RESULT
    else
        echo "Ok! ✅"
    fi
done


echo "Total failed tests: "
# Iterate the loop to read and print each array element
for value in "${FAILED[@]}"
do
    echo "🔺  $value"
done

# Drop tables

$CLICKHOUSE_CLIENT $SETTINGS -nm -q '''
    drop table if exists test.dist_hits SYNC;
    drop table if exists test.dist_visits SYNC;
''';

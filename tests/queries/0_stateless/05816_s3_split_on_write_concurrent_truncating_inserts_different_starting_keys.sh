#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A truncating insert that is not split starts from the last key of the table that is not a part of a split tail,
# here `data.1.tsv`, written by an insert with `s3_create_new_file_on_insert`. A truncating insert that is split
# starts from the raw key `data.tsv`. They must not run at the same time either: the split one would step over
# `data.1.tsv` in its cleanup while the other one is writing it, and the table would read the rows of both rewrites.

PREFIX="05816_split_concurrent_truncating_inserts/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV) SELECT 0 SETTINGS s3_truncate_on_insert = 1;
    CREATE TABLE test_05816 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);
    INSERT INTO test_05816 SELECT 1 SETTINGS s3_create_new_file_on_insert = 1;
"

QUERY_ID="${CLICKHOUSE_DATABASE}_05816_slow_truncating_insert"

# This insert is not split, so it overwrites `data.1.tsv`. Every row takes 50 ms, so it lasts about five seconds.
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "
    INSERT INTO test_05816 SELECT number + 1000000 FROM numbers(100) WHERE NOT sleepEachRow(0.05)
    SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 10, s3_truncate_on_insert = 1;
" &

echo '--- The first truncating insert is running'
for _ in $(seq 1 300)
do
    running=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID' AND read_rows > 0")
    if [ "$running" = "1" ]
    then
        break
    fi
    sleep 0.1
done
echo "$running"

echo '--- The truncating insert that is split is refused while the first one is running'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_05816 SELECT number + 2000000 FROM numbers(200)
    SETTINGS max_block_size = 20, min_insert_block_size_rows = 20, min_insert_block_size_bytes = 0,
        s3_split_on_write_by_size_bytes = 40, s3_truncate_on_insert = 1;
" 2>&1 | grep -c -m1 'is being written by a concurrent insert into the table'

wait

echo '--- Only the rows of the first insert are there, along with the base object it keeps'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64');
"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM test_05816"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_05816"

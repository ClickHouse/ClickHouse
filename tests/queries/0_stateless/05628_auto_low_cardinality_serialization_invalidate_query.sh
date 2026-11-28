#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `INVALIDATE_QUERY` of a dictionary may read a `MergeTree` column stored with automatic `LowCardinality`
# serialization. Its result is serialized with the declared type `String`, so it must be materialized first,
# like the blocks of a dictionary load. The periodic invalidation must notice a changed value and reload.

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_statistics 1 --materialize_statistics_on_insert 1"

${CLIENT} --multiquery "
    CREATE TABLE inv_src (k UInt64, s String STATISTICS(uniq))
    ENGINE = MergeTree ORDER BY k
    SETTINGS max_uniq_number_for_low_cardinality = 1000, ratio_of_defaults_for_sparse_serialization = 1,
        min_bytes_for_wide_part = 0, auto_statistics_types = '';
    INSERT INTO inv_src VALUES (1, 'version1');

    SELECT column, serialization_kind FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'inv_src' AND active AND column = 's';

    CREATE TABLE dict_src (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dict_src VALUES (1, 'first');

    CREATE DICTIONARY inv_dict (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '${CLICKHOUSE_DATABASE}'
        INVALIDATE_QUERY 'SELECT s FROM ${CLICKHOUSE_DATABASE}.inv_src LIMIT 1'))
    LAYOUT(FLAT())
    LIFETIME(MIN 1 MAX 1);

    SELECT dictGet('inv_dict', 'value', toUInt64(1));

    TRUNCATE TABLE dict_src;
    INSERT INTO dict_src VALUES (1, 'second');
    TRUNCATE TABLE inv_src;
    INSERT INTO inv_src VALUES (1, 'version2');

    SELECT column, serialization_kind FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'inv_src' AND active AND column = 's';
"

# The periodic check runs the invalidation query, sees the new value and reloads the dictionary.
for _ in {1..600}
do
    value=$(${CLICKHOUSE_CLIENT} --query "SELECT dictGet('inv_dict', 'value', toUInt64(1))")
    [ "$value" = "second" ] && break
    sleep 0.1
done
echo "$value"

${CLICKHOUSE_CLIENT} --query "SELECT status, last_exception FROM system.dictionaries WHERE database = currentDatabase() AND name = 'inv_dict'"

${CLICKHOUSE_CLIENT} --multiquery "
    DROP DICTIONARY inv_dict;
    DROP TABLE dict_src;
    DROP TABLE inv_src;
"

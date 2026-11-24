#!/usr/bin/env bash
# Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
# `PartsSplitter` fault injection reads the granule on each layer border twice, which changes `SelectedRows`.
# A query without `FROM` reads only `system.one`, which is exempt from quotas, but its scalar subquery is not:
# what the subquery consumes is charged to the quotas over profile events, with and without the analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05508_${CLICKHOUSE_DATABASE}"
Q="quota_05508_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05508"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05508 (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05508 SELECT number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05508 TO ${U}"

QUERY="SELECT (SELECT sum(x) FROM ${CLICKHOUSE_DATABASE}.t_05508)"

for analyzer in 1 0
do
    echo "-- enable_analyzer = ${analyzer}"
    # A new quota starts with zero usage.
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectedRows = 1, SelectedParts = 1 TO ${U}"
    # The first query is allowed and is charged with what its scalar subquery reads (1000 rows of `t_05508`), and with the
    # row of `system.one` it reads itself.
    ${CLICKHOUSE_CLIENT} --user "${U}" --enable_analyzer "${analyzer}" -q "${QUERY}"
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectedRows'], profile_events['SelectedParts'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
    # The quota is exhausted, so the following one is rejected.
    ${CLICKHOUSE_CLIENT} --user "${U}" --enable_analyzer "${analyzer}" -q "${QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
done

cleanup

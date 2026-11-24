#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `database_attach_table_fail_after_removing_detached_permanently_flag` failpoint is
# process-global and would make the short `ATTACH TABLE` of any concurrently running test fail.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, but the test cluster executes
# the `ATTACH TABLE` on all replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A permanently detached table records no named collection dependency: it is not loaded at startup.
# A short `ATTACH TABLE` removes the permanent-detach flag before the table is published, so if it
# fails in between, the table stays detached while its metadata is loaded at the next start again.
# The dependency registered by the failed query must then be kept as the one of a detached table,
# and `DROP NAMED COLLECTION` must be refused.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:11111/test.csv', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t PERMANENTLY;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_attach_table_fail_after_removing_detached_permanently_flag"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_attach_table_fail_after_removing_detached_permanently_flag"

${CLICKHOUSE_CLIENT} -q "EXISTS TABLE t"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE t;
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

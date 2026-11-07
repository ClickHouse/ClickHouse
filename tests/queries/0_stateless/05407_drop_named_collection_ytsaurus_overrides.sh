#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the named collection is created on one replica only, so the other replicas cannot create the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ytsaurus` with a named collection and two overrides (four arguments) must register the collection,
# like the two-argument form does, so that `DROP NAMED COLLECTION` is refused while a table uses it.
# Creating the table does not connect to YTsaurus.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_ytsaurus_table_function 1 -m -q "
CREATE NAMED COLLECTION ${NC} AS http_proxy_urls = 'http://localhost:8000', cypress_path = '//tmp/t', oauth_token = 'token';
CREATE TABLE t AS ytsaurus(${NC}, cypress_path = '//tmp/t', oauth_token = 'token', 'a UInt8');
"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

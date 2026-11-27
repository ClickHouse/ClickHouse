#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: rewrite rules are global server state

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client decides from the submitted query whether to upload the rows of an INSERT. A rewrite
# of a `SELECT` into an INSERT whose rows come from the client (`FROM INFILE`)
# would make the server wait for rows the client never sends, so it must be rejected before
# execution. A rewrite into `INSERT ... SELECT`, which obtains its own rows, keeps working.

RULE="rule_client_upload_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS rr_dst;
CREATE TABLE rr_dst (x UInt32) ENGINE = Memory;
CREATE RULE ${RULE}_infile AS (SELECT 9009009) REWRITE TO (INSERT INTO ${CLICKHOUSE_DATABASE}.rr_dst FROM INFILE 'unused');
CREATE RULE ${RULE}_select AS (SELECT 9009011) REWRITE TO (INSERT INTO ${CLICKHOUSE_DATABASE}.rr_dst SELECT 9009011);
"

echo "native, rewrite into INSERT FROM INFILE:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}_infile" --query "SELECT 9009009" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
echo "http, rewrite into INSERT FROM INFILE:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_rules=${RULE}_infile" -d "SELECT 9009009" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
echo "native, rewrite into INSERT ... SELECT:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}_select" --query "SELECT 9009011"
$CLICKHOUSE_CLIENT --query "SELECT * FROM rr_dst"

$CLICKHOUSE_CLIENT --query "
DROP RULE ${RULE}_infile;
DROP RULE ${RULE}_select;
DROP TABLE rr_dst;
"

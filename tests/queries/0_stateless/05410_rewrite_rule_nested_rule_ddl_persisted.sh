#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CREATE RULE` / `ALTER RULE` produced by another rule's result template must be persisted
# without the enclosing `)`, so that the rule storage can be read back by the next session.
# Uses `clickhouse local` with a private path to start a fresh process on the same storage.

WORKDIR="${CLICKHOUSE_TMP}/05410_rewrite_rule_nested_rule_ddl_persisted_${CLICKHOUSE_DATABASE}"
rm -rf "${WORKDIR}"
mkdir -p "${WORKDIR}"

${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "
CREATE RULE outer_create AS (SELECT 1) REWRITE TO (CREATE RULE inner_rule AS (SELECT 2) REWRITE TO (SELECT 3));
CREATE RULE outer_alter AS (SELECT 11) REWRITE TO (ALTER RULE inner_rule AS (SELECT 2) REWRITE TO (SELECT 4));
SET query_rules = 'outer_create';
SELECT 1;
"

# A fresh process loads the storage.
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "
SELECT name FROM system.query_rules ORDER BY name;
SET query_rules = 'inner_rule';
SELECT 2;
SET query_rules = 'outer_alter';
SELECT 11;
"

${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "
SET query_rules = 'inner_rule';
SELECT 2;
"

rm -rf "${WORKDIR}"

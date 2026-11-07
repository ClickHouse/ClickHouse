#!/usr/bin/env bash
# The shutdown of `clickhouse-local` flushes pending `Buffer` rows: an `Alias` whose target was dropped must not stop it,
# and a synchronous `QueryRunner` destination must still run the buffered queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

LOCAL="${CLICKHOUSE_LOCAL} --path ${WORKING_DIR}"

$LOCAL -q "
CREATE DATABASE db;
CREATE TABLE db.dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db.buf (x UInt64) ENGINE = Buffer(db, dst, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE db.target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db.alias ENGINE = Alias(db, target);
DROP TABLE db.target;
INSERT INTO db.buf SELECT number FROM numbers(3);

CREATE TABLE db.result (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db.runner (query String, database String) ENGINE = QueryRunner SETTINGS mode = 'synchronous' SQL SECURITY NONE;
CREATE TABLE db.runner_buf (query String, database String) ENGINE = Buffer(db, runner, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO db.runner_buf SELECT 'INSERT INTO db.result VALUES (1)', 'db';
"

$LOCAL -q "SELECT count() FROM db.dst; SELECT count() FROM db.result"

rm -rf "${WORKING_DIR}"

#!/usr/bin/env bash
# The Mongo dialect:
# - checks the fields of a `$unionWith` of temporary tables against those tables, not against the
#   tables of the current database;
# - rejects a field name that contains a zero byte, instead of cutting it at that byte;
# - returns `null` from `$toLong` of `null`, also with short-circuit evaluation disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --mutations_sync 2 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_zero_names;
    DROP TABLE IF EXISTS mongo_to_long;
    CREATE TABLE mongo_zero_names (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_zero_names VALUES (1, 1), (2, 2);
    CREATE TABLE mongo_to_long (id Int32, n Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_to_long VALUES (1, NULL), (2, 5);
"

echo '-- $unionWith of temporary tables with the same fields in a different order'
# The union is rejected rather than relabeling the fields of the second table. Only the name of the
# error is kept: the first document may or may not be sent before it. With parallel replicas,
# reading a temporary table logs a warning that the other replicas do not have it, so only errors
# are sent to the client.
${CLICKHOUSE_CLIENT} --send_logs_level=error --format JSONEachRow --query "
    CREATE TEMPORARY TABLE mongo_tmp_a (x Int32, y Int32);
    CREATE TEMPORARY TABLE mongo_tmp_b (y Int32, x Int32);
    INSERT INTO mongo_tmp_a VALUES (1, 2);
    INSERT INTO mongo_tmp_b VALUES (3, 4);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_tmp_a.aggregate([{\"\$unionWith\" : \"mongo_tmp_b\"}]);
" 2>&1 | grep -oE '\([A-Z_]+\)$' | head -1

echo '-- $unionWith of temporary tables with the same fields in the same order'
${CLICKHOUSE_CLIENT} --send_logs_level=error --format JSONEachRow --query "
    CREATE TEMPORARY TABLE mongo_tmp_a (x Int32, y Int32);
    CREATE TEMPORARY TABLE mongo_tmp_b (x Int32, y Int32);
    INSERT INTO mongo_tmp_a VALUES (1, 2);
    INSERT INTO mongo_tmp_b VALUES (3, 4);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_tmp_a.aggregate([{\"\$unionWith\" : \"mongo_tmp_b\"}, {\"\$sort\" : {\"x\" : 1}}]);
"

echo '-- a field name with a zero byte'
err --query 'db.mongo_zero_names.deleteMany({"x\u0000ignored" : 1});'
err --query 'db.mongo_zero_names.find({"x\u0000ignored" : 1});'
err --query 'db.mongo_zero_names.find({}).sort({"x\u0000ignored" : 1});'
err --query 'db.mongo_zero_names.updateMany({"x\u0000ignored" : 1}, {"$set" : {"x" : 10}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM mongo_zero_names ORDER BY id"

echo '-- $toLong of null'
for short_circuit in enable disable; do
    mongo --short_circuit_function_evaluation "$short_circuit" --query 'db.mongo_to_long.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "a" : {"$toLong" : null}, "b" : {"$toLong" : "$n"}}}]);'
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_zero_names;
    DROP TABLE mongo_to_long;
"

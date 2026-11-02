#!/usr/bin/env bash
# The Mongo dialect:
# - keeps a string, a number and a date bound of a comparison to the fields of its own type category,
#   so that `{"x": {"$gt": "0"}}` does not select a numeric `x`, also in a delete;
# - rejects a numeric `$numberInt` outside of the range of a 32-bit integer;
# - reads the type of the value of a `Dynamic` field in the numeric accumulators and in the truth value;
# - rejects a `$unionWith` that would turn the booleans of one side into the numbers of the other.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    DROP TABLE IF EXISTS mongo_dynamic;
    DROP TABLE IF EXISTS mongo_booleans;
    DROP TABLE IF EXISTS mongo_numbers;
    CREATE TABLE mongo_t (id Int32, x Int32, s String, d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 1, '5', '2020-01-01 00:00:00'), (2, 0, 'a', '2021-01-01 00:00:00');
    CREATE TABLE mongo_dynamic (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dynamic VALUES (1, NULL), (2, 2), (3, 0), (4, false), (5, 'x'), (6, true), (7, 1.5);
    CREATE TABLE mongo_booleans (v Bool) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE mongo_numbers (v Int32) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO mongo_booleans VALUES (true);
    INSERT INTO mongo_numbers VALUES (1);
"

echo '-- type brackets'
mongo --query 'db.mongo_t.find({"x" : {"$gt" : "0"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$gte" : "abc"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$not" : {"$gt" : "0"}}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$gt" : 0}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"s" : {"$gt" : "4"}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"d" : {"$gt" : 0}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"d" : {"$gt" : "2020-06-01"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"d" : {"$gt" : {"$date" : "2020-06-01T00:00:00Z"}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.deleteMany({"x" : {"$gt" : "0"}});'
mongo --query 'db.mongo_t.deleteMany({"d" : {"$lt" : 1}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM mongo_t ORDER BY id"

echo '-- $numberInt'
err --query 'db.mongo_t.find({"x" : {"$numberInt" : 2147483648}});'
err --query 'db.mongo_t.find({"x" : {"$numberInt" : -2147483649}});'
err --query 'db.mongo_t.find({"x" : {"$numberInt" : "4294967297"}});'
err --query 'db.mongo_t.deleteMany({"x" : {"$numberInt" : 4294967297}});'
mongo --query 'db.mongo_t.find({"x" : {"$numberInt" : 1}}, {"_id" : 0, "id" : 1});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t"

echo '-- Dynamic'
mongo --query 'db.mongo_dynamic.aggregate([{"$group" : {"_id" : null, "s" : {"$sum" : "$v"}, "a" : {"$avg" : "$v"}, "p" : {"$stdDevPop" : "$v"}, "n" : {"$stdDevSamp" : "$v"}}}]);'
mongo --query 'db.mongo_dynamic.aggregate([{"$match" : {"id" : {"$in" : [1, 5]}}}, {"$group" : {"_id" : null, "s" : {"$sum" : "$v"}, "a" : {"$avg" : "$v"}}}]);'
mongo --query 'db.mongo_dynamic.aggregate([{"$project" : {"_id" : 0, "id" : 1, "t" : {"$toBool" : "$v"}}}, {"$sort" : {"id" : 1}}]);'
mongo --query 'db.mongo_dynamic.find({"$expr" : "$v"}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_dynamic.deleteMany({"$expr" : "$v"});'
${CLICKHOUSE_CLIENT} --query "SELECT id FROM mongo_dynamic ORDER BY id"

echo '-- $unionWith'
err --query 'db.mongo_booleans.aggregate([{"$unionWith" : "mongo_numbers"}]);'
err --query 'db.mongo_booleans.aggregate([{"$unionWith" : "mongo_numbers"}, {"$group" : {"_id" : null, "s" : {"$sum" : "$v"}}}]);'
err --query 'db.mongo_booleans.aggregate([{"$group" : {"_id" : "$v"}}, {"$unionWith" : {"coll" : "mongo_numbers", "pipeline" : [{"$group" : {"_id" : "$v"}}]}}]);'
mongo --query 'db.mongo_numbers.aggregate([{"$group" : {"_id" : "$v"}}, {"$unionWith" : {"coll" : "mongo_numbers", "pipeline" : [{"$group" : {"_id" : "$v"}}]}}, {"$group" : {"_id" : null, "s" : {"$sum" : "$_id"}}}]);'
mongo --query 'db.mongo_booleans.aggregate([{"$group" : {"_id" : "$v"}}, {"$unionWith" : {"coll" : "mongo_booleans", "pipeline" : [{"$group" : {"_id" : "$v"}}]}}, {"$group" : {"_id" : null, "n" : {"$sum" : 1}}}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_t;
    DROP TABLE mongo_dynamic;
    DROP TABLE mongo_booleans;
    DROP TABLE mongo_numbers;
"

#!/usr/bin/env bash
# The Mongo dialect:
# - keeps booleans and numbers apart in filters, as BSON type bracketing does, although ClickHouse
#   stores a `Bool` as a number: `{"active": 1}` does not select or delete `{"active": true}`;
# - rounds the ties of `$round` to even;
# - checks the values of `insertOne` and `insertMany` against the types of their columns and writes
#   nothing when one does not fit;
# - compares the columns of the two collections of a `$unionWith` the way `*` expands them, including
#   `MATERIALIZED` and `ALIAS` columns when the settings include them.

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
    DROP TABLE IF EXISTS mongo_flags;
    CREATE TABLE mongo_flags (id Int32, active Bool, n Int32, flags Array(Bool), maybe Nullable(Bool)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_flags VALUES (1, true, 1, [true], true), (2, false, 0, [false], NULL), (3, true, 2, [], false);
"

ids() {
    mongo --query "db.mongo_flags.find($1, {\"_id\" : 0, \"id\" : 1}).sort({\"id\" : 1});" | tr -d '{}"id:' | tr '\n' ' '
    echo
}

echo '-- type bracketing'
ids '{"active" : 1}'
ids '{"active" : true}'
ids '{"active" : {"$eq" : 0}}'
ids '{"active" : {"$ne" : 1}}'
ids '{"active" : {"$gt" : 0}}'
ids '{"active" : {"$in" : [true, 0]}}'
ids '{"active" : {"$nin" : [1]}}'
ids '{"n" : true}'
ids '{"n" : {"$numberInt" : "1"}}'
ids '{"n" : {"$in" : [true, 2]}}'
ids '{"n" : {"$lt" : true}}'
ids '{"flags" : 1}'
ids '{"flags" : true}'
ids '{"maybe" : 0}'
ids '{"maybe" : false}'
mongo --mutations_sync 2 --query 'db.mongo_flags.deleteMany({"active" : 1});'
mongo --mutations_sync 2 --query 'db.mongo_flags.updateMany({"n" : true}, {"$set" : {"n" : 100}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, n FROM mongo_flags ORDER BY id"

echo '-- $round'
mongo --query 'db.mongo_flags.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "a" : {"$round" : [45, -1]}, "b" : {"$round" : [55, -1]}, "c" : {"$round" : [{"$numberDecimal" : "2.5"}, 0]}, "d" : {"$round" : [3.5]}, "e" : {"$round" : [-2.5]}}}]);'
ids '{"$expr" : {"$eq" : [{"$round" : [{"$add" : ["$n", 0.5]}]}, 0]}}'

echo '-- checked inserts'
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_typed;
    CREATE TABLE mongo_typed (id Int32, x Int32, dec Decimal(38, 1), arr Array(Int32), f Float32) ENGINE = MergeTree ORDER BY id;
"
err --query 'db.mongo_typed.insertOne({"id" : 1, "x" : 2147483648});'
err --query 'db.mongo_typed.insertOne({"id" : 1, "x" : 1.5});'
err --query 'db.mongo_typed.insertOne({"id" : 1, "dec" : {"$numberDecimal" : "1.25"}});'
err --query 'db.mongo_typed.insertOne({"id" : 1, "dec" : 1.25});'
err --query 'db.mongo_typed.insertMany([{"id" : 1, "arr" : [1]}, {"id" : 2, "arr" : [2147483648]}]);'
err --query 'db.mongo_typed.insertOne({"id" : 1, "nope" : 1});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_typed"
mongo --query 'db.mongo_typed.insertMany([{"id" : 1, "x" : 2147483647, "dec" : {"$numberDecimal" : "1.20"}, "arr" : [1, -2147483648], "f" : 0.1}, {"id" : 2, "x" : {"$numberDecimal" : "3"}, "dec" : 2.5, "arr" : [], "f" : 1}]);'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM mongo_typed ORDER BY id"

echo '-- $unionWith with computed columns'
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_union_ma;
    DROP TABLE IF EXISTS mongo_union_mb;
    DROP TABLE IF EXISTS mongo_union_aa;
    DROP TABLE IF EXISTS mongo_union_ab;
    CREATE TABLE mongo_union_ma (x Int32, ma Int32 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE mongo_union_mb (x Int32, mb Int32 MATERIALIZED x + 2) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE mongo_union_aa (x Int32, aa Int32 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE mongo_union_ab (x Int32, ab Int32 ALIAS x + 2) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mongo_union_ma VALUES (1);
    INSERT INTO mongo_union_mb VALUES (2);
    INSERT INTO mongo_union_aa VALUES (1);
    INSERT INTO mongo_union_ab VALUES (2);
"
mongo --query 'db.mongo_union_ma.aggregate([{"$unionWith" : "mongo_union_mb"}, {"$sort" : {"x" : 1}}]);'
err --asterisk_include_materialized_columns 1 --query 'db.mongo_union_ma.aggregate([{"$unionWith" : "mongo_union_mb"}]);'
mongo --query 'db.mongo_union_aa.aggregate([{"$unionWith" : "mongo_union_ab"}, {"$sort" : {"x" : 1}}]);'
err --asterisk_include_alias_columns 1 --query 'db.mongo_union_aa.aggregate([{"$unionWith" : "mongo_union_ab"}]);'
mongo --asterisk_include_materialized_columns 1 --query 'db.mongo_union_ma.aggregate([{"$unionWith" : "mongo_union_ma"}, {"$sort" : {"x" : 1}}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_flags;
    DROP TABLE mongo_typed;
    DROP TABLE mongo_union_ma;
    DROP TABLE mongo_union_mb;
    DROP TABLE mongo_union_aa;
    DROP TABLE mongo_union_ab;
"

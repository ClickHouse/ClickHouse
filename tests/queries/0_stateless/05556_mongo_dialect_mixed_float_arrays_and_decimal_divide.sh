#!/usr/bin/env bash
# The Mongo dialect:
# - checks the integers of an array mixed with doubles against a `Float32` element type, which does
#   not hold `16777217` exactly, in inserts and updates;
# - keeps the fraction of `$divide` when an operand is a decimal, also in a `$expr` filter.

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
    DROP TABLE IF EXISTS mongo_mixed;
    CREATE TABLE mongo_mixed (id Int32, a Array(Float32), n Array(Array(Float32)), d Decimal(38, 0), x Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_mixed VALUES (1, [1], [[1]], 5, 5), (2, [2], [[2]], 7, 2);
"

echo '-- an integer next to a double in an array of Float32'
err --query 'db.mongo_mixed.insertOne({"id" : 3, "a" : [16777217, 0.1]});'
err --query 'db.mongo_mixed.insertOne({"id" : 3, "n" : [[16777217], [0.1]]});'
err --query 'db.mongo_mixed.updateMany({}, {"$set" : {"a" : [16777217, 0.1]}});'
err --query 'db.mongo_mixed.updateMany({}, {"$push" : {"a" : {"$each" : [16777217, 0.1]}}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mongo_mixed'"
mongo --query 'db.mongo_mixed.insertOne({"id" : 4, "a" : [16777216, 0.1], "n" : [[16777216, 0.5]]});'
mongo --query 'db.mongo_mixed.updateMany({"id" : 4}, {"$set" : {"a" : [3, 0.25]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, a, n FROM mongo_mixed ORDER BY id"

echo '-- $divide of decimals'
mongo --query 'db.mongo_mixed.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "q" : {"$divide" : [{"$numberDecimal" : "5"}, 2]}, "r" : {"$divide" : [5, {"$numberDecimal" : "2"}]}, "c" : {"$divide" : ["$d", 2]}, "i" : {"$divide" : ["$x", 2]}}}]);'
mongo --query 'db.mongo_mixed.find({"$expr" : {"$eq" : [{"$divide" : ["$d", 2]}, 3.5]}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_mixed.deleteMany({"$expr" : {"$eq" : [{"$divide" : ["$d", 2]}, 2.5]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id FROM mongo_mixed ORDER BY id"
err --query 'db.mongo_mixed.aggregate([{"$project" : {"_id" : 0, "q" : {"$divide" : [{"$numberDecimal" : "5"}, 0]}}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_mixed"

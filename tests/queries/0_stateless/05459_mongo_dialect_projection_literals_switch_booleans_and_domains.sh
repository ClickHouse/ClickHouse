#!/usr/bin/env bash
# The Mongo dialect:
# - computes a field of a `find` projection with an aggregation expression: a string is a literal,
#   `"$a"` refers to the field `a`, and the integer arithmetic is checked for overflow;
# - fails a `$switch` without a `default` only for the documents no branch matches, also with
#   `short_circuit_function_evaluation = 'disable'`, and does not copy its branches, so nesting it
#   grows the query linearly;
# - skips booleans in `$sum`, `$avg`, `$stdDevPop` and `$stdDevSamp`, as Mongo does;
# - rejects a division by zero and the square root of a negative number;
# - rejects a number other than 0 and 1 for a `Bool` column in `insertOne`, `insertMany` and
#   `updateMany`, instead of storing `true`.

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
    CREATE TABLE mongo_t (id Int32, a Int32, v Int64, active Bool, flags Array(Bool)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 7, -9223372036854775808, true, [true]), (2, 0, 5, false, [false]);
"

echo '-- find projections'
mongo --query 'db.mongo_t.find({"id" : 1}, {"_id" : 0, "label" : "a", "ref" : "$a", "total" : {"$add" : ["$a", 1]}, "lit" : {"$literal" : "$a"}});'
mongo --query 'db.mongo_t.find({"id" : 1}, {"_id" : 0, "id" : 1, "x" : {"$multiply" : ["$a", 2]}});'
err --query 'db.mongo_t.find({}, {"x" : {"$add" : [-9223372036854775808, -1]}});'
err --query 'db.mongo_t.find({"id" : 1}, {"x" : {"$add" : ["$v", -1]}});'
mongo --query 'db.mongo_t.find({"id" : 2}, {"_id" : 0, "x" : {"$add" : ["$v", -1]}});'

echo '-- $switch without a default'
mongo --short_circuit_function_evaluation disable --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$gt" : ["$id", 0]}, "then" : "matched"}]}}}}]);'
err --short_circuit_function_evaluation disable --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$id", 1]}, "then" : "one"}]}}}}]);'
mongo --short_circuit_function_evaluation disable --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$id", 1]}, "then" : "one"}, {"case" : {"$eq" : ["$id", 2]}, "then" : null}]}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 3}}, {"$project" : {"_id" : 0, "r" : {"$switch" : {"branches" : [{"case" : false, "then" : 1}]}}}}]);'

# Each level of a translation that copied its branch would double the query.
nested='"$id"'
for _ in $(seq 8); do
    nested="{\"\$switch\" : {\"branches\" : [{\"case\" : true, \"then\" : $nested}]}}"
done
mongo --max_ast_elements 1000 --query "db.mongo_t.aggregate([{\"\$sort\" : {\"id\" : 1}}, {\"\$project\" : {\"_id\" : 0, \"r\" : $nested}}]);"

echo '-- booleans in numeric accumulators'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "s" : {"$sum" : "$active"}, "a" : {"$avg" : "$active"}, "p" : {"$stdDevPop" : "$active"}, "q" : {"$stdDevSamp" : "$active"}, "n" : {"$sum" : "$id"}, "m" : {"$avg" : "$id"}, "c" : {"$sum" : 1}}}]);'

echo '-- domains of $divide and $sqrt'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$divide" : [1, 0]}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$divide" : ["$id", "$a"]}}}]);'
err --query 'db.mongo_t.find({"$expr" : {"$gt" : [{"$divide" : ["$id", "$a"]}, 0]}});'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$sqrt" : -1}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"_id" : 0, "r" : {"$sqrt" : {"$subtract" : [0, "$id"]}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "d" : {"$divide" : ["$a", 2]}, "r" : {"$sqrt" : 16}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 3}}, {"$project" : {"_id" : 0, "r" : {"$divide" : [1, 0]}}}]);'

echo '-- integers into a Bool column'
err --query 'db.mongo_t.insertOne({"id" : 3, "active" : 2});'
err --query 'db.mongo_t.insertMany([{"id" : 3, "active" : 1}, {"id" : 4, "active" : -1}]);'
err --query 'db.mongo_t.insertOne({"id" : 3, "flags" : [1, 2]});'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$set" : {"active" : 2}});'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 1}, {"$push" : {"flags" : 2}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mongo_t'"
mongo --query 'db.mongo_t.insertMany([{"id" : 3, "active" : 1, "flags" : [0, 1]}, {"id" : 4, "active" : true, "flags" : []}]);' > /dev/null
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 2}, {"$set" : {"active" : 1}});' > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT id, active, flags FROM mongo_t ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"

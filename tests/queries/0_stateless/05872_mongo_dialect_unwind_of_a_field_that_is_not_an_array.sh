#!/usr/bin/env bash
# The Mongo dialect unwinds a field that is not an array into itself, as Mongo does:
# - the document is kept once, with the value and the type of the field unchanged and a null index;
# - a document whose field is null is dropped, unless `preserveNullAndEmptyArrays` keeps it;
# - an array is still unwound into its elements, also an array of arrays;
# - a `Dynamic` field, which holds an array in one row and a value in another, is still rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --enable_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_u;
    CREATE TABLE mongo_u (id Int32, name String, n Nullable(String), lc LowCardinality(String), tags Array(String), nested Array(Array(Int32)), d Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_u VALUES (1, 'x', NULL, 'l', ['a', 'b'], [[1, 2], [3]], [1, 2]), (2, 'y', 'v', 'm', [], [], 's');
"

echo '-- a string and a number'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "name" : 1}}, {"$unwind" : "$name"}]);'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$unwind" : "$id"}, {"$match" : {"id" : 2}}, {"$project" : {"_id" : 0, "id" : 1, "lc" : 1}}]);'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "lc" : 1}}, {"$unwind" : {"path" : "$lc"}}]);'

echo '-- the field is still a string or a number'
mongo --query 'db.mongo_u.aggregate([{"$match" : {"id" : 1}}, {"$unwind" : "$name"}, {"$unwind" : "$id"}, {"$project" : {"_id" : 0, "name" : {"$concat" : ["$name", "!"]}, "id" : {"$add" : ["$id", 1]}}}]);'

echo '-- the index is null'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "name" : 1}}, {"$unwind" : {"path" : "$name", "includeArrayIndex" : "i"}}]);'

echo '-- a null is dropped'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "n" : 1}}, {"$unwind" : {"path" : "$n", "includeArrayIndex" : "i"}}]);'

echo '-- a null is kept with preserveNullAndEmptyArrays'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "n" : 1}}, {"$unwind" : {"path" : "$n", "includeArrayIndex" : "i", "preserveNullAndEmptyArrays" : true}}]);'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "name" : 1}}, {"$unwind" : {"path" : "$name", "preserveNullAndEmptyArrays" : true}}]);'

echo '-- an array is unwound'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "tags" : 1}}, {"$unwind" : {"path" : "$tags", "includeArrayIndex" : "i"}}]);'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "tags" : 1}}, {"$unwind" : {"path" : "$tags", "includeArrayIndex" : "i", "preserveNullAndEmptyArrays" : true}}]);'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "nested" : 1}}, {"$unwind" : "$nested"}]);'

echo '-- an array and then its elements'
mongo --query 'db.mongo_u.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "tags" : 1}}, {"$unwind" : "$tags"}, {"$unwind" : "$tags"}]);'

echo '-- a Dynamic field is rejected'
err --query 'db.mongo_u.aggregate([{"$project" : {"_id" : 0, "id" : 1, "d" : 1}}, {"$unwind" : "$d"}]);'
err --query 'db.mongo_u.aggregate([{"$project" : {"_id" : 0, "id" : 1, "d" : 1}}, {"$unwind" : {"path" : "$d", "preserveNullAndEmptyArrays" : true}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_u"

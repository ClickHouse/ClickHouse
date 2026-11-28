#!/usr/bin/env bash
# The Mongo dialect:
# - rejects a sort on a `Dynamic` field holding an array, as on an array column: Mongo sorts an
#   array by its smallest or largest element;
# - keeps a `null` held by a `Dynamic` field `null` through `$toDouble`, `$toInt`, `$toLong` and
#   `$toString`, also in the predicate of a delete;
# - rejects the logarithm of a number that is not positive, and a base of `$log` that is not
#   positive or is `1`;
# - adds integers exactly until it reaches a double, as Mongo does;
# - rejects `$inc` and `$mul` of a boolean field;
# - checks an inserted value against the type of the column of a temporary table that shadows a
#   table of the current database;
# - does not copy the preceding pipeline into the type check of every `$unionWith`.

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
    DROP TABLE IF EXISTS mongo_dyn;
    DROP TABLE IF EXISTS mongo_flag;
    DROP TABLE IF EXISTS mongo_shadowed;
    CREATE TABLE mongo_dyn (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dyn SELECT 1, [3, 10]::Array(Int64);
    INSERT INTO mongo_dyn SELECT 2, [4, 1]::Array(Int64);
    CREATE TABLE mongo_flag (id Int32, active Bool) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_flag VALUES (1, false);
    CREATE TABLE mongo_shadowed (x Int64) ENGINE = MergeTree ORDER BY tuple();
"

echo '-- a sort on a Dynamic holding arrays'
err --query 'db.mongo_dyn.aggregate([{"$sort" : {"v" : 1}}, {"$limit" : 1}]);'
err --query 'db.mongo_dyn.aggregate([{"$sort" : {"v" : -1}}, {"$limit" : 1}]);'
err --allow_suspicious_types_in_order_by 1 --query 'db.mongo_dyn.find({}).sort({"v" : 1}).limit(1);'

${CLICKHOUSE_CLIENT} --query "
    TRUNCATE TABLE mongo_dyn;
    INSERT INTO mongo_dyn SELECT 1, NULL;
    INSERT INTO mongo_dyn SELECT 2, 1::Int64;
    INSERT INTO mongo_dyn SELECT 3, 0::Int64;
"
echo '-- a sort on a Dynamic holding scalars'
mongo --allow_suspicious_types_in_order_by 1 --query 'db.mongo_dyn.find({}, {"_id" : 0, "id" : 1}).sort({"v" : -1});'

echo '-- conversions of a null held by a Dynamic'
mongo --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "id" : 1, "d" : {"$toDouble" : "$v"}, "i" : {"$toInt" : "$v"}, "l" : {"$toLong" : "$v"}, "s" : {"$toString" : "$v"}}}, {"$sort" : {"id" : 1}}]);'
mongo --query 'db.mongo_dyn.deleteMany({"$expr" : {"$eq" : [{"$toDouble" : "$v"}, 0]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id FROM mongo_dyn ORDER BY id"

echo '-- logarithms'
mongo --query 'db.mongo_dyn.aggregate([{"$match" : {"id" : 2}}, {"$project" : {"_id" : 0, "a" : {"$log" : [8, 2]}, "b" : {"$ln" : 1}, "c" : {"$log10" : 100}, "n" : {"$ln" : null}, "m" : {"$log" : [null, 2]}}}]);'
err --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "x" : {"$log" : [8, 1]}}}]);'
err --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "x" : {"$log" : [8, -2]}}}]);'
err --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "x" : {"$log" : [0, 2]}}}]);'
err --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "x" : {"$ln" : 0}}}]);'
err --query 'db.mongo_dyn.aggregate([{"$project" : {"_id" : 0, "x" : {"$log10" : -1}}}]);'
err --query 'db.mongo_flag.aggregate([{"$project" : {"_id" : 0, "x" : {"$ln" : {"$subtract" : ["$id", 1]}}}}]);'
err --query 'db.mongo_flag.aggregate([{"$project" : {"_id" : 0, "x" : {"$log" : [10, "$id"]}}}]);'

echo '-- integers are added exactly before a double'
mongo --query 'db.mongo_flag.aggregate([{"$project" : {"_id" : 0, "s" : {"$add" : [{"$numberLong" : "9007199254740993"}, {"$numberLong" : "-9007199254740992"}, 0.0]}, "d" : {"$add" : [1e16, 1.0, -1e16, 1.0]}, "p" : {"$multiply" : [{"$numberLong" : "9007199254740993"}, 1, 1.0]}}}]);'

echo '-- an arithmetic update of a boolean field'
err --query 'db.mongo_flag.updateMany({}, {"$inc" : {"active" : 1}});'
err --query 'db.mongo_flag.updateMany({}, {"$mul" : {"active" : 1}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mongo_flag'"
${CLICKHOUSE_CLIENT} --query "SELECT id, active FROM mongo_flag"

echo '-- an insert into a temporary table that shadows a table'
${CLICKHOUSE_CLIENT} --query "
    CREATE TEMPORARY TABLE mongo_shadowed (x Int32);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_shadowed.insertOne({\"x\" : 2147483648});
" 2>&1 | grep -oE '\([A-Z_]+\)$' | head -1
${CLICKHOUSE_CLIENT} --query "
    CREATE TEMPORARY TABLE mongo_shadowed (x Int32);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_shadowed.insertOne({\"x\" : 2147483647});
    SET dialect = 'clickhouse';
    SELECT x FROM mongo_shadowed;
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_shadowed"

echo '-- consecutive unions under a small AST budget'
union='{"$unionWith" : {"coll" : "mongo_flag", "pipeline" : [{"$project" : {"x" : {"$literal" : 1}}}]}}'
mongo --max_ast_elements 1000 --query "db.mongo_flag.aggregate([{\"\$project\" : {\"x\" : {\"\$literal\" : 1}}}, $union, $union, $union, $union, $union, $union, $union, $union, {\"\$count\" : \"n\"}]);"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_dyn;
    DROP TABLE mongo_flag;
    DROP TABLE mongo_shadowed;
"

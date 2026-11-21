#!/usr/bin/env bash
# The Mongo dialect:
# - checks the exact truncation of a decimal for `$toInt`, not its rounding to a double;
# - reads a `BFloat16` zero as false;
# - adds and multiplies floating point operands in their order, as Mongo does;
# - computes `$pow` of integers exactly, and rejects a decimal operand;
# - keeps `$divide` of integers a floating point division with `sql_compatible_division_for_integers`;
# - converts a `null` to the empty string in `$toUpper` and `$toLower`;
# - replaces an existing field with the index of `$unwind`;
# - rejects a numeric string for a numeric column and an integer that a `Float32` does not hold;
# - rejects a non-numeric operand of `$inc` and `$mul`, an operand of `$min` and `$max` of another
#   type than the field, and a `$rename` to a field of a narrower type, leaving the rows unchanged.

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
    DROP TABLE IF EXISTS mongo_n;
    CREATE TABLE mongo_n (id Int32, a Array(Int32), i Int32, s Nullable(String), b BFloat16, f Float32, d Decimal(38, 1), x Int32, big Int64, small Int32, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_n VALUES (1, [4, 5], 99, NULL, 0, 1, 1.5, 5, 2147483648, 7, 'n'), (2, [], 98, 'Ab', 1, 2, 2.5, 6, 1, 8, 'm');
"

echo '-- $toInt of a decimal just below the upper bound'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$toInt" : {"$numberDecimal" : "2147483647.999999999999999"}}, "l" : {"$toInt" : {"$numberDecimal" : "-2147483648.999999999999999"}}}}]);'
err --query 'db.mongo_n.aggregate([{"$project" : {"_id" : 0, "r" : {"$toInt" : {"$numberDecimal" : "2147483648"}}}}]);'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$toInt" : null}, "c" : {"$toInt" : "$f"}}}]);'

echo '-- BFloat16 zero is false'
mongo --query 'db.mongo_n.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "t" : {"$toBool" : "$b"}}}]);'
mongo --query 'db.mongo_n.find({"$expr" : "$b"}, {"_id" : 0, "id" : 1});'

echo '-- floating point operands in their order'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "x" : {"$add" : [1e16, 1.0, -1e16, 1.0]}, "y" : {"$add" : ["$f", 1e16, 1.0, -1e16, 1.0]}, "z" : {"$add" : [1, 2, 3]}}}]);'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "x" : {"$toString" : {"$multiply" : [1e300, 1e10, 1e-300, 0.5]}}, "n" : {"$add" : [null, 1.5, 2.5]}, "m" : {"$add" : [1.5, null, "$f"]}}}]);'

echo '-- $pow'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "p" : {"$pow" : [3, 34]}, "q" : {"$pow" : ["$x", 3]}, "r" : {"$pow" : [2, -1]}, "f" : {"$pow" : ["$f", 2]}, "z" : {"$pow" : ["$x", 0]}, "m" : {"$pow" : [-1, 1000001]}}}]);'
err --query 'db.mongo_n.aggregate([{"$project" : {"_id" : 0, "p" : {"$pow" : [3, 41]}}}]);'
err --query 'db.mongo_n.aggregate([{"$project" : {"_id" : 0, "p" : {"$pow" : [{"$numberDecimal" : "1.5"}, 2]}}}]);'
err --query 'db.mongo_n.aggregate([{"$project" : {"_id" : 0, "p" : {"$pow" : [3, "$i"]}}}]);'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "p" : {"$pow" : [2, "$x"]}}}]);'

echo '-- $divide with sql_compatible_division_for_integers'
mongo --sql_compatible_division_for_integers 1 --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "x" : {"$divide" : [5, 2]}, "y" : {"$divide" : ["$x", 2]}}}]);'
mongo --sql_compatible_division_for_integers 1 --query 'db.mongo_n.find({"$expr" : {"$eq" : [{"$divide" : ["$x", 2]}, 2.5]}}, {"_id" : 0, "id" : 1});'

echo '-- $toUpper and $toLower of null'
mongo --query 'db.mongo_n.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "u" : {"$ifNull" : [{"$toUpper" : null}, "fallback"]}, "s" : {"$toUpper" : "$s"}, "l" : {"$toLower" : "$s"}}}]);'

echo '-- $unwind replaces the field of the index'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$unwind" : {"path" : "$a", "includeArrayIndex" : "i"}}, {"$project" : {"_id" : 0, "a" : 1, "out" : "$i"}}]);'
mongo --query 'db.mongo_n.aggregate([{"$sort" : {"id" : 1}}, {"$unwind" : {"path" : "$a", "includeArrayIndex" : "i", "preserveNullAndEmptyArrays" : true}}, {"$project" : {"_id" : 0, "id" : 1, "a" : 1, "out" : "$i"}}]);'
mongo --query 'db.mongo_n.aggregate([{"$match" : {"id" : 1}}, {"$unwind" : {"path" : "$a", "includeArrayIndex" : "i"}}, {"$project" : {"_id" : 0, "id" : 0, "s" : 0, "b" : 0, "f" : 0, "d" : 0, "x" : 0, "big" : 0, "small" : 0, "name" : 0}}]);'

echo '-- a numeric string for a numeric column'
err --query 'db.mongo_n.insertOne({"id" : 3, "d" : "1.25"});'
err --query 'db.mongo_n.updateMany({}, {"$set" : {"d" : "1.25"}});'
err --query 'db.mongo_n.updateMany({}, {"$set" : {"a" : ["1"]}});'
mongo --query 'db.mongo_n.updateMany({"id" : 1}, {"$set" : {"name" : "1.25"}});'

echo '-- an integer that a Float32 does not hold'
err --query 'db.mongo_n.insertOne({"id" : 3, "f" : 16777217});'
err --query 'db.mongo_n.updateMany({}, {"$set" : {"f" : 16777217}});'
mongo --query 'db.mongo_n.insertOne({"id" : 4, "f" : 16777216});'
mongo --query 'db.mongo_n.updateMany({"id" : 4}, {"$set" : {"f" : 0.1}});'

echo '-- non-numeric operands of $inc and $mul'
err --query 'db.mongo_n.updateMany({}, {"$inc" : {"x" : "2"}});'
err --query 'db.mongo_n.updateMany({}, {"$inc" : {"x" : true}});'
err --query 'db.mongo_n.updateMany({}, {"$mul" : {"x" : "2"}});'
err --query 'db.mongo_n.updateMany({}, {"$inc" : {"x" : null}});'
mongo --query 'db.mongo_n.updateMany({"id" : 2}, {"$inc" : {"x" : {"$numberLong" : "1"}}});'

echo '-- operands of $min and $max of another type'
err --query 'db.mongo_n.updateMany({}, {"$min" : {"x" : "2"}});'
err --query 'db.mongo_n.updateMany({}, {"$max" : {"x" : "9"}});'
err --query 'db.mongo_n.updateMany({}, {"$max" : {"name" : 1}});'
mongo --query 'db.mongo_n.updateMany({"id" : 1}, {"$min" : {"x" : 2}});'

echo '-- $rename to a narrower type'
err --query 'db.mongo_n.updateMany({}, {"$rename" : {"big" : "small"}});'
mongo --query 'db.mongo_n.updateMany({"id" : 2}, {"$rename" : {"small" : "big"}});'

echo '-- nothing else was written, and no mutation failed'
mongo --query 'db.mongo_n.find({}, {"_id" : 0, "id" : 1, "f" : 1, "d" : 1, "x" : 1, "big" : 1, "small" : 1, "name" : 1}).sort({"id" : 1});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mongo_n' AND latest_fail_reason != ''"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_n"

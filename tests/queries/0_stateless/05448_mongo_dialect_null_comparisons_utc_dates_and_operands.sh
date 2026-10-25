#!/usr/bin/env bash
# - `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` and `$cmp` follow the BSON order of `null`: it
#   equals `null` and is less than any other value, and a comparison is always a boolean, also in
#   a `$expr` filter of `find` and of an update;
# - `$dateTrunc` and the date part operators read a date in UTC unless `timezone` says otherwise,
#   whatever the time zone of the column;
# - `$toLong` of a decimal truncates it exactly before checking the range;
# - nested `$toDate` do not grow the query exponentially;
# - an operand of an arithmetic operator that is not an expression is rejected;
# - a constant assigned by `$set` through a subdocument, a `$numberDecimal` or a nested array has
#   to fit the column, and is rejected before a mutation is created.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    CREATE TABLE mongo_t (id Int32, v Nullable(Int32), ms Int64, d DateTime64(3, 'Asia/Kolkata'), dec Decimal(20, 1), \`profile.age\` Int32, aa Array(Array(Int32))) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, NULL, 1704067200000, '2024-01-01 05:30:00', 9007199254740991.9, 1, [[1]]), (2, 5, 0, 0, 0, 2, [[2]]);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

echo '-- comparisons with null'
mongo --query 'db.mongo_t.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "eq" : {"$eq" : ["$v", null]}, "ne" : {"$ne" : ["$v", null]}, "lt" : {"$lt" : ["$v", 1]}, "lte" : {"$lte" : ["$v", null]}, "gt" : {"$gt" : ["$v", null]}, "gte" : {"$gte" : [null, "$v"]}, "cmp" : {"$cmp" : ["$v", null]}, "null_eq" : {"$eq" : [null, null]}}}]);'
mongo --query 'db.mongo_t.find({"$expr" : {"$eq" : ["$v", null]}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_t.find({"$expr" : {"$ne" : ["$v", null]}}, {"_id" : 0, "id" : 1});'
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"$expr" : {"$eq" : ["$v", null]}}, {"$set" : {"profile.age" : 7}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, \`profile.age\` FROM mongo_t ORDER BY id"

echo '-- dates in UTC'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "trunc" : {"$dateToString" : {"date" : {"$dateTrunc" : {"date" : "$d", "unit" : "day"}}}}, "year" : {"$year" : "$d"}, "day" : {"$dayOfMonth" : "$d"}, "hour" : {"$hour" : "$d"}, "minute" : {"$minute" : "$d"}, "week" : {"$week" : "$d"}, "dow" : {"$dayOfWeek" : "$d"}, "iso_dow" : {"$isoDayOfWeek" : "$d"}, "kolkata_hour" : {"$hour" : {"date" : "$d", "timezone" : "Asia/Kolkata"}}, "kolkata_minute" : {"$minute" : {"date" : "$d", "timezone" : "Asia/Kolkata"}}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"h" : {"$hour" : {"date" : "$d", "zone" : "UTC"}}}}]);'

echo '-- $toLong of a decimal'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "constant" : {"$toLong" : {"$numberDecimal" : "9007199254740991.9"}}, "column" : {"$toLong" : "$dec"}, "negative" : {"$toLong" : {"$numberDecimal" : "-9223372036854775808.9"}}}}]);'
err --query 'db.mongo_t.aggregate([{"$project" : {"v" : {"$toLong" : {"$numberDecimal" : "9223372036854775808"}}}}]);'

echo '-- bounded growth of $toDate'
nested='"$ms"'
for _ in $(seq 1 10); do nested="{\"\$toDate\" : ${nested}}"; done
mongo --max_ast_elements 10000 --query "db.mongo_t.aggregate([{\"\$match\" : {\"id\" : 1}}, {\"\$project\" : {\"_id\" : 0, \"v\" : {\"\$dateToString\" : {\"date\" : ${nested}}}}}]);"

echo '-- arithmetic operands that are not expressions'
err --query 'db.mongo_t.find({}, {"x" : {"$add" : [{}, 1]}});'
err --query 'db.mongo_t.find({}, {"x" : {"$add" : [{"id" : 1}, 1]}});'
err --query 'db.mongo_t.find({}, {"x" : {"$add" : [1, {"$comment" : "c"}]}});'
mongo --query 'db.mongo_t.find({"id" : 2}, {"_id" : 0, "x" : {"$add" : [{"$add" : ["id", 1]}, 1]}});'

echo '-- constants assigned through a subdocument, a wrapper or a nested array'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({}, {"$set" : {"profile" : {"age" : 2147483648}}});'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({}, {"$set" : {"profile.age" : {"$numberDecimal" : "2147483648"}}});'
err --mutations_sync 2 --query 'db.mongo_t.updateMany({}, {"$set" : {"aa" : [[2147483648]]}});'
mongo --mutations_sync 2 --query 'db.mongo_t.updateMany({"id" : 2}, {"$set" : {"profile" : {"age" : 3}, "aa" : [[3, 4]]}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, \`profile.age\`, aa FROM mongo_t ORDER BY id; SELECT count() FROM system.mutations WHERE database = currentDatabase() AND NOT is_done"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"

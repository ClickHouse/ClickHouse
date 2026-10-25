#!/usr/bin/env bash
# - `$toBool`, `$toLong` and `$toDouble` follow MongoDB: every string is true, a date converts to
#   its milliseconds, and a value out of the range of a 64-bit integer is rejected;
# - date arithmetic in `$add` and `$subtract` is rejected rather than counted in seconds;
# - `$dateAdd` and `$dateDiff` honour `timezone`, and `$dateDiff` weeks start on Sunday by default;
# - `$dateToString` formats in UTC by default, and omits the `Z` for another time zone;
# - a missing array element is null, `$setDifference` is a set, `$regexFind` reports the index of
#   the match in code points, and `$$x` is the variable while `$x` is the field;
# - a regular expression's dot matches a newline only with the `s` option, and `x` is rejected;
# - nested conversions and long `$addToSet` lists do not grow the query exponentially;
# - `$pull` of `null` keeps the other elements, a constant that does not fit the column is
#   rejected, and the Mongo statement itself is bounded by `max_query_size`;
# - an update of a subdocument with hidden leaves is denied instead of becoming a partial write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05427_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_conv;
    DROP TABLE IF EXISTS mongo_upd;
    DROP TABLE IF EXISTS mongo_secret;
    DROP USER IF EXISTS ${user};
    CREATE TABLE mongo_conv (id Int32, s String, n Int64, x Int32, a Array(Int32), d DateTime64(3, 'UTC'), k DateTime64(3, 'Asia/Kolkata'), text String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_conv VALUES (1, 'false', 0, 10, [1, 2], '2021-03-13 15:00:00.500', 0, 'a\nb');
    CREATE TABLE mongo_upd (id Int32, x Int32, a Array(Int32), na Array(Nullable(Int32))) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_upd VALUES (1, 1, [1, 2], [1, NULL, 2]);
    CREATE TABLE mongo_secret (id Int32, \`profile.name\` String, \`profile.secret\` Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_secret VALUES (1, 'a', 1);
    CREATE USER ${user};
    GRANT SELECT, ALTER UPDATE(\`profile.name\`) ON ${CLICKHOUSE_DATABASE}.mongo_secret TO ${user};
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

echo '-- conversions'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "b1" : {"$toBool" : "$s"}, "b2" : {"$toBool" : ""}, "b3" : {"$toBool" : "$n"}, "b4" : {"$toBool" : "$x"}, "b5" : {"$toBool" : null}, "l" : {"$toLong" : "$d"}, "f" : {"$toDouble" : "$d"}, "m" : {"$toLong" : 9223372036854775807}, "t" : {"$toLong" : -1.7}, "any" : {"$anyElementTrue" : [["", 0]]}, "all" : {"$allElementsTrue" : [["false", null]]}}}]);'
err --query 'db.mongo_conv.aggregate([{"$project" : {"v" : {"$toLong" : 9223372036854775808}}}]);'
err --query 'db.mongo_conv.aggregate([{"$project" : {"v" : {"$toLong" : 1e19}}}]);'

echo '-- date arithmetic in $add and $subtract'
err --query 'db.mongo_conv.aggregate([{"$project" : {"v" : {"$add" : ["$d", 1]}}}]);'
err --query 'db.mongo_conv.aggregate([{"$project" : {"v" : {"$subtract" : ["$d", "$d"]}}}]);'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "v" : {"$add" : ["$x", 1, 2]}, "w" : {"$subtract" : ["$x", 3]}}}]);'

echo '-- $dateAdd, $dateDiff and $dateToString'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "added" : {"$dateToString" : {"date" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1, "timezone" : "America/New_York"}}}}, "utc" : {"$dateToString" : {"date" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1}}}}}}]);'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "sunday" : {"$dateDiff" : {"startDate" : {"$date" : "2021-01-03T00:00:00Z"}, "endDate" : {"$date" : "2021-01-04T00:00:00Z"}, "unit" : "week"}}, "monday" : {"$dateDiff" : {"startDate" : {"$date" : "2021-01-03T00:00:00Z"}, "endDate" : {"$date" : "2021-01-04T00:00:00Z"}, "unit" : "week", "startOfWeek" : "mon"}}, "saturday" : {"$dateDiff" : {"startDate" : {"$date" : "2021-01-02T00:00:00Z"}, "endDate" : {"$date" : "2021-01-03T00:00:00Z"}, "unit" : "week"}}, "days" : {"$dateDiff" : {"startDate" : {"$date" : "2021-01-01T23:00:00Z"}, "endDate" : {"$date" : "2021-01-02T01:00:00Z"}, "unit" : "day", "timezone" : "Asia/Tokyo"}}}}]);'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "default" : {"$dateToString" : {"date" : "$k"}}, "kolkata" : {"$dateToString" : {"date" : "$k", "timezone" : "Asia/Kolkata"}}}}]);'

echo '-- arrays, sets, regular expressions and variables'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "past" : {"$ifNull" : [{"$arrayElemAt" : ["$a", 5]}, 99]}, "before" : {"$ifNull" : [{"$arrayElemAt" : ["$a", -5]}, 99]}, "first" : {"$ifNull" : [{"$first" : {"$filter" : {"input" : "$a", "cond" : false}}}, 99]}, "difference" : {"$setDifference" : [[1, 1, 2], [2]]}, "mapped" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$add" : ["$$x", "$x"]}}}, "nested" : {"$map" : {"input" : "$a", "as" : "x", "in" : {"$size" : {"$filter" : {"input" : "$a", "as" : "y", "cond" : {"$gte" : ["$$y", "$$x"]}}}}}}}}]);'
mongo --query 'db.mongo_conv.aggregate([{"$project" : {"_id" : 0, "anchored" : {"$regexFind" : {"input" : "a a", "regex" : "a$"}}, "multibyte" : {"$regexFind" : {"input" : "éfoo", "regex" : "foo"}}}}]);'
mongo --query 'db.mongo_conv.find({"text" : {"$regex" : "^a.b$"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_conv.find({"text" : {"$regex" : "^a.b$", "$options" : "i"}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_conv.find({"text" : {"$regex" : "^a.b$", "$options" : "s"}}, {"_id" : 0, "id" : 1});'
err --query 'db.mongo_conv.find({"text" : {"$regex" : "a b", "$options" : "x"}});'

echo '-- bounded growth'
nested='"$x"'
for _ in $(seq 1 10); do nested="{\"\$toInt\" : ${nested}}"; done
mongo --max_ast_elements 10000 --query "db.mongo_conv.aggregate([{\"\$project\" : {\"_id\" : 0, \"v\" : ${nested}}}]);"
values=$(seq -s ', ' 1 40)
mongo --max_ast_elements 10000 --mutations_sync 2 --query "db.mongo_upd.updateMany({}, {\"\$addToSet\" : {\"a\" : {\"\$each\" : [${values}, 3, 3]}}});"
${CLICKHOUSE_CLIENT} --query "SELECT length(a), arraySum(a) FROM mongo_upd"

echo '-- update values'
mongo --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$set" : {"a" : [1, 2]}});'
mongo --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$pull" : {"a" : null}});'
mongo --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$pull" : {"na" : 1}});'
mongo --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$addToSet" : {"a" : {"$each" : [2, 3, 3]}}});'
${CLICKHOUSE_CLIENT} --query "SELECT a, na FROM mongo_upd"
err --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$set" : {"x" : 2147483648}});'
err --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$inc" : {"x" : 1.5}});'
err --mutations_sync 2 --query 'db.mongo_upd.updateMany({}, {"$push" : {"a" : 2147483648}});'
${CLICKHOUSE_CLIENT} --query "SELECT x, a FROM mongo_upd; SELECT count() FROM system.mutations WHERE database = currentDatabase() AND NOT is_done"

echo '-- max_query_size'
err --max_query_size 1000 --query "db.mongo_conv.find({\"\$comment\" : \"$(printf 'c%.0s' $(seq 1 2000))\"});"

echo '-- an update of a subdocument with hidden leaves'
err --user "${user}" --mutations_sync 2 --query 'db.mongo_secret.updateMany({}, {"$unset" : {"profile" : ""}});'
mongo --user "${user}" --mutations_sync 2 --query 'db.mongo_secret.updateMany({}, {"$set" : {"profile.name" : "b"}});'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM mongo_secret"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_conv;
    DROP TABLE mongo_upd;
    DROP TABLE mongo_secret;
    DROP USER ${user};
"

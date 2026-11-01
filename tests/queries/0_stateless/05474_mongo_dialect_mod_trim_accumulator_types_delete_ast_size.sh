#!/usr/bin/env bash
# The Mongo dialect:
# - truncates the divisor and the remainder of the query `$mod` towards zero, and the field as well,
#   and matches only a numeric field;
# - trims the default whitespace of Mongo in `$trim`, `$ltrim` and `$rtrim`: the null character,
#   the ASCII whitespace and the Unicode spaces;
# - skips the values that are not numbers - a date, a string - in `$sum`, `$avg` and the standard
#   deviations;
# - sums decimals without wrapping around and averages them as decimals;
# - checks the size of the filter of `deleteMany` against `max_ast_elements`.

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
    CREATE TABLE mongo_t (id Int32, x Int32, f Float64, s String, d DateTime64(3, 'UTC'), nd Nullable(DateTime64(3, 'UTC')), dec Decimal(38, 0)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 9, 9.7, '9', '2020-01-01 00:00:00', NULL, 90000000000000000000000000000000000000), (2, -9, -9.7, '8', '2021-01-01 00:00:00', '2021-01-01 00:00:00', 90000000000000000000000000000000000000);
"

echo '-- $mod'
mongo --query 'db.mongo_t.find({"x" : {"$mod" : [4.5, 1]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$mod" : [4, 1.9]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$mod" : [-4, -1]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"f" : {"$mod" : [4, 1]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"x" : {"$mod" : [-1, 0]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"s" : {"$mod" : [3, 0]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
err --query 'db.mongo_t.find({"x" : {"$mod" : [0.5, 0]}});'
err --query 'db.mongo_t.find({"x" : {"$mod" : [1e300, 0]}});'
err --query 'db.mongo_t.find({"x" : {"$mod" : ["4", 0]}});'
# Truncated, the divisor is 4, and 9 is not a multiple of it: the document stays.
mongo --mutations_sync 2 --query 'db.mongo_t.deleteMany({"x" : {"$mod" : [4.5, 0]}});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t"

echo '-- $trim'
mongo --query 'db.mongo_t.aggregate([{"$limit" : 1}, {"$project" : {"_id" : 0, "t" : {"$trim" : {"input" : "\t\u0000 alpha beta\n\r\u000b\f    "}}, "l" : {"$ltrim" : {"input" : "\n　 alpha "}}, "r" : {"$rtrim" : {"input" : " alpha\t "}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$limit" : 1}, {"$project" : {"_id" : 0, "t" : {"$trim" : {"input" : " \t\n"}}, "e" : {"$trim" : {"input" : ""}}, "n" : {"$trim" : {"input" : null}}}}]);'

echo '-- non-numbers in numeric accumulators'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "a" : {"$avg" : "$d"}, "an" : {"$avg" : "$nd"}, "as" : {"$avg" : "$s"}, "s" : {"$sum" : "$d"}, "ss" : {"$sum" : "$s"}, "p" : {"$stdDevPop" : "$d"}, "q" : {"$stdDevSamp" : "$nd"}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "a" : {"$avg" : {"$date" : 1500}}, "s" : {"$sum" : {"$date" : 1500}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "a" : {"$avg" : "$x"}, "f" : {"$avg" : "$f"}, "s" : {"$sum" : "$x"}, "p" : {"$stdDevPop" : "$x"}}}]);'

echo '-- decimals in $sum and $avg'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "s" : {"$sum" : "$dec"}, "c" : {"$sum" : {"$numberDecimal" : "9e37"}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "a" : {"$avg" : {"$numberDecimal" : "9007199254740993"}}, "h" : {"$avg" : {"$cond" : [{"$eq" : ["$id", 1]}, {"$numberDecimal" : "1"}, {"$numberDecimal" : "2"}]}}}}]);'
mongo --query 'db.mongo_t.aggregate([{"$match" : {"id" : 3}}, {"$group" : {"_id" : null, "a" : {"$avg" : "$dec"}}}]);'

echo '-- the size of the filter of deleteMany'
filter=$(python3 -c "print(', '.join('{\"id\" : %d}' % i for i in range(300)))")
err --max_ast_elements 200 --query "db.mongo_t.deleteMany({\"\$or\" : [${filter}]});"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mongo_t"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"

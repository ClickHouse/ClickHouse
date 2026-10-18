#!/usr/bin/env bash
# The semantics of the `mongo` dialect where the ClickHouse functions it is lowered into differ:
#
# - a `null` sort key orders first in an ascending sort and last in a descending one, also for the
#   accumulators that read the documents of a group in the order of a `$sort`;
# - a `$sortByCount` orders by the key and the size of each group, even when the source has columns
#   named `_id` and `count`;
# - `$toInt` and `$toLong` truncate a fraction, refuse a value out of their range, and `$toLong`
#   and `$toDouble` convert a date into milliseconds;
# - `$add` and `$subtract` move a date by milliseconds, and the difference of two dates is in
#   milliseconds;
# - `$anyElementTrue` and `$allElementsTrue` decide the truth of an element by its type;
# - an operator that takes a document refuses a scalar;
# - `$pull` keeps the elements that do not equal its value, `null` included;
# - `$addToSet` with `$each` adds equal values once, whatever their literals.
#
# The errors run through the shell rather than through a `-- { serverError ... }` hint: a comment
# is part of the query text in the Mongo dialect, so an annotation would change the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS null_order;
    CREATE TABLE null_order (id Int64, k Int64, ts Nullable(Int64), v Int64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO null_order VALUES (1, 1, NULL, 10), (2, 1, 1, 20), (3, 1, 2, 30);

    DROP TABLE IF EXISTS sort_by_count;
    CREATE TABLE sort_by_count (_id String, kind String, count Int64) ENGINE = MergeTree ORDER BY _id;
    INSERT INTO sort_by_count VALUES ('a', 'x', 100), ('b', 'y', 1), ('c', 'y', 1);

    DROP TABLE IF EXISTS conversions;
    CREATE TABLE conversions (id Int64, big Int64, f Float64, d DateTime64(3, 'UTC'), e DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id;
    INSERT INTO conversions VALUES (1, 2147483648, -2.9, '2024-03-09 23:30:00.123', '2024-03-09 23:30:01.000');

    DROP TABLE IF EXISTS pulled;
    CREATE TABLE pulled (id Int64, arr Array(Nullable(Int64)), tags Array(Float64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO pulled VALUES (1, [1, NULL, 2], []), (2, [1, NULL, 2], []);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --mutations_sync 2 --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

# The name of the error is what the reference records: the message quotes the query.
mongo_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | grep -oE '\(([A-Z_]+)\)' | tail -1
}

echo '-- a null sort key orders first ascending and last descending'
mongo 'db.null_order.find({}).sort({"ts" : 1}).limit(1);'
mongo 'db.null_order.aggregate([{"$sort" : {"ts" : 1}}, {"$project" : {"_id" : 0, "id" : 1}}]);'
mongo 'db.null_order.aggregate([{"$sort" : {"ts" : -1}}, {"$project" : {"_id" : 0, "id" : 1}}]);'

echo '-- and so does it for the accumulators that follow a sort'
mongo 'db.null_order.aggregate([{"$sort" : {"ts" : 1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}, "all" : {"$push" : "$v"}}}]);'
mongo 'db.null_order.aggregate([{"$sort" : {"ts" : -1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "all" : {"$push" : "$v"}}}]);'

echo '-- a sortByCount over columns named _id and count'
mongo 'db.sort_by_count.aggregate([{"$sortByCount" : "$kind"}]);'

echo '-- integer conversions'
mongo 'db.conversions.aggregate([{"$project" : {"_id" : 0, "f" : {"$toInt" : "$f"}, "l" : {"$toLong" : "$big"}, "s" : {"$toInt" : "42"}, "b" : {"$toLong" : true}, "n" : {"$toInt" : null}}}]);'
mongo_error 'db.conversions.aggregate([{"$project" : {"_id" : 0, "i" : {"$toInt" : 2147483648}}}]);'
mongo_error 'db.conversions.aggregate([{"$project" : {"_id" : 0, "i" : {"$toInt" : "$big"}}}]);'
mongo_error 'db.conversions.aggregate([{"$project" : {"_id" : 0, "i" : {"$toInt" : 2147483648.5}}}]);'

echo '-- a date converts into milliseconds'
mongo 'db.conversions.aggregate([{"$project" : {"_id" : 0, "l" : {"$toLong" : "$d"}, "dl" : {"$toDouble" : "$d"}, "literal" : {"$toLong" : {"$date" : "2024-03-09T23:30:00.123Z"}}}}]);'

echo '-- date arithmetic is in milliseconds'
mongo 'db.conversions.aggregate([{"$project" : {"_id" : 0, "plus" : {"$add" : ["$d", 1000]}, "reversed" : {"$add" : [1000, "$d", 1]}, "minus" : {"$subtract" : ["$d", 1000]}, "between" : {"$subtract" : ["$e", "$d"]}, "numbers" : {"$add" : ["$id", 1, 2.5]}, "difference" : {"$subtract" : ["$id", "$f"]}}}]);'
mongo 'db.conversions.aggregate([{"$project" : {"_id" : 0, "plus" : {"$add" : [{"$date" : "2024-03-09T23:30:00.123Z"}, 1]}}}]);'

echo '-- the truth of an array element is decided by its type'
mongo 'db.conversions.aggregate([{"$project" : {"_id" : 0, "any_strings" : {"$anyElementTrue" : [["false", ""]]}, "all_strings" : {"$allElementsTrue" : [["false", ""]]}, "zero" : {"$anyElementTrue" : [[0, null]]}, "with_null" : {"$allElementsTrue" : [[1, null]]}, "nested" : {"$allElementsTrue" : [[[0], [false]]]}}}]);'

echo '-- an operator that takes a document refuses a scalar'
mongo_error 'db.conversions.aggregate([{"$project" : {"t" : {"$dateTrunc" : "xxxxxxxxxxxx"}}}]);'
mongo_error 'db.conversions.aggregate([{"$project" : {"t" : {"$trim" : "xxxxxxxxxxxx"}}}]);'
mongo_error 'db.conversions.aggregate([{"$project" : {"t" : {"$ltrim" : 1}}}]);'

echo '-- pull keeps the elements that do not equal its value'
mongo 'db.pulled.updateMany({"id" : 1}, {"$pull" : {"arr" : null}});'
mongo 'db.pulled.updateMany({"id" : 2}, {"$pull" : {"arr" : 1}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, arr FROM pulled ORDER BY id"

echo '-- addToSet adds equal values once'
mongo 'db.pulled.updateMany({"id" : 1}, {"$addToSet" : {"tags" : {"$each" : [1, 1.0, 2, 1]}}});'
mongo 'db.pulled.updateMany({"id" : 1}, {"$addToSet" : {"tags" : {"$each" : [2.0, 3]}}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, tags FROM pulled ORDER BY id"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE null_order;
    DROP TABLE sort_by_count;
    DROP TABLE conversions;
    DROP TABLE pulled;
"

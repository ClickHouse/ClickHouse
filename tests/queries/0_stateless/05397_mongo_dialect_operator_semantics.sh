#!/usr/bin/env bash
# The semantics of operators of the `mongo` dialect that are easy to get subtly wrong:
#
# - a `$sort` orders the documents the stage before it produced, also by a field that stage replaced;
# - a `$count` and a `$group` by `null` produce nothing out of an empty stream;
# - `$toBool` converts by type: every string is `true`, a number is `false` when it is zero;
# - `$toDecimal` of a double keeps its shortest decimal text;
# - an index of `$arrayElemAt` computed per document counts from the end when it is negative;
# - `$$x` is the variable of `$map` and `$filter`, while `$x` stays the field of the document;
# - the date operators run in UTC unless they name a time zone, whatever the time zone of the type;
# - `$first`, `$last` and `$push` keep a `null` value;
# - the `idx` of `$regexFind` is the position of the match, in code points;
# - nesting the operators that refer to their argument twice is refused promptly;
# - `$addToSet` with `$each` adds each new value once and keeps what the array holds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS semantics;
    CREATE TABLE semantics (_id String, k Int64, ts Int64, a Int64, v Nullable(String), s String, arr Array(Int64), idx Int64,
        x Int64, d DateTime64(3, 'America/New_York')) ENGINE = MergeTree ORDER BY _id;
    INSERT INTO semantics VALUES
        ('p', 1, 1, 1, NULL, 'xAx', [10, 20], -1, 100, '2026-01-01 00:00:00'),
        ('q', 1, 2, 2, 'x', 'éx', [30, 40, 50], 1, 200, '2026-01-01 00:00:00');
    -- The time is the instant 2026-01-01T00:00:00Z, written in the time zone of the column.
    ALTER TABLE semantics UPDATE d = toDateTime64('2026-01-01 00:00:00', 3, 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

    DROP TABLE IF EXISTS add_to_set;
    CREATE TABLE add_to_set (id Int64, tags Array(String)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO add_to_set VALUES (1, ['a', 'a', 'b']);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --mutations_sync 2 --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

echo '-- a sort by a replaced field'
mongo 'db.semantics.aggregate([{"$project" : {"_id" : 0, "a" : {"$multiply" : ["$a", -1]}}}, {"$sort" : {"a" : 1}}]);'
mongo 'db.semantics.aggregate([{"$set" : {"a" : {"$multiply" : ["$a", -1]}}}, {"$sort" : {"a" : 1}}, {"$project" : {"_id" : 0, "a" : 1}}]);'
mongo 'db.semantics.aggregate([{"$group" : {"_id" : "$a", "a" : {"$sum" : {"$multiply" : ["$a", -1]}}}}, {"$sort" : {"a" : 1}}]);'

echo '-- an empty stream'
mongo 'db.semantics.aggregate([{"$match" : {"k" : 7}}, {"$count" : "c"}]);'
mongo 'db.semantics.aggregate([{"$match" : {"k" : 7}}, {"$group" : {"_id" : null, "c" : {"$sum" : 1}}}]);'
mongo 'db.semantics.aggregate([{"$count" : "c"}]);'

echo '-- $toBool'
mongo 'db.semantics.aggregate([{"$match" : {"_id" : "p"}}, {"$project" : {"_id" : 0, "f" : {"$toBool" : "false"}, "e" : {"$toBool" : ""}, "z" : {"$toBool" : 0}, "o" : {"$toBool" : 2.5}, "n" : {"$toBool" : null}}}]);'
mongo 'db.semantics.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "s" : {"$toBool" : "$s"}, "v" : {"$toBool" : "$v"}, "z" : {"$toBool" : {"$subtract" : ["$k", 1]}}, "i" : {"$toBool" : "$idx"}}}]);'

echo '-- $toDecimal of a double'
mongo 'db.semantics.aggregate([{"$match" : {"_id" : "p"}}, {"$project" : {"_id" : 0, "d" : {"$toDecimal" : 1.15}}}]);'

echo '-- $arrayElemAt with a computed index'
mongo 'db.semantics.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "e" : {"$arrayElemAt" : ["$arr", "$idx"]}, "c" : {"$arrayElemAt" : ["$arr", {"$subtract" : ["$k", 2]}]}, "z" : {"$arrayElemAt" : ["$arr", {"$subtract" : ["$k", 1]}]}}}]);'

echo '-- a variable named after a field'
mongo 'db.semantics.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "m" : {"$map" : {"input" : "$arr", "as" : "x", "in" : {"$add" : ["$$x", "$x"]}}}, "f" : {"$filter" : {"input" : "$arr", "as" : "x", "cond" : {"$gt" : ["$$x", {"$divide" : ["$x", 5]}]}}}}}]);'

echo '-- the date operators in UTC'
mongo 'db.semantics.aggregate([{"$match" : {"_id" : "p"}}, {"$project" : {"_id" : 0, "default" : {"$dateToString" : {"date" : "$d"}}, "formatted" : {"$dateToString" : {"date" : "$d", "format" : "%Y-%m-%d %H"}}, "named" : {"$dateToString" : {"date" : "$d", "format" : "%Y-%m-%d %H", "timezone" : "America/New_York"}}}}]);'
mongo 'db.semantics.aggregate([{"$match" : {"_id" : "p"}}, {"$project" : {"_id" : 0, "day" : {"$dateToString" : {"date" : {"$dateTrunc" : {"date" : "$d", "unit" : "day"}}, "format" : "%Y-%m-%d %H"}}, "hour" : {"$hour" : "$d"}, "local" : {"$hour" : {"date" : "$d", "timezone" : "America/New_York"}}, "year" : {"$year" : "$d"}}}]);'

echo '-- null values of the accumulators'
mongo 'db.semantics.aggregate([{"$sort" : {"ts" : 1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}, "pushed" : {"$push" : "$v"}}}]);'
mongo 'db.semantics.aggregate([{"$match" : {"_id" : "p"}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "pushed" : {"$push" : "$v"}, "set" : {"$addToSet" : "$v"}, "firstN" : {"$firstN" : {"input" : "$v", "n" : 2}}}}]);'

echo '-- the index of $regexFind'
mongo 'db.semantics.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "$s", "regex" : "x$"}}}}]);'
mongo 'db.semantics.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "r" : {"$regexFind" : {"input" : "$s", "regex" : "X", "options" : "i"}}}}]);'

echo '-- deeply nested operators that copy their argument'
nested='"$ts"'
for _ in $(seq 1 15); do nested="{\"\$toDate\" : ${nested}}"; done
mongo "db.semantics.aggregate([{\"\$project\" : {\"d\" : ${nested}}}]);" | grep -o -m1 'larger than 100000 elements'

echo '-- $addToSet with $each'
mongo 'db.add_to_set.updateMany({"id" : 1}, {"$addToSet" : {"tags" : {"$each" : ["b", "c", "c", "d", "a", "c"]}}});'
${CLICKHOUSE_CLIENT} --query "SELECT tags FROM add_to_set"
values=$(for i in $(seq 1 40); do printf '"v%d", ' "$i"; done)
mongo "db.add_to_set.updateMany({\"id\" : 1}, {\"\$addToSet\" : {\"tags\" : {\"\$each\" : [${values} \"v1\"]}}});"
${CLICKHOUSE_CLIENT} --query "SELECT length(tags), arrayUniq(tags) FROM add_to_set"

${CLICKHOUSE_CLIENT} --query "DROP TABLE semantics; DROP TABLE add_to_set"

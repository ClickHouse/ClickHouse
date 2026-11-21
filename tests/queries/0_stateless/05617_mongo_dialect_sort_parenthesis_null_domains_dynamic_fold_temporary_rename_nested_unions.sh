#!/usr/bin/env bash
# The Mongo dialect:
# - sorts by a field whose name holds a `)`;
# - returns `null` from `$divide` and `$log` when an operand is `null`, before it checks the domain
#   of the other one;
# - adds the doubles held by a `Dynamic` field in the order of the operands, and multiplies a
#   product of integers and doubles as Mongo does;
# - checks a `$rename` of a temporary table against the types of its own columns;
# - bounds the query a nesting of `$unionWith` stages translates into.

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
    DROP TABLE IF EXISTS mongo_paren;
    DROP TABLE IF EXISTS mongo_nulls;
    DROP TABLE IF EXISTS mongo_dyn_fold;
    CREATE TABLE mongo_paren (id Int32, \`x)y\` Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_paren VALUES (1, 2), (2, 1), (3, 3);
    CREATE TABLE mongo_nulls (id Int32, n Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_nulls VALUES (1, NULL);
    CREATE TABLE mongo_dyn_fold (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dyn_fold SELECT 1, 1e16::Float64;
    INSERT INTO mongo_dyn_fold SELECT 2, 5::Int64;
"

echo '-- a sort by a field with a parenthesis'
mongo --query 'db.mongo_paren.find({}, {"_id" : 0, "id" : 1}).sort({"x)y" : 1});'
mongo --query 'db.mongo_paren.find({}, {"_id" : 0, "id" : 1}).sort({"x)y" : -1}).limit(1);'

echo '-- null operands of $divide and $log'
mongo --query 'db.mongo_nulls.aggregate([{"$project" : {"_id" : 0, "a" : {"$divide" : [null, 0]}, "b" : {"$divide" : ["$n", 0]}, "c" : {"$log" : [null, 1]}, "d" : {"$log" : [0, null]}, "e" : {"$log" : ["$n", 1]}, "f" : {"$log" : [0, "$n"]}}}]);'
err --query 'db.mongo_nulls.aggregate([{"$project" : {"_id" : 0, "a" : {"$divide" : [1, 0]}}}]);'
err --query 'db.mongo_nulls.aggregate([{"$project" : {"_id" : 0, "a" : {"$log" : [0, 2]}}}]);'
err --query 'db.mongo_nulls.aggregate([{"$project" : {"_id" : 0, "a" : {"$log" : [2, 1]}}}]);'

echo '-- the order of the operands of $add of a Dynamic and the promotion of $multiply'
mongo --query 'db.mongo_dyn_fold.aggregate([{"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "id" : 1, "s" : {"$add" : ["$v", 1.0, -1e16, 1.0]}}}]);'
mongo --query 'db.mongo_dyn_fold.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "p" : {"$multiply" : [{"$numberLong" : "9007199254740993"}, 3, 1.0]}, "q" : {"$multiply" : [{"$numberLong" : "9007199254740993"}, 3]}, "s" : {"$add" : [{"$numberLong" : "9007199254740993"}, {"$numberLong" : "-9007199254740992"}, 0.0]}}}]);'

echo '-- $rename to a narrower type in a temporary table'
# The rename is rejected, and both fields keep their values. Only the name of the error and the
# selected row are kept: a debug build prints the stack trace of the error, whose frames also
# start with a number. With parallel replicas, reading a temporary table logs a warning that the
# other replicas do not have it, so only errors are sent to the client.
${CLICKHOUSE_CLIENT} --send_logs_level=error --mutations_sync 2 --ignore-error --query "
    CREATE TEMPORARY TABLE mongo_tmp_rename (id Int32, source Int64, target Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_tmp_rename VALUES (1, 2147483648, 7);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_tmp_rename.updateMany({}, {\"\$rename\" : {\"source\" : \"target\"}});
    SET dialect = 'clickhouse';
    SELECT source, target FROM mongo_tmp_rename;
" 2>&1 | grep -oE '\([A-Z_]+\)$|^[0-9]+'$'\t''[0-9]+$' | uniq
# A rename to a wider type works.
${CLICKHOUSE_CLIENT} --send_logs_level=error --mutations_sync 2 --query "
    CREATE TEMPORARY TABLE mongo_tmp_rename (id Int32, source Int32, target Int64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_tmp_rename VALUES (1, 5, 7);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_tmp_rename.updateMany({}, {\"\$rename\" : {\"source\" : \"target\"}});
    SET dialect = 'clickhouse';
    SELECT source, target FROM mongo_tmp_rename;
"

echo '-- nested unions'
# P(0) is a projection, P(n) the projection and a union with P(n - 1): the text holds one copy of
# every level.
nested_pipeline() {
    local pipeline='[{"$project" : {"_id" : 0, "x" : {"$literal" : 1}}}]'
    for _ in $(seq 1 "$1"); do
        pipeline="[{\"\$project\" : {\"_id\" : 0, \"x\" : {\"\$literal\" : 1}}}, {\"\$unionWith\" : {\"coll\" : \"mongo_paren\", \"pipeline\" : ${pipeline}}}]"
    done
    echo "$pipeline"
}
mongo --query "db.mongo_paren.aggregate([{\"\$project\" : {\"_id\" : 0, \"x\" : {\"\$literal\" : 1}}}, {\"\$unionWith\" : {\"coll\" : \"mongo_paren\", \"pipeline\" : $(nested_pipeline 3)}}, {\"\$group\" : {\"_id\" : null, \"c\" : {\"\$sum\" : 1}}}, {\"\$project\" : {\"_id\" : 0, \"c\" : 1}}]);"
err --query "db.mongo_paren.aggregate([{\"\$project\" : {\"_id\" : 0, \"x\" : {\"\$literal\" : 1}}}, {\"\$unionWith\" : {\"coll\" : \"mongo_paren\", \"pipeline\" : $(nested_pipeline 40)}}]);"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_paren;
    DROP TABLE mongo_nulls;
    DROP TABLE mongo_dyn_fold;
"

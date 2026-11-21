#!/usr/bin/env bash
# The Mongo dialect:
# - rejects date arithmetic in `$add` and `$subtract` of a date held by a `Dynamic` field;
# - rejects `$divide` and `$pow` of a decimal held by a `Dynamic` field, and a power of integers
#   held by it that a double may not hold exactly;
# - rejects zero raised to a negative exponent in `$pow`;
# - rejects a constant as the argument of `$not`, which leaves the data unchanged;
# - checks an update of a temporary table against the temporary table, not a namesake in the
#   current database.

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

user="user_05627_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_dyn_ops;
    DROP TABLE IF EXISTS mongo_dyn_num;
    DROP TABLE IF EXISTS mongo_dyn_dec;
    DROP TABLE IF EXISTS mongo_pow_zero;
    DROP TABLE IF EXISTS mongo_not;
    DROP TABLE IF EXISTS mongo_tmp_update;
    DROP USER IF EXISTS ${user};
    CREATE TABLE mongo_dyn_ops (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dyn_ops SELECT 1, fromUnixTimestamp64Milli(1000, 'UTC');
    INSERT INTO mongo_dyn_ops SELECT 2, 3::Int64;
    INSERT INTO mongo_dyn_ops SELECT 3, toDecimal128('5', 0);
    INSERT INTO mongo_dyn_ops SELECT 4, 2.5::Float64;
    CREATE TABLE mongo_dyn_num (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dyn_num SELECT 2, 3::Int64;
    INSERT INTO mongo_dyn_num SELECT 4, 2.5::Float64;
    CREATE TABLE mongo_dyn_dec (id Int32, v Dynamic) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dyn_dec SELECT 3, toDecimal128('5', 0);
    CREATE TABLE mongo_pow_zero (id Int32, b Float64, e Float64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_pow_zero VALUES (1, 0, -1);
    CREATE TABLE mongo_not (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_not VALUES (1, 1), (2, 2);
    CREATE TABLE mongo_tmp_update (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_tmp_update VALUES (1, 1);
    CREATE USER ${user};
    GRANT SELECT(id) ON ${CLICKHOUSE_DATABASE}.mongo_tmp_update TO ${user};
    GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user};
    GRANT TABLE ENGINE ON MergeTree TO ${user};
"

echo '-- a date held by a Dynamic in $add and $subtract'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "s" : {"$add" : ["$v", 1]}}}]);'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "s" : {"$add" : [1, 2, "$v"]}}}]);'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"_id" : 0, "s" : {"$subtract" : ["$v", 1]}}}]);'
mongo --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 2}}, {"$project" : {"_id" : 0, "s" : {"$add" : ["$v", 1]}, "t" : {"$subtract" : ["$v", 1]}, "u" : {"$add" : ["$v", 1, 2]}}}]);'

echo '-- a decimal held by a Dynamic in $divide'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 3}}, {"$project" : {"_id" : 0, "q" : {"$divide" : ["$v", 2]}}}]);'
# The condition of `$expr` is evaluated for every row of a block read, not only for the ones a
# condition on `id` would select, so the queries with `$expr` read tables of a single kind of value:
# otherwise the date or the decimal of another row fails the query first.
err --query 'db.mongo_dyn_dec.find({"$expr" : {"$eq" : [{"$divide" : ["$v", 2]}, 2.5]}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 2}}, {"$project" : {"_id" : 0, "q" : {"$divide" : ["$v", 2]}}}]);'
mongo --query 'db.mongo_dyn_num.find({"$expr" : {"$eq" : [{"$divide" : ["$v", 2]}, 1.5]}}, {"_id" : 0, "id" : 1});'

echo '-- $pow of a Dynamic'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 3}}, {"$project" : {"_id" : 0, "p" : {"$pow" : ["$v", 2]}}}]);'
err --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : 2}}, {"$project" : {"_id" : 0, "p" : {"$pow" : ["$v", 34]}}}]);'
mongo --query 'db.mongo_dyn_ops.aggregate([{"$match" : {"id" : {"$in" : [2, 4]}}}, {"$sort" : {"id" : 1}}, {"$project" : {"_id" : 0, "p" : {"$pow" : ["$v", 3]}, "q" : {"$pow" : [2, "$v"]}}}]);'

echo '-- zero to a negative exponent'
err --query 'db.mongo_pow_zero.aggregate([{"$project" : {"_id" : 0, "p" : {"$pow" : [0.0, -1.0]}}}]);'
err --query 'db.mongo_pow_zero.aggregate([{"$project" : {"_id" : 0, "p" : {"$pow" : ["$b", "$e"]}}}]);'
mongo --query 'db.mongo_pow_zero.aggregate([{"$project" : {"_id" : 0, "a" : {"$pow" : [null, -1.0]}, "b" : {"$pow" : [0.0, null]}, "c" : {"$pow" : [2.0, -1.0]}, "d" : {"$pow" : ["$b", 2.0]}, "e" : {"$pow" : [0, 0]}}}]);'

echo '-- a constant as the argument of $not'
err --query 'db.mongo_not.deleteMany({"x" : {"$not" : 1}});'
err --query 'db.mongo_not.deleteMany({"x" : {"$not" : {"$numberLong" : "1"}}});'
err --query 'db.mongo_not.updateMany({"x" : {"$not" : 1}}, {"$set" : {"x" : 5}});'
err --query 'db.mongo_not.updateMany({"x" : {"$not" : {}}}, {"$set" : {"x" : 5}});'
err --query 'db.mongo_not.find({"x" : {"$not" : {"$gt" : 1, "y" : 1}}});'
mongo --query 'db.mongo_not.find({"x" : {"$not" : {"$gt" : 1}}}, {"_id" : 0, "id" : 1});'
mongo --query 'db.mongo_not.find({}, {"_id" : 0, "id" : 1, "x" : 1}).sort({"id" : 1});'

echo '-- an update of a temporary table with a namesake in the current database'
# With parallel replicas, reading a temporary table logs a warning that the other replicas do not
# have it, so only errors are sent to the client.
${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=error --mutations_sync 2 --query "
    CREATE TEMPORARY TABLE mongo_tmp_update (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_tmp_update VALUES (1, 1);
    SET enable_mongo_dialect = 1;
    SET dialect = 'mongo';
    db.mongo_tmp_update.updateMany({}, {\"\$set\" : {\"x\" : 2}});
    SET dialect = 'clickhouse';
    SELECT id, x FROM mongo_tmp_update;
"
# The namesake is still denied.
err --user "${user}" --query 'db.mongo_tmp_update.updateMany({}, {"$set" : {"x" : 3}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM mongo_tmp_update"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_dyn_ops;
    DROP TABLE mongo_dyn_num;
    DROP TABLE mongo_dyn_dec;
    DROP TABLE mongo_pow_zero;
    DROP TABLE mongo_not;
    DROP TABLE mongo_tmp_update;
    DROP USER ${user};
"

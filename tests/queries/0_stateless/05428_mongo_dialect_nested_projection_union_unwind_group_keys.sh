#!/usr/bin/env bash
# What a Mongo pipeline returns follows MongoDB:
# - an inclusion projection of a parent path keeps a sort key under it, so an order-sensitive
#   `$group` after it can still read the documents in that order;
# - the nested form of a projection, `{"profile": {"age": 1}}`, keeps or removes the field, in
#   `aggregate` and in `find`, while in `$set` the same document assigns a value;
# - `$unionWith` appends documents with their own field names: the branches are aligned by name
#   when the pipeline names the fields of both, two collections read as they are must have the
#   same fields in the same order, and the query is rejected otherwise;
# - `$sortByCount` orders the groups, even when the source has fields named `count` and `_id`;
# - `$unwind` with `preserveNullAndEmptyArrays` keeps a document with an empty array without an
#   element value and without an index, rather than with the default value of the element type;
# - a number as the `_id` of a `$group` or the key of `$sortByCount` is a value, not a position.
#
# Each failing query runs on its own rather than in a `.sql` file with `-- { serverError ... }`
# hints: a comment is part of the query text in the Mongo dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_profiles;
    DROP TABLE IF EXISTS mongo_counts;
    DROP TABLE IF EXISTS mongo_arrays;
    DROP TABLE IF EXISTS mongo_union_x;
    DROP TABLE IF EXISTS mongo_union_x2;
    DROP TABLE IF EXISTS mongo_union_y;
    DROP TABLE IF EXISTS mongo_union_xy;
    DROP TABLE IF EXISTS mongo_union_yx;
    CREATE TABLE mongo_profiles (id Int32, \`profile.name\` String, \`profile.age\` Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_profiles VALUES (1, 'alpha', 40), (2, 'beta', 30), (3, 'gamma', 50);
    CREATE TABLE mongo_counts (kind String, count Int32, _id Int32) ENGINE = MergeTree ORDER BY kind;
    INSERT INTO mongo_counts VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30), ('c', 4, 40), ('c', 5, 50), ('c', 6, 60);
    CREATE TABLE mongo_arrays (id Int32, a Array(Int32), nested Array(Array(Int32))) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_arrays VALUES (1, [], []), (2, [5, 6], [[1]]);
    CREATE TABLE mongo_union_x (x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mongo_union_x VALUES (1);
    CREATE TABLE mongo_union_x2 (x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mongo_union_x2 VALUES (3);
    CREATE TABLE mongo_union_y (y Int32) ENGINE = MergeTree ORDER BY y;
    INSERT INTO mongo_union_y VALUES (2);
    CREATE TABLE mongo_union_xy (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mongo_union_xy VALUES (1, 2);
    CREATE TABLE mongo_union_yx (y Int32, x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mongo_union_yx VALUES (20, 10);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the name of the error of a query.
err() {
    mongo "$1" 2>&1 >/dev/null | grep -oE '\([A-Z_]+\)$' | head -1
}

echo '-- an inclusion of a parent path keeps the sort key under it'
mongo 'db.mongo_profiles.aggregate([{"$sort" : {"profile.age" : 1}}, {"$project" : {"profile" : 1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$profile.name"}, "last" : {"$last" : "$profile.name"}}}]);'

echo '-- nested projection flags'
mongo 'db.mongo_profiles.aggregate([{"$project" : {"profile" : {"age" : 1}}}, {"$sort" : {"profile.age" : 1}}]);'
mongo 'db.mongo_profiles.aggregate([{"$project" : {"profile" : {"age" : 0}}}, {"$sort" : {"id" : 1}}]);'
mongo 'db.mongo_profiles.aggregate([{"$project" : {"id" : 1, "profile" : {"next" : {"$add" : ["$profile.age", 1]}}}}, {"$sort" : {"id" : 1}}]);'
mongo 'db.mongo_profiles.aggregate([{"$set" : {"profile" : {"age" : 1}}}, {"$sort" : {"id" : 1}}]);'
mongo 'db.mongo_profiles.find({}, {"profile" : {"age" : 1}}).sort({"profile.age" : 1});'
mongo 'db.mongo_profiles.find({}, {"profile" : {"name" : 0}}).sort({"id" : 1});'
err 'db.mongo_profiles.aggregate([{"$project" : {"profile" : {"age" : 1, "name" : 0}}}]);'
err 'db.mongo_profiles.aggregate([{"$project" : {"profile" : {}}}]);'
err 'db.mongo_profiles.find({}, {"profile" : {}});'

echo '-- $unionWith aligns the branches by field name or is rejected'
mongo 'db.mongo_union_xy.aggregate([{"$unionWith" : "mongo_union_xy"}]);'
mongo 'db.mongo_union_x.aggregate([{"$unionWith" : "mongo_union_x2"}, {"$sort" : {"x" : 1}}]);'
mongo 'db.mongo_union_xy.aggregate([{"$project" : {"x" : {"$add" : ["$x", 0]}, "y" : {"$add" : ["$y", 0]}}}, {"$unionWith" : {"coll" : "mongo_union_yx", "pipeline" : [{"$project" : {"y" : {"$add" : ["$y", 0]}, "x" : {"$add" : ["$x", 0]}}}]}}, {"$sort" : {"x" : 1}}]);'
mongo 'db.mongo_union_x.aggregate([{"$group" : {"_id" : "$x", "n" : {"$sum" : 1}}}, {"$unionWith" : {"coll" : "mongo_union_y", "pipeline" : [{"$group" : {"_id" : "$y", "n" : {"$sum" : 1}}}, {"$project" : {"n" : 1, "_id" : 1}}]}}, {"$sort" : {"_id" : 1}}]);'
err 'db.mongo_union_x.aggregate([{"$unionWith" : "mongo_union_y"}]);'
err 'db.mongo_union_xy.aggregate([{"$unionWith" : "mongo_union_yx"}]);'
err 'db.mongo_union_x.aggregate([{"$unionWith" : {"coll" : "mongo_union_y", "pipeline" : [{"$project" : {"x" : "$y"}}]}}]);'
err 'db.mongo_union_x.aggregate([{"$group" : {"_id" : "$x"}}, {"$unionWith" : {"coll" : "mongo_union_y", "pipeline" : [{"$group" : {"_id" : "$y", "n" : {"$sum" : 1}}}]}}]);'

echo '-- $sortByCount with source fields named count and _id'
mongo 'db.mongo_counts.aggregate([{"$sortByCount" : "$kind"}]);'
mongo 'db.mongo_counts.aggregate([{"$sortByCount" : "$kind"}, {"$group" : {"_id" : null, "top" : {"$first" : "$_id"}}}]);'

echo '-- $unwind with preserveNullAndEmptyArrays does not fabricate an element'
mongo 'db.mongo_arrays.aggregate([{"$unwind" : {"path" : "$a", "preserveNullAndEmptyArrays" : true}}, {"$project" : {"id" : 1, "v" : {"$ifNull" : ["$a", 99]}}}, {"$sort" : {"id" : 1, "v" : 1}}]);'
mongo 'db.mongo_arrays.aggregate([{"$unwind" : {"path" : "$a", "preserveNullAndEmptyArrays" : true, "includeArrayIndex" : "i"}}, {"$project" : {"id" : 1, "a" : 1, "i" : 1}}, {"$sort" : {"id" : 1, "i" : 1}}]);'
mongo 'db.mongo_arrays.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "i"}}, {"$project" : {"id" : 1, "a" : 1, "i" : 1}}, {"$sort" : {"id" : 1, "i" : 1}}]);'
err 'db.mongo_arrays.aggregate([{"$unwind" : {"path" : "$nested", "preserveNullAndEmptyArrays" : true}}]);'

echo '-- a numeric group key is a value, not a position'
mongo 'db.mongo_counts.aggregate([{"$group" : {"_id" : 42, "n" : {"$sum" : 1}}}]);'
mongo 'db.mongo_counts.aggregate([{"$group" : {"_id" : 2, "n" : {"$sum" : 1}}}]);'
mongo 'db.mongo_counts.aggregate([{"$match" : {"kind" : "z"}}, {"$group" : {"_id" : 42, "n" : {"$sum" : 1}}}]);'
mongo 'db.mongo_counts.aggregate([{"$sortByCount" : 5}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_profiles;
    DROP TABLE mongo_counts;
    DROP TABLE mongo_arrays;
    DROP TABLE mongo_union_x;
    DROP TABLE mongo_union_x2;
    DROP TABLE mongo_union_y;
    DROP TABLE mongo_union_xy;
    DROP TABLE mongo_union_yx;
"

#!/usr/bin/env bash
# The Mongo dialect:
# - skips a `LowCardinality(Bool)` column in `$sum`, `$avg`, `$stdDevPop` and `$stdDevSamp`;
# - matches `null` and a missing value with `$lte: null` and `$gte: null`, and nothing with
#   `$lt: null` and `$gt: null`, also under `$not` and in a delete;
# - does not match a fraction, a boolean or a number out of the signed 64-bit range with a `$bits*`
#   test, also in a delete;
# - resolves the fields of the filter, the sort and the projection to the stored columns, not to
#   the computed fields of the projection with the same names;
# - parses a number with full precision.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --output_format_json_quote_64bit_integers 0 --format JSONEachRow "$@"
}

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_t;
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE TABLE mongo_t (id Int32, a Int32, v Nullable(Int32), f Float64, b Bool, lb LowCardinality(Bool)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_t VALUES (1, 7, NULL, 1.5, true, true), (2, 3, 1, 3, false, false), (3, 5, 2, 1e19, true, true);
"

echo '-- LowCardinality(Bool) in numeric accumulators'
mongo --query 'db.mongo_t.aggregate([{"$group" : {"_id" : null, "s" : {"$sum" : "$lb"}, "a" : {"$avg" : "$lb"}, "p" : {"$stdDevPop" : "$lb"}, "q" : {"$stdDevSamp" : "$lb"}}}]);'

echo '-- range comparisons with null'
mongo --query 'db.mongo_t.find({"v" : {"$lte" : null}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$gte" : null}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$lt" : null}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$gt" : null}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$not" : {"$lte" : null}}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"v" : {"$not" : {"$lt" : null}}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

echo '-- bit tests'
mongo --query 'db.mongo_t.find({"f" : {"$bitsAllSet" : 1}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"f" : {"$bitsAnyClear" : 4}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"f" : {"$bitsAllClear" : 4}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"b" : {"$bitsAnySet" : 1}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"a" : {"$bitsAllSet" : [0, 1]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'
mongo --query 'db.mongo_t.find({"a" : {"$bitsAllClear" : [63]}}, {"_id" : 0, "id" : 1}).sort({"id" : 1});'

echo '-- computed projection fields named as stored fields'
mongo --query 'db.mongo_t.find({"id" : 1}, {"_id" : 0, "a" : {"$literal" : 99}, "original" : "$a"});'
mongo --query 'db.mongo_t.find({"a" : 7}, {"_id" : 0, "a" : {"$literal" : 99}});'
mongo --query 'db.mongo_t.find({}, {"_id" : 0, "id" : 1, "a" : {"$subtract" : [0, "$a"]}}).sort({"a" : 1});'

echo '-- full precision numbers'
mongo --query 'db.mongo_t.insertOne({"id" : 4, "a" : 0, "f" : 0.013697458904123693477});' > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT f = 0.013697458904123693 FROM mongo_t WHERE id = 4"
mongo --query 'db.mongo_t.find({"f" : 0.013697458904123693477}, {"_id" : 0, "id" : 1});'

echo '-- deletes'
mongo --mutations_sync 2 --query 'db.mongo_t.deleteMany({"v" : {"$not" : {"$lte" : null}}});' > /dev/null
mongo --mutations_sync 2 --query 'db.mongo_t.deleteMany({"f" : {"$bitsAllSet" : 1}});' > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT id FROM mongo_t ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_t"

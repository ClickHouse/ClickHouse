-- The numeric Extended JSON wrappers `$numberInt`, `$numberLong` and `$numberDouble` nested inside
-- an embedded document are converted into the numbers they wrap, the same way a direct value of a
-- field is. They used to be an error there, while `$oid`, `$numberDecimal` and `$date` worked.
-- Both the canonical form (a string) and the relaxed one (a number) are accepted.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS numeric_wrappers;
CREATE TABLE numeric_wrappers (id Int64, events Array(JSON)) ENGINE = MergeTree ORDER BY id;

SET enable_mongo_dialect = 1;
SET mutations_sync = 1;
SET dialect='mongo';

db.numeric_wrappers.insertOne({"id": 1, "events": [{"n": {"$numberLong": "5000000000"}}]});
db.numeric_wrappers.insertOne({"id": 2, "events": [{"i": {"$numberInt": "-7"}, "d": {"$numberDouble": "1.5"}, "deep": {"list": [{"$numberLong": 9007199254740993}]}}]});
db.numeric_wrappers.insertOne({"id": 3, "events": []});
db.numeric_wrappers.updateMany({"id": 3}, {"$push": {"events": {"n": {"$numberLong": "-5000000000"}}}});
db.numeric_wrappers.updateMany({"id": 3}, {"$addToSet": {"events": {"i": {"$numberInt": 42}}}});

SET dialect='clickhouse';

-- The paths say that no wrapper is left as an `n.$numberLong` path, and the types that the numbers
-- are stored as numbers rather than as the text of the canonical form.
SELECT id, arrayMap(event -> JSONAllPathsWithTypes(event), events) FROM numeric_wrappers ORDER BY id;

SELECT id, events FROM numeric_wrappers ORDER BY id FORMAT JSONEachRow;

SET dialect='mongo';

db.numeric_wrappers.aggregate([{"$match": {"id": 1}}, {"$project": {"_id": 0, "doc": {"$literal": {"n": {"$numberLong": "5000000000"}, "d": {"$numberDouble": "2.5"}}}}}]);

SET dialect='clickhouse';

DROP TABLE numeric_wrappers;

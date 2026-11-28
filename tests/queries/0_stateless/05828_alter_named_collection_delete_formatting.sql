-- The parser reads each key of `DELETE` as one identifier.
-- The formatted query must keep the quotes of a key that is not a plain identifier, otherwise it is not parseable.
-- The second column shows that the formatted query is parseable and that the format is stable.

SELECT formatQuery('ALTER NAMED COLLECTION c DELETE `kafka.ssl_key_location`') AS f, formatQuery(f) = f;
SELECT formatQuery('ALTER NAMED COLLECTION c DELETE a, `b.c`, `d e`, `f.g h`, `i``j`') AS f, formatQuery(f) = f;
SELECT formatQuery('ALTER NAMED COLLECTION IF EXISTS c ON CLUSTER test_shard_localhost DELETE `a.b`, c') AS f, formatQuery(f) = f;

-- The keys of `SET` accept a compound identifier. Their format does not change.
SELECT formatQuery('ALTER NAMED COLLECTION c SET `a.b` = 1, c.d = 2 DELETE `e.f`') AS f, formatQuery(f) = f;

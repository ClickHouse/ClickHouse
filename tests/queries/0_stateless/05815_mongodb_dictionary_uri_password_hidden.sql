-- Tags: no-fasttest
-- no-fasttest: the MongoDB dictionary source needs USE_MONGODB

-- The password in the `uri` of a `MongoDB` dictionary source is hidden in `system.tables` and `system.query_log`.

SET format_display_secrets_in_show_and_select = 0;

DROP DICTIONARY IF EXISTS d_mongo;

CREATE DICTIONARY d_mongo (id UInt64, v String) PRIMARY KEY id
SOURCE(MONGODB(URI 'mongodb://admin:SEKRIT_PW@localhost:27017/db' COLLECTION 'c'))
LIFETIME(0) LAYOUT(FLAT());

SELECT extract(create_table_query, 'SOURCE.*\\)\\)')
FROM system.tables WHERE database = currentDatabase() AND name = 'd_mongo';

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(query LIKE concat('%', 'SEKRIT', '%'))
FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create' AND type = 'QueryFinish' AND event_date >= yesterday();

DROP DICTIONARY d_mongo;

-- The `url` and `endpoint` of an `HTTP` dictionary source are hidden whole in `SHOW CREATE DICTIONARY`,
-- `system.tables` and `system.query_log`.

SET format_display_secrets_in_show_and_select = 0;

DROP DICTIONARY IF EXISTS d_url;
DROP DICTIONARY IF EXISTS d_endpoint;
DROP DICTIONARY IF EXISTS d_plain;
DROP DICTIONARY IF EXISTS d_token;

CREATE DICTIONARY d_url (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(URL 'http://admin:SEKRIT_PW@localhost:11111/data.csv?access_token=SEKRIT_TOKEN&x=1' FORMAT 'CSV'))
LIFETIME(0) LAYOUT(FLAT());

CREATE DICTIONARY d_endpoint (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(ENDPOINT 'http://admin:SEKRIT_PW@localhost:11111/data.csv?sig=SEKRIT_SIG' FORMAT 'CSV'))
LIFETIME(0) LAYOUT(FLAT());

CREATE DICTIONARY d_plain (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(URL 'http://localhost:11111/data.csv?x=1' FORMAT 'CSV'))
LIFETIME(0) LAYOUT(FLAT());

CREATE DICTIONARY d_token (id UInt64, v String) PRIMARY KEY id
SOURCE(HTTP(URL 'https://SEKRIT_TOKEN@localhost:11111/data.csv' FORMAT 'CSV'))
LIFETIME(0) LAYOUT(FLAT());

SHOW CREATE DICTIONARY d_url;

SELECT name, extract(create_table_query, 'SOURCE.*\\)\\)')
FROM system.tables WHERE database = currentDatabase() AND name IN ('d_url', 'd_endpoint', 'd_plain', 'd_token') ORDER BY name;

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(query LIKE concat('%', 'SEKRIT', '%'))
FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create' AND type = 'QueryFinish' AND event_date >= yesterday();

DROP DICTIONARY d_url;
DROP DICTIONARY d_endpoint;
DROP DICTIONARY d_plain;
DROP DICTIONARY d_token;

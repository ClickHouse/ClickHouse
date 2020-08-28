CREATE DATABASE IF NOT EXISTS db_dict;
DROP DICTIONARY IF EXISTS db_dict.cache_hits;

CREATE DICTIONARY db_dict.cache_hits 
(WatchID UInt64, UserID UInt64, SearchPhrase String) 
PRIMARY KEY WatchID 
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'hits' PASSWORD '' DB 'test')) 
LIFETIME(MIN 1 MAX 10) 
LAYOUT(CACHE(SIZE_IN_CELLS 1 QUERY_WAIT_TIMEOUT_MILLISECONDS 60000));

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr 
FROM test.hits PREWHERE WatchID % 5 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr 
FROM test.hits PREWHERE WatchID % 7 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr 
FROM test.hits PREWHERE WatchID % 13 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

DROP DICTIONARY IF EXISTS db_dict.cache_hits;
DROP DATABASE IF  EXISTS db_dict;

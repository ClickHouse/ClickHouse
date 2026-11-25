-- Tags: no-fasttest
-- no-fasttest: the MySQL and PostgreSQL integrations are not available in the fast test build.

-- The queries of a MySQL, PostgreSQL or ODBC dictionary source (`query`, `where`, `update_field`, `invalidate_query`)
-- must be `SELECT`, without what the database may read differently from ClickHouse.
-- No database server is needed: the queries are checked by `CREATE`, before any connection. The sources point
-- at `127.0.0.1` port 1, where nothing listens, so an accepted source ends in a connection failure at
-- `SYSTEM RELOAD DICTIONARY`. The privileges are tested in `05871_dictionary_external_source_queries_select_only`.

SELECT '--- Statements other than SELECT are rejected';
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'CREATE TABLE t2 (x UInt8) ENGINE = Memory'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t INTO OUTFILE ''/tmp/x'''))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' WHERE '1; DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' UPDATE_FIELD 'x >= 0; DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError SYNTAX_ERROR }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DELETE FROM t WHERE id = 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- The query is a constant expression.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY concat('DROP', ' TABLE t')))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }

SELECT '--- What the database reads differently from ClickHouse is rejected';
-- MySQL executes the text of `/*! ... */`.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY '/*!CREATE TABLE t2 AS */ SELECT 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- MySQL does not read `--` without a following space as a comment.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'SELECT 1 --1 INTO OUTFILE ''/tmp/x'''))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' WHERE 'id > 0 /*!*/'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT $$a$$ AS id, value FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'SELECT 1 AS ‘, 2 INTO OUTFILE ''/tmp/x'' ’'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- PostgreSQL reads a backslash in a string literally.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' UPDATE_FIELD 'value = ''\\'' OR updated'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- SQL Server reads `[']` as an identifier in brackets, and the rest as more statements.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'SELECT [''] = 1 DROP TABLE victim SELECT [''] = 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- ClickHouse reads the words after `FORMAT` and `SETTINGS` itself, a database can read them as another statement.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'SELECT 1 FORMAT TSV'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY 'SELECT 1 SETTINGS max_threads = 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- SQL Server reads `SELECT 1 SHUTDOWN` as `SELECT 1` and `SHUTDOWN`, ClickHouse reads `SHUTDOWN` as an alias.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'SELECT 1 SHUTDOWN'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'SELECT 1 FROM t SHUTDOWN'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'SELECT "SHUTDOWN" FROM t SHUTDOWN'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'SELECT 1 AS SHUTDOWN, 2 SHUTDOWN'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }

SELECT '--- {condition} in quotes is rejected';
-- The source replaces `{condition}` with a condition on the keys that contains quotes: in a string literal,
-- `SELECT ''{condition}'' AS id` would become `SELECT ''("id"='' AS ... '')'' AS id`, and the key would be read as SQL.
CREATE DICTIONARY d (id String, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' SCHEMA 'dbo' QUERY 'SELECT ''{condition}'' AS id, '''' AS value'))
    LAYOUT(COMPLEX_KEY_DIRECT()); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t WHERE {condition} AND value = ''{condition}'''))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t WHERE "{condition}"'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }

SELECT '--- Every way to create a dictionary from a definition is checked';
SET distributed_ddl_output_mode = 'none';
CREATE DICTIONARY d ON CLUSTER test_shard_localhost (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
-- With an old format of the DDL entry, older versions send the query to the hosts before normalizing it.
SET distributed_ddl_entry_format_version = 2;
CREATE DICTIONARY d ON CLUSTER test_shard_localhost (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
SET distributed_ddl_entry_format_version = DEFAULT;
CREATE OR REPLACE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1); -- { serverError INCORRECT_QUERY }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'd';

SELECT '--- A source replaced by Null is not checked';
-- `restore_replace_external_dictionary_source_to_null` replaces the source before the checks: it sends nothing.
SET restore_replace_external_dictionary_source_to_null = 1;
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT /* hint */ id, value FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SELECT create_table_query LIKE '%SOURCE(NULL%', create_table_query LIKE '%hint%'
FROM system.tables WHERE database = currentDatabase() AND name = 'd';
DROP DICTIONARY d;
SET restore_replace_external_dictionary_source_to_null = 0;

SELECT '--- SELECT is accepted';
CREATE DICTIONARY d (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t WHERE {condition}' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError ALL_CONNECTION_TRIES_FAILED }
DROP DICTIONARY d;
CREATE DICTIONARY d (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
    SOURCE(MYSQL(HOST concat('127.0.0.1', '') PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' INVALIDATE_QUERY concat('SELECT ', '1')))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError ALL_CONNECTION_TRIES_FAILED }
DROP DICTIONARY d;
CREATE DICTIONARY d (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' WHERE 'id > 0' UPDATE_FIELD 'updated' INVALIDATE_QUERY 'SELECT max(updated) FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError POSTGRESQL_CONNECTION_FAILURE }
DROP DICTIONARY d;
-- Without a load: an ODBC source needs the bridge.
CREATE DICTIONARY d (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' TABLE 't' INVALIDATE_QUERY 'WITH c AS (SELECT max(ts) AS m FROM t) SELECT m AS "x", "m" AS y FROM c AS c2 ORDER BY m'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
DROP DICTIONARY d;
-- `{condition}` outside quotes is replaced by a condition on the keys, which stays one expression.
CREATE DICTIONARY d (id String, value String) PRIMARY KEY id
    SOURCE(ODBC(CONNECTION_STRING 'DSN=none' SCHEMA 'dbo' QUERY 'SELECT id AS id, value AS value FROM t WHERE {condition}'))
    LAYOUT(COMPLEX_KEY_DIRECT());
DROP DICTIONARY d;

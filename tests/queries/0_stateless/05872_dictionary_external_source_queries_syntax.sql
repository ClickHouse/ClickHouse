-- Tags: no-fasttest
-- no-fasttest: the MySQL and PostgreSQL integrations are not available in the fast test build.

-- A MySQL, PostgreSQL or ODBC dictionary created by `CREATE DICTIONARY` sends only `SELECT` to the database: the query
-- is checked when it is sent, before any connection. An ODBC source needs the bridge to be created, so it is not tested here. The sources point at `127.0.0.1` port 1, where nothing listens,
-- so a checked query ends in a connection failure. The invalidate query is tested in `05871_dictionary_external_source_queries_select_only`.

SELECT '--- Statements other than SELECT are rejected';
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DELETE FROM t WHERE id = 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
-- `where` is a part of the composed query, so a second statement in it is rejected too.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' WHERE '1; DROP TABLE t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError SYNTAX_ERROR }
DROP DICTIONARY d;

SELECT '--- What the database reads differently from ClickHouse is rejected';
-- MySQL executes the text of `/*! ... */`.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t /*! INTO OUTFILE ''/tmp/x'' */'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
-- MySQL writes the file of `INTO OUTFILE` on its host.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t INTO OUTFILE ''/tmp/x'''))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
-- PostgreSQL reads a backslash in a string literally, so `'\\'` is two backslashes for it and one for ClickHouse.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, ''\\\\'' AS value FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
-- `FORMAT` and `SETTINGS` are a part of a `SELECT` only for ClickHouse.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t FORMAT TSV'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t SETTINGS max_threads = 1'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;

SELECT '--- The load query of a lookup is checked';
-- A `direct` layout never loads all, its lookups add a condition on the keys to the load query.
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'DELETE FROM t WHERE {condition}'))
    LAYOUT(DIRECT());
SELECT dictGet(d, 'value', toUInt64(1)); -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;
-- In a string literal, the quotes of the condition on the keys would end the literal, and a key would be read as SQL.
CREATE DICTIONARY d (id String, value String) PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT ''{condition}'' AS id, '''' AS value'))
    LAYOUT(COMPLEX_KEY_DIRECT());
SELECT dictGet(d, 'value', tuple('x')); -- { serverError INCORRECT_QUERY }
DROP DICTIONARY d;

SELECT '--- SELECT is sent';
CREATE DICTIONARY d (id UInt64, value String) PRIMARY KEY id
    SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' QUERY 'SELECT id, value FROM t WHERE {condition}' INVALIDATE_QUERY 'SELECT max(updated) FROM t'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError ALL_CONNECTION_TRIES_FAILED }
DROP DICTIONARY d;
CREATE DICTIONARY d (id UInt64, value String DEFAULT '' EXPRESSION 'concat(value, ''x'')') PRIMARY KEY id
    SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' WHERE 'id > 0' UPDATE_FIELD 'updated'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 1);
SYSTEM RELOAD DICTIONARY d; -- { serverError POSTGRESQL_CONNECTION_FAILURE }
DROP DICTIONARY d;

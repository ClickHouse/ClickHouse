-- Tags: no-fasttest
-- no-fasttest: the MySQL integration is not available in the fast test build.

-- A dictionary created with a DDL query may not set `socket`, at the source or at a replica: for `localhost`
-- the connection goes to that Unix socket, while `remote_url_allow_hosts` checks only `host` and `port`.
-- No MySQL server is needed: the source is rejected when the dictionary is loaded, before any connection.

CREATE DICTIONARY d_socket (id UInt64, v String) PRIMARY KEY id
SOURCE(MYSQL(HOST 'localhost' PORT 3306 SOCKET '/nonexistent/mysql.sock' USER 'u' PASSWORD 'p' DB 'd' TABLE 't'))
LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_socket; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY d_socket;

CREATE DICTIONARY d_socket_inherited (id UInt64, v String) PRIMARY KEY id
SOURCE(MYSQL(USER 'u' PASSWORD 'p' DB 'd' TABLE 't' PORT 3306 SOCKET '/nonexistent/mysql.sock' REPLICA(PRIORITY 1 HOST 'localhost')))
LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_socket_inherited; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY d_socket_inherited;

CREATE DICTIONARY d_socket_replica (id UInt64, v String) PRIMARY KEY id
SOURCE(MYSQL(USER 'u' PASSWORD 'p' DB 'd' TABLE 't' PORT 3306 REPLICA(PRIORITY 1 HOST '127.0.0.1') REPLICA(PRIORITY 2 HOST 'localhost' SOCKET '/nonexistent/mysql.sock')))
LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_socket_replica; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY d_socket_replica;

-- Also next to a named collection: `socket` is rejected before the collection is looked up.
CREATE DICTIONARY d_socket_named (id UInt64, v String) PRIMARY KEY id
SOURCE(MYSQL(NAME nc_05452_absent SOCKET '/nonexistent/mysql.sock'))
LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_socket_named; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY d_socket_named;

-- Without `socket` the source is accepted and reaches a connection attempt.
CREATE DICTIONARY d_no_socket (id UInt64, v String) PRIMARY KEY id
SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't'))
LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_no_socket; -- { serverError ALL_CONNECTION_TRIES_FAILED }
DROP DICTIONARY d_no_socket;

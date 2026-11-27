-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: requires a build with MongoDB support
-- Tag no-parallel: creates server-global named collections with fixed names

DROP DICTIONARY IF EXISTS d_05876;
DROP TABLE IF EXISTS t_05876;
DROP NAMED COLLECTION IF EXISTS nc_dictionary_05876;
DROP NAMED COLLECTION IF EXISTS nc_dictionary_no_user_05876;
DROP NAMED COLLECTION IF EXISTS nc_table_05876;

-- A port that is not a number would add hosts to the seed list of the URI, which carries the stored credentials.
CREATE NAMED COLLECTION nc_dictionary_05876 AS
    host = 'localhost' NOT OVERRIDABLE, db = 'db' NOT OVERRIDABLE, collection = 'c' NOT OVERRIDABLE,
    user = 'u' NOT OVERRIDABLE, password = 'p' NOT OVERRIDABLE;

CREATE DICTIONARY d_05876 (id UInt64, value String) PRIMARY KEY id LAYOUT(FLAT()) LIFETIME(0)
SOURCE(MONGODB(NAME nc_dictionary_05876 PORT '1,localhost:27017'));
SELECT * FROM d_05876; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY d_05876;

CREATE NAMED COLLECTION nc_table_05876 AS
    host = 'localhost' NOT OVERRIDABLE, database = 'db' NOT OVERRIDABLE, collection = 'c' NOT OVERRIDABLE,
    user = 'u' NOT OVERRIDABLE, password = 'p' NOT OVERRIDABLE;

CREATE TABLE t_05876 (id UInt64) ENGINE = MongoDB(nc_table_05876, port = '1,localhost:27017'); -- { serverError BAD_ARGUMENTS }

-- A user with URI delimiters would replace the hosts and move the stored password out of the credentials.
CREATE NAMED COLLECTION nc_dictionary_no_user_05876 AS
    host = 'localhost' NOT OVERRIDABLE, port = 1 NOT OVERRIDABLE, db = 'db' NOT OVERRIDABLE, collection = 'c' NOT OVERRIDABLE,
    password = 'stored_password_05876' NOT OVERRIDABLE;

CREATE DICTIONARY d_05876 (id UInt64, value String) PRIMARY KEY id LAYOUT(FLAT()) LIFETIME(0)
SOURCE(MONGODB(NAME nc_dictionary_no_user_05876 USER 'x@localhost:2/'));
SELECT * FROM d_05876; -- { serverError DICTIONARIES_WAS_NOT_LOADED }
SELECT position(last_exception, 'stored_password_05876') FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_05876';
DROP DICTIONARY d_05876;

DROP NAMED COLLECTION nc_dictionary_05876;
DROP NAMED COLLECTION nc_dictionary_no_user_05876;
DROP NAMED COLLECTION nc_table_05876;

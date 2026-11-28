-- Tags: no-fasttest
-- Tag no-fasttest: connects to the server's own MySQL protocol port, which the fast test does not open.

-- The MySQL table engine decodes every value of the MySQL text protocol with the declared
-- ClickHouse column type, so a column declared Float64 is parsed by
-- mysqlxx::Value::getDouble -> mysqlxx::Value::readFloatText (src/Common/mysqlxx/Value.cpp).
-- Declaring a column of a different type than the remote one is documented in
-- docs/reference/engines/table-engines/integrations/mysql.mdx: "Column types may differ from
-- those in the original MySQL table. ClickHouse tries to cast values to the ClickHouse data
-- types." The renderings inf, -inf and nan of the decoded doubles are documented in
-- docs/reference/data-types/float.mdx ("NaN and Inf").

SET send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.

DROP TABLE IF EXISTS float_text_source;
DROP TABLE IF EXISTS float_text_over_mysql;
DROP TABLE IF EXISTS bad_float_source;
DROP TABLE IF EXISTS bad_float_over_mysql;

CREATE TABLE float_text_source (k UInt8, t String) ENGINE = Memory;

-- One row per spelling a MySQL server can send for a floating point value: a signed decimal,
-- a lower and an upper case exponent, a negative value with an exponent, the two infinities
-- and the two spellings of a not-a-number. Only values that a double holds exactly are used,
-- so no rounding is asserted.
INSERT INTO float_text_source VALUES (1, '+1.5'), (2, '-2.5'), (3, '1e3'), (4, '1E2'), (5, '-2e1'), (6, 'inf'), (7, '-inf'), (8, 'Infinity'), (9, 'nan'), (10, 'NaN');

CREATE TABLE float_text_over_mysql (k UInt8, t Float64)
ENGINE = MySQL('127.0.0.1:' || getServerPort('mysql_port'), currentDatabase(), 'float_text_source', 'default', '')
SETTINGS connect_timeout = 100, connection_wait_timeout = 100;

SELECT k, t FROM float_text_over_mysql ORDER BY k;

-- A value that is not a floating point number at all must fail instead of decoding to a number:
-- Value::readFloatText ends in Value::throwException, which raises mysqlxx::CannotParseValue,
-- a Poco::Exception, and the server reports it under the error code POCO_EXCEPTION.
CREATE TABLE bad_float_source (t String) ENGINE = Memory;
INSERT INTO bad_float_source VALUES ('abc');

CREATE TABLE bad_float_over_mysql (t Float64)
ENGINE = MySQL('127.0.0.1:' || getServerPort('mysql_port'), currentDatabase(), 'bad_float_source', 'default', '')
SETTINGS connect_timeout = 100, connection_wait_timeout = 100;

SELECT t FROM bad_float_over_mysql; -- { serverError POCO_EXCEPTION }

DROP TABLE float_text_over_mysql;
DROP TABLE float_text_source;
DROP TABLE bad_float_over_mysql;
DROP TABLE bad_float_source;

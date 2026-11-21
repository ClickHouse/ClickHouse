-- The `STREAM` grammar documented in src/Parsers/ASTStreamSettings.h produces the stream settings
-- only as `STREAM [BOUNDED] [UNORDERED] [CURSOR {...}] [WATERMARK FOR <col> AS <expr> [IDLE TIMEOUT
-- INTERVAL n SECOND]]`, and `writeJSON` emits the three watermark fields together. So `readJSON` must
-- round trip exactly that shape, and reject a payload the parser cannot produce with BAD_ARGUMENTS
-- rather than dropping the field, which would make the query hash and format as if it never had one.

-- The two flags round trip on their own.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM BOUNDED'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM UNORDERED'));

-- The whole clause at once: both flags, a cursor, and a watermark with an idle timeout.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t STREAM BOUNDED UNORDERED CURSOR {''a'': 10} WATERMARK FOR ts AS ts - 5 IDLE TIMEOUT INTERVAL 1 SECOND'));

-- A `cursor_tree` leaf above the Int64 range. `CURSOR` accepts only unsigned integer literals that
-- fit into Int64, and `buildCursorTree` reads the leaf as Int64.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': 10}'),
    '{"field_type":"Int64","value":10}',
    '{"field_type":"UInt64","value":9223372036854775808}')); -- { serverError BAD_ARGUMENTS }

-- An empty `watermark_column`. `WATERMARK FOR` always names a column.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM WATERMARK FOR ts AS ts'),
    '"watermark_column":"ts"',
    '"watermark_column":""')); -- { serverError BAD_ARGUMENTS }

-- A `watermark_column` without its expression. `WATERMARK FOR <col> AS <expr>` always produces both.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM WATERMARK FOR ts AS ts'),
    ',"watermark_expression":{"type":"Identifier","name":"ts"}',
    '')); -- { serverError BAD_ARGUMENTS }

-- A negative idle timeout. `IDLE TIMEOUT INTERVAL` never yields one.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT * FROM t STREAM WATERMARK FOR ts AS ts'),
    '"watermark_idle_timeout_ms":0',
    '"watermark_idle_timeout_ms":-1')); -- { serverError BAD_ARGUMENTS }

#!/usr/bin/env bash
# KQLLexer::lexString (src/Parsers/Kusto/KQLLexer.cpp): the escapes of a KQL string literal.
# The literal forms are the ones named in docs/guides/clickhouse/kusto-query-language.mdx
# ("Literals: strings (including verbatim @'...')"). The decoded bytes are shown with `hex`,
# because the TabSeparated output escapes a backslash and a quote on its own.
#
# The query text goes into the HTTP body unchanged, byte for byte: three of the branches run
# only when the text ends inside the literal, which a .sql file, always closed by a semicolon
# and a newline, cannot express.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&dialect=kusto&enable_kusto_dialect=1"

# $1: label, $2: the query text, sent as it is written here
kql_error()
{
    local code
    code=$(${CLICKHOUSE_CURL} -sS "$url" --data-binary "$2" | grep -o -m1 'SYNTAX_ERROR')
    echo "$1 ${code:-NO_ERROR}"
}

echo '-- a doubled quote in a verbatim string is one quote --'
${CLICKHOUSE_CURL} -sS "$url" --data-binary @- <<'EOF'
print verbatim = @'it''s', verbatim_hex = hex(@'it''s'), verbatim_len = strlen(@'it''s')
EOF

echo '-- the escapes for newline, return, tab, NUL and for the three quoted characters --'
${CLICKHOUSE_CURL} -sS "$url" --data-binary @- <<'EOF'
print controls = hex('\n\r\t\0'), quotes = hex('\\\'\"'), quotes_len = strlen('\\\'\"')
EOF

echo '-- a u-escape is encoded as UTF-8: one, two and three bytes --'
${CLICKHOUSE_CURL} -sS "$url" --data-binary @- <<'EOF'
print unicode = '\u0041\u00FC\u20AC', unicode_hex = hex('\u0041\u00FC\u20AC')
EOF

echo '-- an unknown escape keeps its backslash, so a regex survives a plain string --'
${CLICKHOUSE_CURL} -sS "$url" --data-binary @- <<'EOF'
print unknown = hex('\w+\d'), unknown_len = strlen('\w+\d')
EOF

echo '-- an unterminated literal and a broken u-escape are lexer errors --'
kql_error 'newline_in_literal' $'print \'a\nb\''
kql_error 'text_ends_in_literal' "print 'abc"
kql_error 'text_ends_at_backslash' "print 'abc\\"
kql_error 'text_ends_in_u_escape' "print 'x\\u00"
kql_error 'non_hex_in_u_escape' "print 'x\\uZZZZ'"

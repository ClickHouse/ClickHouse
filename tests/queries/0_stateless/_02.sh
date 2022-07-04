#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

compare () {
    if [ "$3" == 2 ];then
        R_1=$($CLICKHOUSE_CLIENT -q "EXPLAIN AST $1")
        R_2=$($CLICKHOUSE_CLIENT -q "EXPLAIN AST $2" 2>/dev/null)

        if [ "$R_1" == "$R_2" ];then
            echo "equal (RES): $1";
        else
            echo "============== not equal ==================="
            echo "not equal (RES): $1";
            echo "# Original: $R_1";
            echo "# Ours: $R_2";
            echo "============================================"
        fi
    fi

    if [ "$2" != 0 ];then
        R_1=$($CLICKHOUSE_CLIENT -q "SELECT $1")
        R_2=$($CLICKHOUSE_CLIENT -q "SELECT \$ $1" 2>/dev/null)

        if [ "$R_1" == "$R_2" ];then
            echo "equal (RES): SELECT $1";
        else
            echo "============== not equal ==================="
            echo "not equal (RES): SELECT $1";
            echo "# Original: $R_1";
            echo "# Ours: $R_2";
            echo "============================================"
        fi
    fi

    R_1=$($CLICKHOUSE_CLIENT -q "EXPLAIN AST SELECT $1")
    R_2=$($CLICKHOUSE_CLIENT -q "EXPLAIN AST SELECT \$ $1" 2>/dev/null)

    if [ "$R_1" == "$R_2" ];then
        echo "equal (AST): SELECT $1";
    else
        echo "============== not equal ==================="
        echo "not equal (AST): SELECT $1";
        echo "# Original: $R_1";
        echo "# Ours: $R_2";
        echo "============================================"
    fi
}

# compare "1 + 1"
# compare "3 + 7 * 5 + 32 / 2 - 5 * 2"
# compare "100 MOD 5 DIV 20 MOD 5"
# compare "1 + 2 * 3 - 3 / 2 < 80 / 8 + 2 * 5"
# compare "20 MOD 10 > 200 DIV 6"
# compare "5 != 80 / 8 + 2 * 5"

# compare "a.5" 0
# compare "a.b.5" 0
# compare "a.b.n.v" 0
# compare "10 * a.b.5 / 3" 0

# compare "-1::Int64"
# compare "[1,2,3]::Array(Int64)"
# compare "[1,2,cos(1)]"
# compare "[a,b,c]" 0
# compare "[a,b,c]::Array(UInt8)" 0


# compare "number AS a1, number AS b2, number FROM numbers(10)"
# compare "*[n]" 0

# compare "3 + 7 * (5 + 32) / 2 - 5 * (2 - 1)"
# compare "(a, b, c) * ((a, b, c) + (a, b, c))" 0

# compare "1 + 2 * 3 < a / b mod 5 OR [a, b, c] + 1 != [c, d, e] AND n as res" 0
# compare "1 + 2 * 3 < a / b mod 5 AND [a, b, c] + 1 != [c, d, e] OR n as res" 0

# compare "'needle' LIKE 'haystack' AND NOT needle NOT ILIKE haystack" 0
# compare "'needle' LIKE 'haystack' AND (NOT needle) NOT ILIKE haystack" 0

# compare "[1, 2, 3, cast(['a', 'b', c] as Array(String)), 4]" 0
# compare "[1, 2, 3, cast(['a', 'b', c], Array(String)), 4]" 0

# compare "[1, 2, 3, cast(['a', 'b', c] as Array(String)), 4]" 0
# compare "[1, 2, 3, cast(['a', 'b', c], Array(String)), 4]" 0

# compare "EXTRACT(DAY FROM toDate('2017-06-15'))"
# compare "substring(toFixedString('hello12345', 16) from 1 for 8)"
# compare "position('Hello, world!' IN '!')"

# compare "trim(TRAILING 'x' FROM 'xxfooxx')"
# compare "ltrim('') || rtrim('') || trim('')"

# compare "WITH 2 AS \`b.c\`, [4, 5] AS a, 6 AS u, 3 AS v, 2 AS d, TRUE AS e, 1 AS f, 0 AS g, 2 AS h, 'Hello' AS i, 'World' AS j, TIMESTAMP '2022-02-02 02:02:02' AS w, [] AS k, (1, 2) AS l, 2 AS m, 3 AS n, [] AS o, [1] AS p, 1 AS q, q AS r, 1 AS s, 1 AS t
# SELECT INTERVAL CASE CASE WHEN NOT -a[b.c] * u DIV v + d IS NOT NULL AND e OR f BETWEEN g AND h THEN i ELSE j END WHEN w THEN k END || [l, (m, n)] MINUTE IS NULL OR NOT o::Array(INT) = p <> q < r > s != t AS upyachka;" "WITH 2 AS \`b.c\`, [4, 5] AS a, 6 AS u, 3 AS v, 2 AS d, TRUE AS e, 1 AS f, 0 AS g, 2 AS h, 'Hello' AS i, 'World' AS j, TIMESTAMP '2022-02-02 02:02:02' AS w, [] AS k, (1, 2) AS l, 2 AS m, 3 AS n, [] AS o, [1] AS p, 1 AS q, q AS r, 1 AS s, 1 AS t
# SELECT \$ INTERVAL CASE CASE WHEN NOT -a[b.c] * u DIV v + d IS NOT NULL AND e OR f BETWEEN g AND h THEN i ELSE j END WHEN w THEN k END || [l, (m, n)] MINUTE IS NULL OR NOT o::Array(INT) = p <> q < r > s != t AS upyachka;" 2
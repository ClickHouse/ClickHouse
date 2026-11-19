-- A bare `NULL` key skips every row, so it must accept every range a typed key accepts,
-- including a `UInt64` upper bound above the `Int64` range.

SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, 0, 18446744073709551615)(NULL) AS r);
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, 0, 18446744073709551615)(CAST(NULL AS Nullable(UInt64))) AS r);
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT sumSparkbar(3, 0, 18446744073709551615)(NULL, number) AS r FROM numbers(3));

-- Signed bounds still work with a bare `NULL` key.
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, -5, 5)(NULL) AS r FROM numbers(3));

-- An invalid range is still rejected.
SELECT countSparkbar(3, 5, 5)(NULL); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(3, 18446744073709551615, 0)(NULL); -- { serverError BAD_ARGUMENTS }

-- A non-negative `Int64` bound selects the unsigned path too.
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, toInt64(0), 18446744073709551615)(NULL) AS r);
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, toInt64(0), 18446744073709551615)(CAST(NULL AS Nullable(UInt64))) AS r);
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT countSparkbar(3, toInt64(0), toInt64(10))(NULL) AS r);

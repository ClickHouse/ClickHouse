SELECT k, any(u) AS u, uniqMerge(us) AS us FROM (SELECT domain(URL) AS k, uniq(UserID) AS u, uniqState(UserID) AS us FROM test.hits GROUP BY k) GROUP BY k ORDER BY u DESC, k ASC LIMIT 100

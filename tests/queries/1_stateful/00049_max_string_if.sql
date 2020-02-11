SELECT CounterID, count(), maxIf(SearchPhrase, notEmpty(SearchPhrase)) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20

SELECT count() FROM test.hits WHERE NOT (EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01'))

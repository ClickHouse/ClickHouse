/* Заметим, что запросы написаны так, как будто пользователь не понимает смысл символа _ в LIKE выражении. */
SELECT count() FROM test.hits WHERE URL LIKE '%/avtomobili_s_probegom/_%__%__%__%';
SELECT count() FROM test.hits WHERE URL LIKE '/avtomobili_s_probegom/_%__%__%__%';
SELECT count() FROM test.hits WHERE URL LIKE '%_/avtomobili_s_probegom/_%__%__%__%';
SELECT count() FROM test.hits WHERE URL LIKE '%avtomobili%';

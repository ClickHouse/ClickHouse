-- Check that server does not get segfault due to bad stack unwinding from Hyperscan

SET query_profiler_cpu_time_period_ns = 1000000;
SET query_profiler_real_time_period_ns = 1000000;

SELECT count() FROM test.hits WHERE match(URL, ' *tranio\\.ru/spain/*/commercial/*') settings max_threads=5;

select count(position(URL, 'yandex')), count(position(URL, 'google')) FROM test.hits;
select count(multiSearchAllPositions(URL, ['yandex', 'google'])) FROM test.hits;
select count(match(URL, 'yandex|google')) FROM test.hits;
select count(multiMatchAny(URL, ['yandex', 'google'])) FROM test.hits;

select sum(match(URL, 'yandex')), sum(match(URL, 'google')), sum(match(URL, 'yahoo')), sum(match(URL, 'pikabu')) FROM test.hits;
select sum(multiSearchAny(URL, ['yandex', 'google', 'yahoo', 'pikabu'])) from test.hits;
select sum(multiMatchAny(URL, ['yandex', 'google', 'yahoo', 'pikabu'])) from test.hits;
select sum(match(URL, 'yandex|google|yahoo|pikabu')) FROM test.hits;

select sum(match(URL, 'yandex')), sum(match(URL, 'google')), sum(match(URL, 'http')) FROM test.hits;
select sum(multiSearchAny(URL, ['yandex', 'google', 'http'])) from test.hits;
select sum(multiMatchAny(URL, ['yandex', 'google', 'http'])) from test.hits;
select sum(match(URL, 'yandex|google|http')) FROM test.hits;

select sum(match(URL, 'yandex')), sum(match(URL, 'google')), sum(match(URL, 'facebook')), sum(match(URL, 'wikipedia')), sum(match(URL, 'reddit')) FROM test.hits;
select sum(multiSearchAny(URL, ['yandex', 'google', 'facebook', 'wikipedia', 'reddit'])) from test.hits;
select sum(multiMatchAny(URL, ['yandex', 'google', 'facebook', 'wikipedia', 'reddit'])) from test.hits;
select sum(match(URL, 'yandex|google|facebook|wikipedia|reddit')) FROM test.hits;

select sum(multiSearchFirstIndex(URL, ['yandex', 'google', 'http', 'facebook', 'google'])) from test.hits;

SELECT count() FROM test.hits WHERE multiMatchAny(URL, ['about/address', 'for_woman', '^https?://lm-company.ruy/$', 'ultimateguitar.com']);
SELECT count() FROM test.hits WHERE match(URL, 'about/address|for_woman|^https?://lm-company.ruy/$|ultimateguitar.com');

SELECT count() FROM test.hits WHERE match(URL, 'chelyabinsk.74.ru|doctor.74.ru|transport.74.ru|m.74.ru|//74.ru/|chel.74.ru|afisha.74.ru|diplom.74.ru|chelfin.ru|//chel.ru|chelyabinsk.ru|cheldoctor.ru|//mychel.ru|cheldiplom.ru|74.ru/video|market|poll|mail|conference|consult|contest|tags|feedback|pages|text');
SELECT count() FROM test.hits WHERE multiMatchAny(URL, ['chelyabinsk.74.ru', 'doctor.74.ru', 'transport.74.ru', 'm.74.ru', '//74.ru/', 'chel.74.ru', 'afisha.74.ru', 'diplom.74.ru', 'chelfin.ru', '//chel.ru', 'chelyabinsk.ru', 'cheldoctor.ru', '//mychel.ru', 'cheldiplom.ru', '74.ru/video', 'market', 'poll', 'mail', 'conference', 'consult', 'contest', 'tags', 'feedback', 'pages', 'text']);

SELECT count() FROM test.hits WHERE multiMatchAny(URL, ['chelyabinsk\\.74\\.ru', 'doctor\\.74\\.ru', 'transport\\.74\\.ru', 'm\\.74\\.ru', '//74\\.ru/', 'chel\\.74\\.ru', 'afisha\\.74\\.ru', 'diplom\\.74\\.ru', 'chelfin\\.ru', '//chel\\.ru', 'chelyabinsk\\.ru', 'cheldoctor\\.ru', '//mychel\\.ru', 'cheldiplom\\.ru', '74\\.ru/video', 'market', 'poll', 'mail', 'conference', 'consult', 'contest', 'tags', 'feedback', 'pages', 'text']);
SELECT count() FROM test.hits WHERE multiSearchAny(URL, ['chelyabinsk.74.ru', 'doctor.74.ru', 'transport.74.ru', 'm.74.ru', '//74.ru/', 'chel.74.ru', 'afisha.74.ru', 'diplom.74.ru', 'chelfin.ru', '//chel.ru', 'chelyabinsk.ru', 'cheldoctor.ru', '//mychel.ru', 'cheldiplom.ru', '74.ru/video', 'market', 'poll', 'mail', 'conference', 'consult', 'contest', 'tags', 'feedback', 'pages', 'text']);

SELECT DISTINCT Title, multiFuzzyMatchAny(Title, 2, ['^metrika\\.ry$']) AS distance FROM test.hits WHERE distance = 1 ORDER BY Title;
SELECT DISTINCT Title, multiFuzzyMatchAny(Title, 5, ['^metrika\\.ry$']) AS distance FROM test.hits WHERE distance = 1 ORDER BY Title;
SELECT sum(multiFuzzyMatchAny(Title, 3, ['hello$', 'world$', '^hello'])) FROM test.hits;
SELECT count() FROM test.hits WHERE multiFuzzyMatchAny(URL, 2, ['about/address', 'for_woman', '^https?://lm-company.ruy/$', 'ultimateguitar.com']);

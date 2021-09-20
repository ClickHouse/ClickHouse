-- Check that server does not get segfault due to bad stack unwinding from Hyperscan

SET query_profiler_cpu_time_period_ns = 1000000;
SET query_profiler_real_time_period_ns = 1000000;

SELECT count() FROM test.hits WHERE multiFuzzyMatchAny(URL, 2, ['about/address', 'for_woman', '^https?://lm-company.ruy/$', 'ultimateguitar.com']);

# Known findings and accepted non-bug classes (for dedup; triage.sh greps this)

## Real bugs found & fixed on this branch (do NOT re-report)
- finding 25 Float64 literal written as >64-bit integer, unreadable by formatQueryFromJSON (ASTJSONHelpers writeFieldJSON)
- finding 26 Tuple(partially named) rejected by readJSON (ASTTupleDataType)
- finding 27 BACKUP/RESTORE "... AS t" rename lost through JSON (ASTBackupQuery writeJSON)
- finding 28 TTL GROUP BY with no keys rejected by readJSON (ASTTTLElement)
- finding 30 JIT number->Decimal conversion silent overflow, returns garbage not DECIMAL_OVERFLOW (FunctionsConversion convertCompileImpl)

## Real bug reported, NOT fixed (maintainer decision)
- finding 31 optimize_arithmetic_operations_in_aggregate_functions: min/max(x +/- c) over EMPTY input returns c not 0 (default-on). SELECT min(number+1) FROM numbers(0) = 1.

## Accepted NON-BUG oracle classes (set aside; NOT bugs)
- Code 403 "Cannot determine join keys in JOIN ON" — non-equi JOIN ON rejected by grace_hash/partial_merge (parallel variant), fine on hash join.
- Code 6 / Code 53 "Cannot parse/convert" in a dead or short-circuited branch — optimization skips a throwing non-selected branch (CAST in caseWithExpression/multiIf, concat(NULL)+intDiv, notLike(...,NULL)+toFixedString). default folds to NULL/selected branch; outcome is throw-vs-NULL, never a wrong value.
- Code 153 "Division by zero" via concat(...,NULL,...)+intDiv — same NULL-folding dead-computation class.
- count(*) on ENGINE=Set/Join — optimize_trivial_count_query returns count from metadata; unoptimized path can't read the storage.
- range()/mapPopulateSeries "produce too many elements > max" — resource limit (function_range_max_elements_in_block), block-size dependent; never a wrong result.
- stochasticLinearRegression/stochasticLogisticRegression state differs across variants (SGD order/thread dependent).
- INFORMATION_SCHEMA.* / system.* queries differ (volatile estimate columns table_rows/data_length; live catalog).
- timeSeriesTagsToGroup/timeSeriesRemoveTag group IDs differ across variants (session group registry).
These are already filtered by monitor_check_once.sh and/or the oracle variant settings.

## Benign slow-unit / contention classes (set aside; NOT bugs)
- Timeouts with the whole stack in cleanupFuzzerObjects (DROP ... SYNC of a background-engine table).
- arrayWithConstant/arrayResize/replicate with a huge count (plan-time const-folding ignores limits) — skipped by usesUnboundedGenerator.
- range/timeSeriesRange huge count — bounded by --function_range_max_elements_in_block=1000000 in exec_local_args.txt.
- levenshteinDistance/quantilesExact etc. over numbers() (no-arg, unbounded) or huge LIMIT — expensive per-row, max_execution_time is checked between blocks.
- CREATE ... ENGINE = NATS/Kafka/RabbitMQ with huge *_connect_tries — connect-retry to a dead endpoint (PENDING: add bare engine names to usesExternalTableFunction).
- ASan "out of memory: allocator is trying to allocate <huge>" — a query requesting an absurd allocation; benign.
- 4-shards-at-once timeouts with runner CPU-starved ("Timed out waiting for runner thread stack trace") = co-tenant contention, never a bug. Check loadavg + top CPU first.

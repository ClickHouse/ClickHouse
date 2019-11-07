### New Feature
* Merging the first part of [#4354](https://github.com/yandex/ClickHouse/issues/4354) (input and output data format `Template`) [#6727](https://github.com/yandex/ClickHouse/pull/6727) ([tavplubix](https://github.com/tavplubix))
* Alter table drop detached part [#6158](https://github.com/yandex/ClickHouse/pull/6158) ([tavplubix](https://github.com/tavplubix))
* Possibility to change the location of clickhouse history file using `CLICKHOUSE_HISTORY_FILE` env. [#6840](https://github.com/yandex/ClickHouse/pull/6840) ([filimonov](https://github.com/filimonov))
* Globs in storage file and HDFS [#6092](https://github.com/yandex/ClickHouse/pull/6092) ([Olga Khvostikova](https://github.com/stavrolia))
* Enable processors by default. [#6592](https://github.com/yandex/ClickHouse/pull/6592) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Function to check if given token is in haystack, supported by tokenbf_v1 index; ... [#6596](https://github.com/yandex/ClickHouse/pull/6596) ([Vasily Nemkov](https://github.com/Enmk))
* VALUES list [#6209](https://github.com/yandex/ClickHouse/pull/6209) ([dimarub2000](https://github.com/dimarub2000))
* WITH TIES modifier for LIMIT and WITH FILL modifier for ORDER BY. (continuation of [#5069](https://github.com/yandex/ClickHouse/issues/5069)) [#6610](https://github.com/yandex/ClickHouse/pull/6610) ([Anton Popov](https://github.com/CurtizJ))
* Now text logs saved in system table. ... [#6103](https://github.com/yandex/ClickHouse/pull/6103) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* New function neighbour(value, offset[, default_value]). Allows to reach prev/next value within column. [#5925](https://github.com/yandex/ClickHouse/pull/5925) ([Alex Krash](https://github.com/alex-krash))
* Created a function currentUser(), returning login of authorized user. ... [#6470](https://github.com/yandex/ClickHouse/pull/6470) ([Alex Krash](https://github.com/alex-krash))
* Suggested in [#5885](https://github.com/yandex/ClickHouse/issues/5885) and implemented in a better way. [#6477](https://github.com/yandex/ClickHouse/pull/6477) ([dimarub2000](https://github.com/dimarub2000))
* hasTokenCaseInsensitive and tests; ... [#6662](https://github.com/yandex/ClickHouse/pull/6662) ([Vasily Nemkov](https://github.com/Enmk))
* Turn on query profiler by default to sample every query execution thread once a second. [#6283](https://github.com/yandex/ClickHouse/pull/6283) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add an ability to alter storage settings. [#6366](https://github.com/yandex/ClickHouse/pull/6366) ([alesapin](https://github.com/alesapin))
* Add support for `_partition` and `_timestamp` virtual columns [#6400](https://github.com/yandex/ClickHouse/pull/6400) ([Ivan](https://github.com/abyss7))
* Possibility to remove sensitive data from query_log, server logs, process list with regexp-based rules. [#5710](https://github.com/yandex/ClickHouse/pull/5710) ([filimonov](https://github.com/filimonov))
* bitmapRange implementation. Return new set with specified range (not include the range_end). ... [#6314](https://github.com/yandex/ClickHouse/pull/6314) ([Zhichang Yu](https://github.com/yuzhichang))
* Using `FastOps` library for functions `exp`, `log`, `sigmoid`, `tanh`. Added two new functions: `sigmoid` and `tanh`. FastOps is a fast vector math library from Michael Parakhin (Yandex CTO). Improved performance of `exp` and `log` functions more than 6 times. The functions `exp` and `log` from `Float32` argument will return `Float32` (in previous versions they always return `Float64`). Now `exp(nan)` may return `inf`. The result of `exp` and `log` functions may be not the nearest machine representable number to the true answer. [#6254](https://github.com/yandex/ClickHouse/pull/6254) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Throws an exception if config.d file doesn't have the corresponding root element as the config file [#6123](https://github.com/yandex/ClickHouse/pull/6123) ([dimarub2000](https://github.com/dimarub2000))
* Merge pull request [#6080](https://github.com/yandex/ClickHouse/issues/6080) from abyss7/issue-6071 [#6177](https://github.com/yandex/ClickHouse/pull/6177) ([akuzm](https://github.com/akuzm))
* Merge pull request [#6026](https://github.com/yandex/ClickHouse/issues/6026) from bopohaa/fix-kafka-unclean-stream [#6174](https://github.com/yandex/ClickHouse/pull/6174) ([akuzm](https://github.com/akuzm))
* Merge pull request [#6119](https://github.com/yandex/ClickHouse/issues/6119) from yandex/fix-cast-from-nullable-lc [#6170](https://github.com/yandex/ClickHouse/pull/6170) ([akuzm](https://github.com/akuzm))
* Resolves [#6182](https://github.com/yandex/ClickHouse/issues/6182) and [#6252](https://github.com/yandex/ClickHouse/issues/6252) [#6352](https://github.com/yandex/ClickHouse/pull/6352) ([tavplubix](https://github.com/tavplubix))
* Merge pull request [#6126](https://github.com/yandex/ClickHouse/issues/6126) from yandex/fix_index_write_with_adaptive_gr… [#6193](https://github.com/yandex/ClickHouse/pull/6193) ([akuzm](https://github.com/akuzm))
* Added table function `numbers_mt`, which is multithreaded version of `numbers`. Updated performance tests with hash functions. [#6554](https://github.com/yandex/ClickHouse/pull/6554) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Merging [#4247](https://github.com/yandex/ClickHouse/issues/4247) [#6124](https://github.com/yandex/ClickHouse/pull/6124) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Function geohashesInBox() creates array of geohash-encoded boxes that cover given area. ... [#6127](https://github.com/yandex/ClickHouse/pull/6127) ([Vasily Nemkov](https://github.com/Enmk))
* Table constraints [#5273](https://github.com/yandex/ClickHouse/pull/5273) ([Gleb Novikov](https://github.com/NanoBjorn))
* Implement support for INSERT-query with Kafka tables [#6012](https://github.com/yandex/ClickHouse/pull/6012) ([Ivan](https://github.com/abyss7))
* Add input ORC format ... [#6454](https://github.com/yandex/ClickHouse/pull/6454) ([akonyaev90](https://github.com/akonyaev90))
* Added implementation of LIVE VIEW tables that were originally proposed in [#3925](https://github.com/yandex/ClickHouse/issues/3925), and then updated in [#5541](https://github.com/yandex/ClickHouse/issues/5541). Note that currently only LIVE VIEW tables are supported. See [#5541](https://github.com/yandex/ClickHouse/issues/5541) for detailed description. [#6425](https://github.com/yandex/ClickHouse/pull/6425) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Merge pull request [#6126](https://github.com/yandex/ClickHouse/issues/6126) from yandex/fix_index_write_with_adaptive_gr… [#6169](https://github.com/yandex/ClickHouse/pull/6169) ([akuzm](https://github.com/akuzm))
* Detailed description (optional): ... [#6467](https://github.com/yandex/ClickHouse/pull/6467) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* [WIP] Clickhouse-benchmark comparison mode [#6343](https://github.com/yandex/ClickHouse/pull/6343) ([dimarub2000](https://github.com/dimarub2000))
* Merge pull request [#6080](https://github.com/yandex/ClickHouse/issues/6080) from abyss7/issue-6071 [#6180](https://github.com/yandex/ClickHouse/pull/6180) ([akuzm](https://github.com/akuzm))

### Bug Fix
* Fix segmentation fault when the table has skip indices and vertical merge happens. [#6723](https://github.com/yandex/ClickHouse/pull/6723) ([alesapin](https://github.com/alesapin))
* Fix issue[#6136](https://github.com/yandex/ClickHouse/issues/6136) and is a fixed version of [#6146](https://github.com/yandex/ClickHouse/issues/6146) [#6156](https://github.com/yandex/ClickHouse/pull/6156) ([dimarub2000](https://github.com/dimarub2000))
* Fix column TTL with user defaults. [#6796](https://github.com/yandex/ClickHouse/pull/6796) ([Anton Popov](https://github.com/CurtizJ))
* Fix Kafka messages duplication problem on normal server restart. ... [#6597](https://github.com/yandex/ClickHouse/pull/6597) ([Ivan](https://github.com/abyss7))
* Fix segfault with enabled `optimize_skip_unused_shards` and missing sharding key. [#6384](https://github.com/yandex/ClickHouse/pull/6384) ([Anton Popov](https://github.com/CurtizJ))
* Fix bug introduced in query profiler which leads to endless recv from socket. [#6386](https://github.com/yandex/ClickHouse/pull/6386) ([alesapin](https://github.com/alesapin))
* Removed extra verbose logging from MySQL handler [#6389](https://github.com/yandex/ClickHouse/pull/6389) ([alexey-milovidov](https://github.com/alexey-milovidov))
* I think TTL is supported for AggregatingMergeTree [#6276](https://github.com/yandex/ClickHouse/pull/6276) ([Šimon Podlipský](https://github.com/simPod))
* Return ability to parse boolean settings from 'true' and 'false'. [#6278](https://github.com/yandex/ClickHouse/pull/6278) ([alesapin](https://github.com/alesapin))
* Crash in median over Nullable(Decimal128) [#6378](https://github.com/yandex/ClickHouse/pull/6378) ([Artem Zuikov](https://github.com/4ertus2))
* Fixes [#6248](https://github.com/yandex/ClickHouse/issues/6248) [#6374](https://github.com/yandex/ClickHouse/pull/6374) ([dimarub2000](https://github.com/dimarub2000))
* Fixed the possibility of a fabricated query to cause server crash due to stack overflow in SQL parser. Fixed the possibility of stack overflow in Merge and Distributed tables, materialized views and conditions for row-level security that involve subqueries. [#6433](https://github.com/yandex/ClickHouse/pull/6433) ([alexey-milovidov](https://github.com/alexey-milovidov))
* fix some mutation bug [#6205](https://github.com/yandex/ClickHouse/pull/6205) ([Winter Zhang](https://github.com/zhang2014))
* Fix formula for new_size in WriteBufferFromVector(AppendModeTag). [#6208](https://github.com/yandex/ClickHouse/pull/6208) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix kafka tests [#6805](https://github.com/yandex/ClickHouse/pull/6805) ([Ivan](https://github.com/abyss7))
* Fixes [#6502](https://github.com/yandex/ClickHouse/issues/6502) [#6617](https://github.com/yandex/ClickHouse/pull/6617) ([tavplubix](https://github.com/tavplubix))
* Fix JOIN results for key columns when used with join_use_nulls. Attach Nulls instead of columns defaults. [#6249](https://github.com/yandex/ClickHouse/pull/6249) ([Artem Zuikov](https://github.com/4ertus2))
* Fix recursive materialized view [#6324](https://github.com/yandex/ClickHouse/pull/6324) ([Amos Bird](https://github.com/amosbird))
* Fix extracting a Tuple from JSON [#6718](https://github.com/yandex/ClickHouse/pull/6718) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix for data race in StorageMerge [#6717](https://github.com/yandex/ClickHouse/pull/6717) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Trying to fix skip indices with vertical merge and alter. Also trying to fix: https://github.com/yandex/ClickHouse/issues/6594 [#6713](https://github.com/yandex/ClickHouse/pull/6713) ([alesapin](https://github.com/alesapin))
* Fix rare crash in `ALTER MODIFY COLUMN` and vertical merge when one of merged/altered parts is empty (0 rows). This fixes https://github.com/yandex/ClickHouse/issues/6746. [#6780](https://github.com/yandex/ClickHouse/pull/6780) ([alesapin](https://github.com/alesapin))
* Fixed wrong behaviour of `nullIf` function for constant arguments. [#6580](https://github.com/yandex/ClickHouse/pull/6580) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Link fix [#6288](https://github.com/yandex/ClickHouse/pull/6288) ([BayoNet](https://github.com/BayoNet))
* Fixed bug in conversion of `LowCardinality` types in `AggregateFunctionFactory`. This fixes [#6257](https://github.com/yandex/ClickHouse/issues/6257). [#6281](https://github.com/yandex/ClickHouse/pull/6281) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix https://github.com/yandex/ClickHouse/issues/6224 [#6282](https://github.com/yandex/ClickHouse/pull/6282) ([Nikita Vasilev](https://github.com/nikvas0))
* Do not expose virtual columns in `system.columns` table. This is required for backward compatibility. [#6406](https://github.com/yandex/ClickHouse/pull/6406) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix wrong behavior and possible segfaults in `topK` and `topKWeighted` aggregated functions. [#6404](https://github.com/yandex/ClickHouse/pull/6404) ([Anton Popov](https://github.com/CurtizJ))
* Fixed unsafe code around "getIdentifier" function. This fixes [#6401](https://github.com/yandex/ClickHouse/issues/6401) [#6409](https://github.com/yandex/ClickHouse/pull/6409) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed heap buffer overflow in PacketPayloadWriteBuffer. An error occured due to setting working_buffer to empty. Because of it `next` does not call `nextImpl` and `write` becomes an infinite loop without side effects, which apparently causes `pos` to be incremented beyond the buffer. [#6212](https://github.com/yandex/ClickHouse/pull/6212) ([Yuriy Baranov](https://github.com/yurriy))
* fix bitmapSubsetInRange memory leak [#6819](https://github.com/yandex/ClickHouse/pull/6819) ([Zhichang Yu](https://github.com/yuzhichang))
* Fix rare bug when mutation executed after granularity change. [#6816](https://github.com/yandex/ClickHouse/pull/6816) ([alesapin](https://github.com/alesapin))
* Allow protobuf message with all fields by default [#6132](https://github.com/yandex/ClickHouse/pull/6132) ([Vitaly Baranov](https://github.com/vitlibar))
* Better JOIN ON keys extraction [#6131](https://github.com/yandex/ClickHouse/pull/6131) ([Artem Zuikov](https://github.com/4ertus2))
* Resolve a bug with `nullIf` function when we send a `NULL` argument on the second argument. [#6446](https://github.com/yandex/ClickHouse/pull/6446) ([Guillaume Tassery](https://github.com/YiuRULE))
* Fix rare bug with wrong memory allocation/deallocation in complex key cache dictionaries with string fields which leads to infinite memory consumption (looks like memory leak). Bug reproduces when string size was a power of two starting from eight (8, 16, 32, etc). [#6447](https://github.com/yandex/ClickHouse/pull/6447) ([alesapin](https://github.com/alesapin))
* Fixed Gorilla encoding on small sequences. ... [#6444](https://github.com/yandex/ClickHouse/pull/6444) ([Vasily Nemkov](https://github.com/Enmk))
* Fixes [#6117](https://github.com/yandex/ClickHouse/issues/6117) [#6122](https://github.com/yandex/ClickHouse/pull/6122) ([filimonov](https://github.com/filimonov))
* Fixed error with processing "timezone" in server configuration file. [#6709](https://github.com/yandex/ClickHouse/pull/6709) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to use not nullable types in JOINs with ```join_use_nulls``` enabled [#6705](https://github.com/yandex/ClickHouse/pull/6705) ([Artem Zuikov](https://github.com/4ertus2))
* Disable Poco::AbstractConfiguration substitutions in query in clickhouse-client [#6706](https://github.com/yandex/ClickHouse/pull/6706) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed mismatched header in streams happened in case of reading from empty distributed table with sample and prewhere. Merge [#6167](https://github.com/yandex/ClickHouse/issues/6167). [#6823](https://github.com/yandex/ClickHouse/pull/6823) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Avoid deadlock in REPLACE PARTITION. To be continued... [#6677](https://github.com/yandex/ClickHouse/pull/6677) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Query transformation for MySQL, ODBC, JDBC table functions now works properly for SELECT WHERE queries with multiple AND subqueries. Fixes[#6381](https://github.com/yandex/ClickHouse/issues/6381) [#6676](https://github.com/yandex/ClickHouse/pull/6676) ([dimarub2000](https://github.com/dimarub2000))
* Fix two vulnerabilities in Codecs in decompresion phase. [#6670](https://github.com/yandex/ClickHouse/pull/6670) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed bug in function arrayEnumerateUniqRanked [#6779](https://github.com/yandex/ClickHouse/pull/6779) ([proller](https://github.com/proller))
* Try to fix [6575](https://github.com/yandex/ClickHouse/issues/6575). ... [#6773](https://github.com/yandex/ClickHouse/pull/6773) ([Zhichang Yu](https://github.com/yuzhichang))
* Using `arrayReduce` for constant arguments may lead to segfault. This fixes [#6242](https://github.com/yandex/ClickHouse/issues/6242) [#6326](https://github.com/yandex/ClickHouse/pull/6326) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes [#6522](https://github.com/yandex/ClickHouse/issues/6522) [#6523](https://github.com/yandex/ClickHouse/pull/6523) ([tavplubix](https://github.com/tavplubix))
* Do not pause/resume consumer on subscription at all - otherwise it may get paused indefinitely in some scenarios. [#6354](https://github.com/yandex/ClickHouse/pull/6354) ([Ivan](https://github.com/abyss7))
* Fix crash when casting types to Decimal that do not support it. Throw exception instead. [#6297](https://github.com/yandex/ClickHouse/pull/6297) ([Artem Zuikov](https://github.com/4ertus2))
* Security issue. If the attacker has write access to ZooKeeper and is able to run custom server available from the network where ClickHouse run, it can create custom-built malicious server that will act as ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem. Found by Eldar Zaitov, information security team at Yandex. [#6247](https://github.com/yandex/ClickHouse/pull/6247) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed hang in JSONExtractRaw function. This PR fixes [#6195](https://github.com/yandex/ClickHouse/issues/6195) [#6198](https://github.com/yandex/ClickHouse/pull/6198) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes [#6125](https://github.com/yandex/ClickHouse/issues/6125) [#6550](https://github.com/yandex/ClickHouse/pull/6550) ([tavplubix](https://github.com/tavplubix))
* Fixes the regression, see the test for broken example [#6415](https://github.com/yandex/ClickHouse/pull/6415) ([Ivan](https://github.com/abyss7))
* Fixes [#6426](https://github.com/yandex/ClickHouse/issues/6426) [#6559](https://github.com/yandex/ClickHouse/pull/6559) ([tavplubix](https://github.com/tavplubix))
* Atomicity in drop replicated table. [#6413](https://github.com/yandex/ClickHouse/pull/6413) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix bug with incorrect skip indices serialization and aggregation with adaptive granularity. This fixes https://github.com/yandex/ClickHouse/issues/6594. [#6748](https://github.com/yandex/ClickHouse/pull/6748) ([alesapin](https://github.com/alesapin))
* Fix `WITH ROLLUP` and `WITH CUBE` modifiers of `GROUP BY` with two-level aggregation. ... [#6225](https://github.com/yandex/ClickHouse/pull/6225) ([Anton Popov](https://github.com/CurtizJ))
* Improve error handling in cache dictionaries [#6737](https://github.com/yandex/ClickHouse/pull/6737) ([Vitaly Baranov](https://github.com/vitlibar))
* Parquet: Fix reading boolean columns. [#6579](https://github.com/yandex/ClickHouse/pull/6579) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix bug with writing secondary indices marks with adaptive granularity. ... [#6126](https://github.com/yandex/ClickHouse/pull/6126) ([alesapin](https://github.com/alesapin))
* Since `StorageMergeTree::background_task_handle` is initialized in `startup()` the `MergeTreeBlockOutputStream::write()` may try to use it before initialization. Just check if it is initialized. [#6080](https://github.com/yandex/ClickHouse/pull/6080) ([Ivan](https://github.com/abyss7))
* Hotfix for crash in extractAll() function. [#6644](https://github.com/yandex/ClickHouse/pull/6644) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed wrong behaviour of `trim` functions family. [#6647](https://github.com/yandex/ClickHouse/pull/6647) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Clearing the data buffer from the previous read operation that was completed with an error [#6026](https://github.com/yandex/ClickHouse/pull/6026) ([Nikolay](https://github.com/bopohaa))
* Fix bug with enabling adaptive granularity when creating new replica for Replicated*MergeTree table. Fixes https://github.com/yandex/ClickHouse/issues/6394. [#6452](https://github.com/yandex/ClickHouse/pull/6452) ([alesapin](https://github.com/alesapin))
* Fixed possible crash during server startup in case of exception happened in `libunwind` during exception at access to uninitialised `ThreadStatus` structure. [#6456](https://github.com/yandex/ClickHouse/pull/6456) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fixed data race in system.parts table and ALTER query. This fixes [#6245](https://github.com/yandex/ClickHouse/issues/6245). [#6513](https://github.com/yandex/ClickHouse/pull/6513) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix FPE in yandexConsistentHash function. Found by fuzz test. This fixes [#6304](https://github.com/yandex/ClickHouse/issues/6304) [#6305](https://github.com/yandex/ClickHouse/pull/6305) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed the possibility of hanging queries when server is overloaded and global thread pool becomes near full. This have higher chance to happen on clusters with large number of shards (hundreds), because distributed queries allocate a thread per connection to each shard. For example, this issue may reproduce if a cluster of 330 shards is processing 30 concurrent distributed queries. This issue affects all versions starting from 19.2. [#6301](https://github.com/yandex/ClickHouse/pull/6301) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed logic of `arrayEnumerateUniqRanked` function. [#6423](https://github.com/yandex/ClickHouse/pull/6423) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix segfault when decoding symbol table. [#6603](https://github.com/yandex/ClickHouse/pull/6603) ([Amos Bird](https://github.com/amosbird))
* Hi ... [#6167](https://github.com/yandex/ClickHouse/pull/6167) ([Lixiang Qian](https://github.com/fancyqlx))
* Fixed irrelevant exception in cast of `LowCardinality(Nullable)` to not-Nullable column in case if it doesn't contain Nulls (e.g. in query like `SELECT CAST(CAST('Hello' AS LowCardinality(Nullable(String))) AS String)`. [#6094](https://github.com/yandex/ClickHouse/issues/6094) [#6119](https://github.com/yandex/ClickHouse/pull/6119) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Removed extra quoting of description in `system.settings` table. This fixes [#6696](https://github.com/yandex/ClickHouse/issues/6696) [#6699](https://github.com/yandex/ClickHouse/pull/6699) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid possible deadlock in TRUNCATE of Replicated table. [#6695](https://github.com/yandex/ClickHouse/pull/6695) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix case with same column names in GLOBAL JOIN ON section. [#6181](https://github.com/yandex/ClickHouse/pull/6181) ([Artem Zuikov](https://github.com/4ertus2))
* Fix reading in order of sorting key and refactoring. [#6189](https://github.com/yandex/ClickHouse/pull/6189) ([Anton Popov](https://github.com/CurtizJ))
* Fix `ALTER TABLE ... UPDATE` query for tables with `enable_mixed_granularity_parts=1`. [#6543](https://github.com/yandex/ClickHouse/pull/6543) ([alesapin](https://github.com/alesapin))
* Typo fix in the assumeNotNull description. [#6542](https://github.com/yandex/ClickHouse/pull/6542) ([BayoNet](https://github.com/BayoNet))
* Fixed the case when server may close listening sockets but not shutdown and continue serving remaining queries. You may end up with two running clickhouse-server processes. Sometimes, the server may return an error `bad_function_call` for remaining queries. [#6231](https://github.com/yandex/ClickHouse/pull/6231) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Table function `url` had the vulnerability allowed the attacker to inject arbitrary HTTP headers in the request. This issue was found by [Nikita Tikhomirov](https://github.com/NSTikhomirov). [#6466](https://github.com/yandex/ClickHouse/pull/6466) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added gcc-9 support to `docker/builder` container that builds image locally. [#6333](https://github.com/yandex/ClickHouse/pull/6333) ([Gleb Novikov](https://github.com/NanoBjorn))
* Fix bug opened by https://github.com/yandex/ClickHouse/pull/4405 (since 19.4.0). Reproduces in queries to Distributed tables over MergeTree tables when we doesn't query any columns (`SELECT 1`). [#6236](https://github.com/yandex/ClickHouse/pull/6236) ([alesapin](https://github.com/alesapin))
* Fixed overflow in integer division of signed type to unsigned type. The behaviour was exactly as in C or C++ language (integer promotion rules) that may be surprising. This fixes [#6214](https://github.com/yandex/ClickHouse/issues/6214). Please note that the overflow is still possible when dividing large signed number to large unsigned number or vice-versa (but that case is less usual). The issue existed in all server versions. [#6233](https://github.com/yandex/ClickHouse/pull/6233) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Limit maximum sleep time for throttling when `max_execution_speed` or `max_execution_speed_bytes` is set. Fixed false errors like `Estimated query execution time (inf seconds) is too long`. This fixes[#5547](https://github.com/yandex/ClickHouse/issues/5547) [#6232](https://github.com/yandex/ClickHouse/pull/6232) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Improvement
* Safer interface of mysqlxx::Pool [#6150](https://github.com/yandex/ClickHouse/pull/6150) ([avasiliev](https://github.com/avasiliev))
* Options line size when executing with --help option now corresponds with terminal size. ... [#6590](https://github.com/yandex/ClickHouse/pull/6590) ([dimarub2000](https://github.com/dimarub2000))
* Disable "read in order" optimization for aggregation without keys. [#6599](https://github.com/yandex/ClickHouse/pull/6599) ([Anton Popov](https://github.com/CurtizJ))
* HTTP status code for `INCORRECT_DATA` and `TYPE_MISMATCH` error codes was changed from default 500 Internal Server Error to 400 Bad Request. [#6271](https://github.com/yandex/ClickHouse/pull/6271) ([Alexander Rodin](https://github.com/a-rodin))
* Now values and rows with expired TTL will be removed after 'OPTIMIZE ... FINAL' query from old parts without TTL infos or with outdated TTL infos, e.g. after 'ALTER ... MODIFY TTL' query. ... [#6274](https://github.com/yandex/ClickHouse/pull/6274) ([Anton Popov](https://github.com/CurtizJ))
* Remove `dry_run` flag from `InterpreterSelectQuery`. ... [#6375](https://github.com/yandex/ClickHouse/pull/6375) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Move Join object from ExpressionAction into AnalyzedJoin. ExpressionAnalyzer and ExpressionAction do not know about Join class anymore. Its logic is hidden by AnalyzedJoin iface. [#6801](https://github.com/yandex/ClickHouse/pull/6801) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed possible deadlock of distributed queries when one of shards is localhost but the query is sent via network connection. [#6759](https://github.com/yandex/ClickHouse/pull/6759) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changed semantic of multiple tables RENAME to avoid possible deadlocks. This fixes [#6757](https://github.com/yandex/ClickHouse/issues/6757). [#6756](https://github.com/yandex/ClickHouse/pull/6756) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to ATTACH live views (for example, at the server startup) regardless to `allow_experimental_live_view` setting. [#6754](https://github.com/yandex/ClickHouse/pull/6754) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Rewritten MySQL compatibility server to prevent loading full packet payload in memory. Decreased memory consumption for each connection to approximately 2*DBMS_DEFAULT_BUFFER_SIZE (read/write buffers). [#5811](https://github.com/yandex/ClickHouse/pull/5811) ([Yuriy Baranov](https://github.com/yurriy))
* Move AST alias interpreting logic out of parser that doesn't have to know anything about query semantics. [#6108](https://github.com/yandex/ClickHouse/pull/6108) ([Artem Zuikov](https://github.com/4ertus2))
* Slightly more safe parsing of NamesAndTypesList. This fixes [#6408](https://github.com/yandex/ClickHouse/issues/6408). [#6410](https://github.com/yandex/ClickHouse/pull/6410) ([alexey-milovidov](https://github.com/alexey-milovidov))
* clickhouse-copier: Allow use `where_condition` from config with `partition_key` alias in query for checking partition existence (Earlier it was used only in reading data queries) [#6577](https://github.com/yandex/ClickHouse/pull/6577) ([proller](https://github.com/proller))
* Enabled SIMDJSON for machines without AVX2 but with SSE 4.2 and PCLMUL instruction set. ... [#6320](https://github.com/yandex/ClickHouse/pull/6320) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added optional message argument in throwIf ([#5772](https://github.com/yandex/ClickHouse/issues/5772)) [#6329](https://github.com/yandex/ClickHouse/pull/6329) ([Vdimir](https://github.com/Vdimir))
* Server exception got while sending insertion data by is now being processed in client as well. ... [#6711](https://github.com/yandex/ClickHouse/pull/6711) ([dimarub2000](https://github.com/dimarub2000))
* Added a metric `DistributedFilesToInsert` that shows the total number of files in filesystem that are selected to send to remote servers by Distributed tables. The number is summed across all shards. [#6600](https://github.com/yandex/ClickHouse/pull/6600) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Move most of JOINs prepare logic from ExpressionAction/ExpressionAnalyzer to AnalyzedJoin. [#6785](https://github.com/yandex/ClickHouse/pull/6785) ([Artem Zuikov](https://github.com/4ertus2))
* Support ASOF JOIN with ON section. [#6211](https://github.com/yandex/ClickHouse/pull/6211) ([Artem Zuikov](https://github.com/4ertus2))
* This is to fix TSan warning 'lock-order-inversion': https://clickhouse-test-reports.s3.yandex.net/6399/c1c1d1daa98e199e620766f1bd06a5921050a00d/functional_stateful_tests_(thread).html ... [#6740](https://github.com/yandex/ClickHouse/pull/6740) ([Vasily Nemkov](https://github.com/Enmk))
* Better information messages about lack of Linux capabilities. Logging fatal errors with "fatal" level, that will make it easier to find in `system.text_log`. [#6441](https://github.com/yandex/ClickHouse/pull/6441) ([alexey-milovidov](https://github.com/alexey-milovidov))
* For stack traces gathered by query profiler, do not include stack frames generated by the query profiler itself. [#6250](https://github.com/yandex/ClickHouse/pull/6250) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now table functions `values`, `file`, `url`, `hdfs` have support for ALIAS columns. [#6255](https://github.com/yandex/ClickHouse/pull/6255) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better subquery for join creation in ExpressionAnalyzer [#6824](https://github.com/yandex/ClickHouse/pull/6824) ([Artem Zuikov](https://github.com/4ertus2))
* When enable dumping temporary data to the disk to restrict memroy usage during GROUP BY/SORT, it didn't check the free disk space. The fix add a new setting min_free_disk_space, when the free disk space it smaller then the threshold, the query will stop and throw ErrorCodes::NOT_ENOUGH_SPACE [#6678](https://github.com/yandex/ClickHouse/pull/6678) ([Weiqing Xu](https://github.com/weiqxu))
* It makes no sense, because threads are reused between queries. SELECT query may acquire a lock in one thread, hold a lock from another thread and exit from first thread. In the same time, first thread can be reused by DROP query. This will lead to false "Attempt to acquire exclusive lock recursively" messages. [#6771](https://github.com/yandex/ClickHouse/pull/6771) ([alexey-milovidov](https://github.com/alexey-milovidov))
* When determining shards of a "Distributed" table to be covered by a read query (for `optimize_skip_unused_shards` = 1) Clickhouse now checks conditions from both `prewhere` and `where` clauses of select statement [#6521](https://github.com/yandex/ClickHouse/pull/6521) ([Alexander Kazakov](https://github.com/Akazz))
* Split ExpressionAnalyzer.appendJoin(). Prepare a place in ExpressionAnalyzer for MergeJoin. [#6524](https://github.com/yandex/ClickHouse/pull/6524) ([Artem Zuikov](https://github.com/4ertus2))
* Added mysql_native_password authentication plugin to MySQL compatibility server. [#6194](https://github.com/yandex/ClickHouse/pull/6194) ([Yuriy Baranov](https://github.com/yurriy))
* Less number of "clock_gettime" calls; fixed ABI compatibility between debug/release in Allocator (unsignificant issue). [#6197](https://github.com/yandex/ClickHouse/pull/6197) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixes [#5990](https://github.com/yandex/ClickHouse/issues/5990) [#6055](https://github.com/yandex/ClickHouse/pull/6055) ([tavplubix](https://github.com/tavplubix))
* Move collectUsedColumns from ExpressionAnalyzer to SyntaxAnalyzer. SyntaxAnalyzer makes required_source_columns itself now. [#6416](https://github.com/yandex/ClickHouse/pull/6416) ([Artem Zuikov](https://github.com/4ertus2))
* ClickHouse can work on filesystems without O_DIRECT support (such as ZFS and BtrFS) without additional tuning. This fixes [#4449](https://github.com/yandex/ClickHouse/issues/4449) [#6730](https://github.com/yandex/ClickHouse/pull/6730) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add setting to require aliases for subselects and table functions in FROM that more than one table is present (i.e. queries with JOINs). [#6733](https://github.com/yandex/ClickHouse/pull/6733) ([Artem Zuikov](https://github.com/4ertus2))
* support push down predicate to final subquery [#6120](https://github.com/yandex/ClickHouse/pull/6120) ([TCeason](https://github.com/TCeason))
* Extract GetAggregatesVisitor class from ExpressionAnalyzer. [#6458](https://github.com/yandex/ClickHouse/pull/6458) ([Artem Zuikov](https://github.com/4ertus2))
* system.query_log: change data type of `type` column to Enum. [#6265](https://github.com/yandex/ClickHouse/pull/6265) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Static linking of sha256_password authentication plugin. [#6512](https://github.com/yandex/ClickHouse/pull/6512) ([Yuriy Baranov](https://github.com/yurriy))
* Avoid extra dependency for the setting `compile` to work. In previous versions, the user may get error like `cannot open crti.o`, `unable to find library -lc` etc. [#6309](https://github.com/yandex/ClickHouse/pull/6309) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Upated SIMDJSON. This fixes [#6285](https://github.com/yandex/ClickHouse/issues/6285). [#6306](https://github.com/yandex/ClickHouse/pull/6306) ([alexey-milovidov](https://github.com/alexey-milovidov))
* More validation of the input that may come from malicious replica. [#6303](https://github.com/yandex/ClickHouse/pull/6303) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now `clickhouse-obfuscator` file is available in clickhouse-client package. In previous versions it was available as `clickhouse obfuscator` (with whitespace). Fixes [#5816](https://github.com/yandex/ClickHouse/issues/5816) [#6609](https://github.com/yandex/ClickHouse/pull/6609) ([dimarub2000](https://github.com/dimarub2000))
* Fixed deadlock when we have at least two queries that read at least two tables in different order and another query that performs DDL operation on one of tables. Fixed another very rare deadlock. [#6764](https://github.com/yandex/ClickHouse/pull/6764) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added `os_thread_ids` column to `system.processes` and `system.query_log` for better debugging possibilities. [#6763](https://github.com/yandex/ClickHouse/pull/6763) ([alexey-milovidov](https://github.com/alexey-milovidov))
* A workaround for PHP mysqlnd extension bugs which occur when sha256_password is used as a default authentication plugin (described in [#6031](https://github.com/yandex/ClickHouse/issues/6031)). [#6113](https://github.com/yandex/ClickHouse/pull/6113) ([Yuriy Baranov](https://github.com/yurriy))
* Remove unneded place with changed nullability columns. [#6693](https://github.com/yandex/ClickHouse/pull/6693) ([Artem Zuikov](https://github.com/4ertus2))
* Set default value of `queue_max_wait_ms` to zero, because current value (five seconds) makes no sense. There are rare circumstances when this settings has any use. Added settings `replace_running_query_max_wait_ms`, `kafka_max_wait_ms` and `connection_pool_max_wait_ms` for disambiguation. [#6692](https://github.com/yandex/ClickHouse/pull/6692) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Extract SelectQueryExpressionAnalyzer from ExpressionAnalyzer. Keep the last one for non-select queries. [#6499](https://github.com/yandex/ClickHouse/pull/6499) ([Artem Zuikov](https://github.com/4ertus2))
* Optimize Count() Cond. [#6344](https://github.com/yandex/ClickHouse/pull/6344) ([Amos Bird](https://github.com/amosbird))
* Removed duplicating input and output formats. [#6239](https://github.com/yandex/ClickHouse/pull/6239) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Allow user to override `poll_interval` and `idle_connection_timeout` settings on connection. [#6230](https://github.com/yandex/ClickHouse/pull/6230) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Performance Improvement
* Now less data will be read with enabled optimize_read_in_order and small limit. ... [#6299](https://github.com/yandex/ClickHouse/pull/6299) ([Anton Popov](https://github.com/CurtizJ))
* Disable consecutive key optimization for UInt8/16. [#6298](https://github.com/yandex/ClickHouse/pull/6298) ([akuzm](https://github.com/akuzm))
* Slightly improve performance of MemoryTracker. [#6653](https://github.com/yandex/ClickHouse/pull/6653) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix perfomance bug in Decimal comparison. [#6380](https://github.com/yandex/ClickHouse/pull/6380) ([Artem Zuikov](https://github.com/4ertus2))
* Implemented batch variant of updating aggregate function states. It may lead to performance benefits. [#6435](https://github.com/yandex/ClickHouse/pull/6435) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to use multiple threads during parts loading and removal. This fixes [#6372](https://github.com/yandex/ClickHouse/issues/6372) and [#6074](https://github.com/yandex/ClickHouse/issues/6074). [#6438](https://github.com/yandex/ClickHouse/pull/6438) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Show private symbols in stack traces (this is done via parsing symbol tables of ELF files). Added information about file and line number in stack traces if debug info is present. Speedup symbol name lookup with indexing symbols present in program. Added new SQL functions for introspection: `demangle` and `addressToLine`. Renamed function `symbolizeAddress` to `addressToSymbol` for consistency. Function `addressToSymbol` will return mangled name for performance reasons and you have to apply `demangle`. Added setting `allow_introspection_functions` which is turned off by default. [#6201](https://github.com/yandex/ClickHouse/pull/6201) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Pre-fault pages when allocating memory with mmap(). [#6667](https://github.com/yandex/ClickHouse/pull/6667) ([akuzm](https://github.com/akuzm))
* Implement 'read in order' optimization with processors. ... [#6629](https://github.com/yandex/ClickHouse/pull/6629) ([Anton Popov](https://github.com/CurtizJ))
* Disable consecutive key optimization for UInt8/16 LowCardinality columns. [#6701](https://github.com/yandex/ClickHouse/pull/6701) ([akuzm](https://github.com/akuzm))
* MergeTree now has an additional option `ttl_only_drop_parts` (disabled by default) to avoid partial pruning of parts, so that they dropped completely when all the rows in a part are expired. ... [#6191](https://github.com/yandex/ClickHouse/pull/6191) ([Sergi Vladykin](https://github.com/svladykin))
* Optimize queries with `ORDER BY expressions` clause, where `expressions` have coincidingprefix with`ORDER` key in `MergeTree` tables. [#6054](https://github.com/yandex/ClickHouse/pull/6054) ([Anton Popov](https://github.com/CurtizJ))

### Build/Testing/Packaging Improvement
* Best effort for printing stack traces. Also added SIGPROF as a debugging signal to print stack trace of a running thread. [#6529](https://github.com/yandex/ClickHouse/pull/6529) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix build with internal libc++ [#6724](https://github.com/yandex/ClickHouse/pull/6724) ([Ivan](https://github.com/abyss7))
* In debug version on Linux, increase OOM score. [#6152](https://github.com/yandex/ClickHouse/pull/6152) ([akuzm](https://github.com/akuzm))
* HDFS HA now work in debug build. [#6650](https://github.com/yandex/ClickHouse/pull/6650) ([Weiqing Xu](https://github.com/weiqxu))
* Changed boost::filesystem to std::filesystem, part 2. [#6385](https://github.com/yandex/ClickHouse/pull/6385) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added a test just in case [#6388](https://github.com/yandex/ClickHouse/pull/6388) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Just a test on this feature [#6509](https://github.com/yandex/ClickHouse/pull/6509) ([Ivan](https://github.com/abyss7))
* Removed rarely used table function `catBoostPool` and storage `CatBoostPool`. If you have used this table function, please write email to `clickhouse-feedback@yandex-team.com`. Note that CatBoost integration remains and will be supported. [#6279](https://github.com/yandex/ClickHouse/pull/6279) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make a better build scheme [#6500](https://github.com/yandex/ClickHouse/pull/6500) ([Ivan](https://github.com/abyss7))
* Fixed `test_external_dictionaries` integration in case it was executed under non root user. [#6507](https://github.com/yandex/ClickHouse/pull/6507) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* The bug reproduces when total size of written packets exceeds DBMS_DEFAULT_BUFFER_SIZE. [#6204](https://github.com/yandex/ClickHouse/pull/6204) ([Yuriy Baranov](https://github.com/yurriy))
* Added a test for RENAME / Merge table race condition [#6752](https://github.com/yandex/ClickHouse/pull/6752) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid data race on Settings in KILL QUERY. [#6753](https://github.com/yandex/ClickHouse/pull/6753) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add integration test for handling errors by a cache dictionary. [#6755](https://github.com/yandex/ClickHouse/pull/6755) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix shared build with rdkafka library [#6101](https://github.com/yandex/ClickHouse/pull/6101) ([Ivan](https://github.com/abyss7))
* Move input_format_defaults_for_omitted_fields to incompatible changes [#6573](https://github.com/yandex/ClickHouse/pull/6573) ([Artem Zuikov](https://github.com/4ertus2))
* Disable parsing of ELF object files on Mac OS, because it makes no sense. [#6578](https://github.com/yandex/ClickHouse/pull/6578) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Attempt to make changelog generator better. Changelog for 19.10 and 19.11. [#6327](https://github.com/yandex/ClickHouse/pull/6327) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Adding -Wshadow switch to the GCC [#6325](https://github.com/yandex/ClickHouse/pull/6325) ([kreuzerkrieg](https://github.com/kreuzerkrieg))
* Removed obsolete code for `mimalloc` support. [#6715](https://github.com/yandex/ClickHouse/pull/6715) ([alexey-milovidov](https://github.com/alexey-milovidov))
* zlib-ng determines x86 capabilities and saves this info to global variables. This is done in defalteInit call, which may be made by different threads simultaneously. To avoid multithreaded writes, do it on library startup. [#6141](https://github.com/yandex/ClickHouse/pull/6141) ([akuzm](https://github.com/akuzm))
* Regression test for a bug which PR [#5192](https://github.com/yandex/ClickHouse/issues/5192) fixed [#6147](https://github.com/yandex/ClickHouse/pull/6147) ([Bakhtiyor Ruziev](https://github.com/theruziev))
* Fixed MSan report. [#6144](https://github.com/yandex/ClickHouse/pull/6144) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix flappy TTL test. [#6782](https://github.com/yandex/ClickHouse/pull/6782) ([Anton Popov](https://github.com/CurtizJ))
* Fixed false data race in "MergeTreeDataPart::is_frozen" field [#6583](https://github.com/yandex/ClickHouse/pull/6583) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed timeouts in fuzz test. In previous version, it managed to find false hangup in query `SELECT * FROM numbers_mt(gccMurmurHash(''))`. [#6582](https://github.com/yandex/ClickHouse/pull/6582) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added debug checks to `static_cast` of columns. [#6581](https://github.com/yandex/ClickHouse/pull/6581) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Support for Oracle Linux in official RPM packages. This fixes [#6356](https://github.com/yandex/ClickHouse/issues/6356) [#6585](https://github.com/yandex/ClickHouse/pull/6585) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changed json perftests from `once` to `loop` type. [#6536](https://github.com/yandex/ClickHouse/pull/6536) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* odbc-bridge.cpp defines main() so it should not be included in clickhouse-lib. [#6538](https://github.com/yandex/ClickHouse/pull/6538) ([Orivej Desh](https://github.com/orivej))
* Probably fixed crash [#6362](https://github.com/yandex/ClickHouse/pull/6362) ([Artem Zuikov](https://github.com/4ertus2))
* Fix build issues [#6744](https://github.com/yandex/ClickHouse/pull/6744) ([Ivan](https://github.com/abyss7))
* Added a test for the limit on expansion of aliases just in case [#6442](https://github.com/yandex/ClickHouse/pull/6442) ([alexey-milovidov](https://github.com/alexey-milovidov))
* MySQL 8 integration requires previous declaration checks [#6569](https://github.com/yandex/ClickHouse/pull/6569) ([Rafael David Tinoco](https://github.com/rafaeldtinoco))
* Switched from `boost::filesystem` to `std::filesystem` where appropriate. [#6253](https://github.com/yandex/ClickHouse/pull/6253) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added RPM packages to website. [#6251](https://github.com/yandex/ClickHouse/pull/6251) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add a test for fixed issue [#6708](https://github.com/yandex/ClickHouse/pull/6708) ([Artem Zuikov](https://github.com/4ertus2))
* test for orc input format ... [#6703](https://github.com/yandex/ClickHouse/pull/6703) ([akonyaev90](https://github.com/akonyaev90))
* Simplify `shared_ptr_helper` because people facing difficulties understanding it. [#6675](https://github.com/yandex/ClickHouse/pull/6675) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed Gorilla and DoubleDelta codec performance tests. ... [#6179](https://github.com/yandex/ClickHouse/pull/6179) ([Vasily Nemkov](https://github.com/Enmk))
* Split the integration test `test_dictionaries` into 4 separate tests. [#6776](https://github.com/yandex/ClickHouse/pull/6776) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix PVS warning in PipelineExecutor. [#6777](https://github.com/yandex/ClickHouse/pull/6777) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Look at https://github.com/yandex/ClickHouse/pull/6656#issuecomment-526954789 [#6770](https://github.com/yandex/ClickHouse/pull/6770) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to use library dictionary source with ASan. [#6482](https://github.com/yandex/ClickHouse/pull/6482) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added option to generate changelog from a list of PRs [#6350](https://github.com/yandex/ClickHouse/pull/6350) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Lock the TinyLog storage when reading. [#6226](https://github.com/yandex/ClickHouse/pull/6226) ([akuzm](https://github.com/akuzm))
* Check for broken symlinks in CI. [#6634](https://github.com/yandex/ClickHouse/pull/6634) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Increase timeout for "stack overflow" test because it may take a long time in debug build [#6637](https://github.com/yandex/ClickHouse/pull/6637) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove Compiler (runtime template instantiation) because we've win over it's performance. [#6646](https://github.com/yandex/ClickHouse/pull/6646) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added a check for double whitespaces [#6643](https://github.com/yandex/ClickHouse/pull/6643) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix for Mac OS build [#6390](https://github.com/yandex/ClickHouse/pull/6390) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix new/delete memory tracking then build with sanitizers. Tracking is not clear. It only prevents memory limit exceptions in tests. [#6450](https://github.com/yandex/ClickHouse/pull/6450) ([Artem Zuikov](https://github.com/4ertus2))
* Enable back the check of undefined symbols while linking [#6453](https://github.com/yandex/ClickHouse/pull/6453) ([Ivan](https://github.com/abyss7))
* Avoid rebuilding hyperscan each day. [#6307](https://github.com/yandex/ClickHouse/pull/6307) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added performance test to show degradation of performance in gcc-9 in more isolated way. [#6302](https://github.com/yandex/ClickHouse/pull/6302) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed UBSan report in ProtobufWriter [#6163](https://github.com/yandex/ClickHouse/pull/6163) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Don't allow to use query profiler with sanitizers because it is not compatible. [#6769](https://github.com/yandex/ClickHouse/pull/6769) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add test for reloading a dictionary after fail by timer. [#6114](https://github.com/yandex/ClickHouse/pull/6114) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix inconsistency in `PipelineExecutor::prepareProcessor` argument type. [#6494](https://github.com/yandex/ClickHouse/pull/6494) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Added a test for bad URIs [#6493](https://github.com/yandex/ClickHouse/pull/6493) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added more checks to `CAST` function. This should get more information about segmentation fault in fuzzy test. [#6346](https://github.com/yandex/ClickHouse/pull/6346) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

### Backward Incompatible Change
* Disable ANY RIGHT JOIN and ANY FULL JOIN by default. Set any_join_get_any_from_right_table setting to enable them. [#5126](https://github.com/yandex/ClickHouse/issues/5126) [#6351](https://github.com/yandex/ClickHouse/pull/6351) ([Artem Zuikov](https://github.com/4ertus2))

### Other
* Fix links in docs [#6783](https://github.com/yandex/ClickHouse/pull/6783) ([tavplubix](https://github.com/tavplubix))
* Documentation for "WITH" section of SELECT query. [#5894](https://github.com/yandex/ClickHouse/pull/5894) ([Alex Krash](https://github.com/alex-krash))
* Test added for [#5044](https://github.com/yandex/ClickHouse/issues/5044) [#6219](https://github.com/yandex/ClickHouse/pull/6219) ([dimarub2000](https://github.com/dimarub2000))
* Changelog for 19.11.7.40 and 19.13.2.19 [#6527](https://github.com/yandex/ClickHouse/pull/6527) ([tavplubix](https://github.com/tavplubix))
* Enable processor pipeline by default. [#6606](https://github.com/yandex/ClickHouse/pull/6606) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix `FormatFactory` behaviour for input streams which are not implemented as processor. [#6495](https://github.com/yandex/ClickHouse/pull/6495) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Continuation of https://github.com/yandex/ClickHouse/pull/6366 [#6669](https://github.com/yandex/ClickHouse/pull/6669) ([alesapin](https://github.com/alesapin))

### [No category]
* Release pull request for branch 19.14 [#6847](https://github.com/yandex/ClickHouse/pull/6847) ([robot-clickhouse](https://github.com/robot-clickhouse))
* title case fixes [#6792](https://github.com/yandex/ClickHouse/pull/6792) ([Ivan Blinkov](https://github.com/blinkov))
* fix note markdown [#6791](https://github.com/yandex/ClickHouse/pull/6791) ([Ivan Blinkov](https://github.com/blinkov))
* Fix live view no users thread [#6656](https://github.com/yandex/ClickHouse/pull/6656) ([vzakaznikov](https://github.com/vzakaznikov))
* Make CONSTRAINTs production ready. [#6652](https://github.com/yandex/ClickHouse/pull/6652) ([alexey-milovidov](https://github.com/alexey-milovidov))
* fix Set index check useless [#6651](https://github.com/yandex/ClickHouse/pull/6651) ([Nikita Vasilev](https://github.com/nikvas0))
* This fixes [#5816](https://github.com/yandex/ClickHouse/issues/5816) [#6591](https://github.com/yandex/ClickHouse/pull/6591) ([dimarub2000](https://github.com/dimarub2000))
* Add meetup in Moscow to front page [#6270](https://github.com/yandex/ClickHouse/pull/6270) ([Ivan Blinkov](https://github.com/blinkov))
* Make PairNoInit a simple struct. [#6277](https://github.com/yandex/ClickHouse/pull/6277) ([akuzm](https://github.com/akuzm))
* Add links to hits_100m dataset. [#6379](https://github.com/yandex/ClickHouse/pull/6379) ([akuzm](https://github.com/akuzm))
* Update upcoming meetups [#6206](https://github.com/yandex/ClickHouse/pull/6206) ([Ivan Blinkov](https://github.com/blinkov))
* Translate for data_types/domains/ [#6202](https://github.com/yandex/ClickHouse/pull/6202) ([Big Elephant](https://github.com/DaisyFlower))
* Basic code quality of LiveView [#6619](https://github.com/yandex/ClickHouse/pull/6619) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix splitted build. [#6618](https://github.com/yandex/ClickHouse/pull/6618) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Doc change. Example of another approach to collapsing (using negative numbers). [#6751](https://github.com/yandex/ClickHouse/pull/6751) ([Denis Zhuravlev](https://github.com/den-crane))
* Get rid of dynamic allocation in ParsedJson::Iterator. [#6479](https://github.com/yandex/ClickHouse/pull/6479) ([Vitaly Baranov](https://github.com/vitlibar))
* Text log simplification [#6322](https://github.com/yandex/ClickHouse/pull/6322) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Every function in its own file, part 10 [#6321](https://github.com/yandex/ClickHouse/pull/6321) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added missing [Primary Key expr] to syntax [#6328](https://github.com/yandex/ClickHouse/pull/6328) ([Ramazan Polat](https://github.com/ramazanpolat))
* Doc change. Example of max_block_size impact on runningDifference. [#6786](https://github.com/yandex/ClickHouse/pull/6786) ([Denis Zhuravlev](https://github.com/den-crane))
* Doc change. identity function + translation to russian for throwIf / modelEvaluate [#6788](https://github.com/yandex/ClickHouse/pull/6788) ([Denis Zhuravlev](https://github.com/den-crane))
* Documentation change. range_hashed dict arbitrary type description. [#6660](https://github.com/yandex/ClickHouse/pull/6660) ([Denis Zhuravlev](https://github.com/den-crane))
* Add Comments for set index functions [#6319](https://github.com/yandex/ClickHouse/pull/6319) ([Nikita Vasilev](https://github.com/nikvas0))
* Metric log rectification [#6530](https://github.com/yandex/ClickHouse/pull/6530) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changelog 19.11.5. [#6360](https://github.com/yandex/ClickHouse/pull/6360) ([alesapin](https://github.com/alesapin))
* Add link to Mountain View meetup [#6218](https://github.com/yandex/ClickHouse/pull/6218) ([Ivan Blinkov](https://github.com/blinkov))
* Sync RPM packages instructions to other docs languages [#6568](https://github.com/yandex/ClickHouse/pull/6568) ([Ivan Blinkov](https://github.com/blinkov))
* remove doubled const TABLE_IS_READ_ONLY [#6566](https://github.com/yandex/ClickHouse/pull/6566) ([filimonov](https://github.com/filimonov))
* Added RPM packages to english docs [#6310](https://github.com/yandex/ClickHouse/pull/6310) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Simplification of [#3796](https://github.com/yandex/ClickHouse/issues/3796) [#6316](https://github.com/yandex/ClickHouse/pull/6316) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Using Danila Kutenin variant to make fastops working [#6317](https://github.com/yandex/ClickHouse/pull/6317) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed tests affected by slow stack traces printing [#6315](https://github.com/yandex/ClickHouse/pull/6315) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Formatting changes for StringHashMap PR [#5417](https://github.com/yandex/ClickHouse/issues/5417). [#6700](https://github.com/yandex/ClickHouse/pull/6700) ([akuzm](https://github.com/akuzm))
* Revert "Merge pull request [#5710](https://github.com/yandex/ClickHouse/issues/5710) from filimonov/query_masking" [#6821](https://github.com/yandex/ClickHouse/pull/6821) ([Ivan](https://github.com/abyss7))
* Remove a redundant condition (found by PVS Studio). [#6775](https://github.com/yandex/ClickHouse/pull/6775) ([akuzm](https://github.com/akuzm))
* Fix materialized view with column defaults. [#3796](https://github.com/yandex/ClickHouse/pull/3796) ([Amos Bird](https://github.com/amosbird))
* Separate the hash table interface for ReverseIndex. [#6672](https://github.com/yandex/ClickHouse/pull/6672) ([akuzm](https://github.com/akuzm))
* Just refactoring. [#6689](https://github.com/yandex/ClickHouse/pull/6689) ([alesapin](https://github.com/alesapin))
* Continuation of https://github.com/yandex/ClickHouse/pull/6669 [#6685](https://github.com/yandex/ClickHouse/pull/6685) ([alesapin](https://github.com/alesapin))
* [website] take upcoming meetups from README.md [#6520](https://github.com/yandex/ClickHouse/pull/6520) ([Ivan Blinkov](https://github.com/blinkov))
* build fix [#6486](https://github.com/yandex/ClickHouse/pull/6486) ([vxider](https://github.com/Vxider))
* Some more translation of ToC to Chinese [#6487](https://github.com/yandex/ClickHouse/pull/6487) ([Ivan Blinkov](https://github.com/blinkov))
* Add Paris Meetup link to front page [#6485](https://github.com/yandex/ClickHouse/pull/6485) ([Ivan Blinkov](https://github.com/blinkov))
* Skip introduction/info.md in single page docs [#6292](https://github.com/yandex/ClickHouse/pull/6292) ([Ivan Blinkov](https://github.com/blinkov))
* Fix build under gcc-8.2 [#6196](https://github.com/yandex/ClickHouse/pull/6196) ([Max Akhmedov](https://github.com/zlobober))
* Fix some spelling mistakes in chinese doc [#6190](https://github.com/yandex/ClickHouse/pull/6190) ([XuYi](https://github.com/MyXOF))
* indices and mutations [#5053](https://github.com/yandex/ClickHouse/pull/5053) ([Nikita Vasilev](https://github.com/nikvas0))
* Fix typo [#6631](https://github.com/yandex/ClickHouse/pull/6631) ([Alex Ryndin](https://github.com/alexryndin))
* Revert "Translate database engine documentation(zh)" [#6633](https://github.com/yandex/ClickHouse/pull/6633) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add a test case for [#4402](https://github.com/yandex/ClickHouse/issues/4402). [#6129](https://github.com/yandex/ClickHouse/pull/6129) ([akuzm](https://github.com/akuzm))
* Fixed indices mutations tests [#6645](https://github.com/yandex/ClickHouse/pull/6645) ([Nikita Vasilev](https://github.com/nikvas0))
* Build fixes [#6016](https://github.com/yandex/ClickHouse/pull/6016) ([proller](https://github.com/proller))
* Attempt to fix performance test [#6392](https://github.com/yandex/ClickHouse/pull/6392) ([alexey-milovidov](https://github.com/alexey-milovidov))
* typechecks for set index functions [#6511](https://github.com/yandex/ClickHouse/pull/6511) ([Nikita Vasilev](https://github.com/nikvas0))
* Fix old release versions in changelog [#6424](https://github.com/yandex/ClickHouse/pull/6424) ([Olga Khvostikova](https://github.com/stavrolia))
* In performance test, do not read query log for queries we didn't run. [#6427](https://github.com/yandex/ClickHouse/pull/6427) ([akuzm](https://github.com/akuzm))
* Build fixes [#6421](https://github.com/yandex/ClickHouse/pull/6421) ([proller](https://github.com/proller))
* 1. Fixed throwFromErrno invocations for Mac OS that has been broken after recent refactoring. ... [#6429](https://github.com/yandex/ClickHouse/pull/6429) ([alex-zaitsev](https://github.com/alex-zaitsev))
* Typo in the error message ( is -> are ) [#6839](https://github.com/yandex/ClickHouse/pull/6839) ([Denis Zhuravlev](https://github.com/den-crane))
* Doc change. added note about ArrayElement index 0 case. [#6838](https://github.com/yandex/ClickHouse/pull/6838) ([Denis Zhuravlev](https://github.com/den-crane))
* Merging "support push down predicate to final subquery" [#6162](https://github.com/yandex/ClickHouse/pull/6162) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Merging system text_log [#6164](https://github.com/yandex/ClickHouse/pull/6164) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Replace YouTube link with human-readable one [#6118](https://github.com/yandex/ClickHouse/pull/6118) ([Ivan Blinkov](https://github.com/blinkov))
* Release pull request for branch 19.13 [#6112](https://github.com/yandex/ClickHouse/pull/6112) ([robot-clickhouse](https://github.com/robot-clickhouse))
* Merging "check free space when use external sort/aggerator" [#6691](https://github.com/yandex/ClickHouse/pull/6691) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Build fixes [#6491](https://github.com/yandex/ClickHouse/pull/6491) ([proller](https://github.com/proller))
* build fix [#6348](https://github.com/yandex/ClickHouse/pull/6348) ([vxider](https://github.com/Vxider))
* build fix [#6186](https://github.com/yandex/ClickHouse/pull/6186) ([Amos Bird](https://github.com/amosbird))
* Add link to new third-party jdbc driver [#6793](https://github.com/yandex/ClickHouse/pull/6793) ([Ivan Blinkov](https://github.com/blinkov))
* revert [#6544](https://github.com/yandex/ClickHouse/issues/6544) [#6547](https://github.com/yandex/ClickHouse/pull/6547) ([Ivan Blinkov](https://github.com/blinkov))
* Update roadmap.md [#6545](https://github.com/yandex/ClickHouse/pull/6545) ([Ivan Blinkov](https://github.com/blinkov))
* [experimental] auto-mark documentation PRs with labels [#6544](https://github.com/yandex/ClickHouse/pull/6544) ([Ivan Blinkov](https://github.com/blinkov))
* Add link to Hong Kong Meetup to website front page [#6465](https://github.com/yandex/ClickHouse/pull/6465) ([Ivan Blinkov](https://github.com/blinkov))
* Added 'strict' parameter in windowFunnel() and added testcases. [#6548](https://github.com/yandex/ClickHouse/pull/6548) ([achimbab](https://github.com/achimbab))
* Doc change. FROM FINAL can be used not only with CollapsingMergeTree [#6789](https://github.com/yandex/ClickHouse/pull/6789) ([Denis Zhuravlev](https://github.com/den-crane))

### Bug Fix. Small one.
* Materialized view now could be created with any low cardinality types regardless to the setting about suspicious low cardinality types. [#6428](https://github.com/yandex/ClickHouse/pull/6428) ([Olga Khvostikova](https://github.com/stavrolia))

### Bugfix
* Fixed wrong code in mutations that may lead to memory corruption. Fixed segfault with read of address `0x14c0` that may happed due to concurrent `DROP TABLE` and `SELECT` from `system.parts` or `system.parts_columns`. Fixed race condition in preparation of mutation queries. Fixed deadlock caused by `OPTIMIZE` of Replicated tables and concurrent modification operations like ALTERs. [#6514](https://github.com/yandex/ClickHouse/pull/6514) ([alexey-milovidov](https://github.com/alexey-milovidov))

### Building/Testing/Packaging Improvement
* Updated tests for `send_logs_level` setting. [#6207](https://github.com/yandex/ClickHouse/pull/6207) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

### Document
* continue translate domain data type [#6370](https://github.com/yandex/ClickHouse/pull/6370) ([Winter Zhang](https://github.com/zhang2014))
* Translate database engine documentation(zh) [#6625](https://github.com/yandex/ClickHouse/pull/6625) ([Winter Zhang](https://github.com/zhang2014))
* Translate database engine documentation [#6649](https://github.com/yandex/ClickHouse/pull/6649) ([Winter Zhang](https://github.com/zhang2014))
* Fixed link in the logger word ... [#6345](https://github.com/yandex/ClickHouse/pull/6345) ([Francisco Barón ](https://github.com/Keralin))
* continue [#6202](https://github.com/yandex/ClickHouse/issues/6202) [#6338](https://github.com/yandex/ClickHouse/pull/6338) ([Winter Zhang](https://github.com/zhang2014))

### Solution for [#6137](https://github.com/yandex/ClickHouse/issues/6137)
* DOCAPI-7783: RU Translation for SET query description [#6623](https://github.com/yandex/ClickHouse/pull/6623) ([BayoNet](https://github.com/BayoNet))
* DOCAPI-7783: Update of the SET query documentation [#6165](https://github.com/yandex/ClickHouse/pull/6165) ([BayoNet](https://github.com/BayoNet))

### SQL compatibility
* Correct implementation of ternary logic for AND/OR [#6048](https://github.com/yandex/ClickHouse/pull/6048) ([Alexander Kazakov](https://github.com/Akazz))

Commits which are not from any pull request:

Commit: [6f1a8c37abe6ee4e7ee74c0b5cb9c05a87417b61](https://github.com/yandex/ClickHouse/commit/6f1a8c37abe6ee4e7ee74c0b5cb9c05a87417b61)
Author: [Ivan](https://github.com/abyss7)
Message: Print API costs in utils/github

Commit: [89a444c4a026c36421ed4bd414746f04c1be0af9](https://github.com/yandex/ClickHouse/commit/89a444c4a026c36421ed4bd414746f04c1be0af9)
Author: [Ivan](https://github.com/abyss7)
Message: Fix script for releases

Commit: [2cda8f1563e9b2d11f75c68db817c3f68ace1160](https://github.com/yandex/ClickHouse/commit/2cda8f1563e9b2d11f75c68db817c3f68ace1160)
Author: [alesapin](https://github.com/alesapin)
Message: Add query_masking_rules xml's into debug images

Commit: [657c83219d9a73ea7c142c26ada28674652e43f5](https://github.com/yandex/ClickHouse/commit/657c83219d9a73ea7c142c26ada28674652e43f5)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Delete bug_report.md

Commit: [c7e446e2fd62472fd3f0e1dce95f0f2291cfcaeb](https://github.com/yandex/ClickHouse/commit/c7e446e2fd62472fd3f0e1dce95f0f2291cfcaeb)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Update issue templates

Commit: [6cf5a90c7b16bce1da9f593e90a2c13253c252af](https://github.com/yandex/ClickHouse/commit/6cf5a90c7b16bce1da9f593e90a2c13253c252af)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add documentation issue template

Commit: [8a3841724d8b1bed09c93e45793416327c2232fa](https://github.com/yandex/ClickHouse/commit/8a3841724d8b1bed09c93e45793416327c2232fa)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add performance issue template

Commit: [6f1a8c37abe6ee4e7ee74c0b5cb9c05a87417b61](https://github.com/yandex/ClickHouse/commit/6f1a8c37abe6ee4e7ee74c0b5cb9c05a87417b61)
Author: [Ivan](https://github.com/abyss7)
Message: Print API costs in utils/github

Commit: [89a444c4a026c36421ed4bd414746f04c1be0af9](https://github.com/yandex/ClickHouse/commit/89a444c4a026c36421ed4bd414746f04c1be0af9)
Author: [Ivan](https://github.com/abyss7)
Message: Fix script for releases

Commit: [2cda8f1563e9b2d11f75c68db817c3f68ace1160](https://github.com/yandex/ClickHouse/commit/2cda8f1563e9b2d11f75c68db817c3f68ace1160)
Author: [alesapin](https://github.com/alesapin)
Message: Add query_masking_rules xml's into debug images

Commit: [657c83219d9a73ea7c142c26ada28674652e43f5](https://github.com/yandex/ClickHouse/commit/657c83219d9a73ea7c142c26ada28674652e43f5)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Delete bug_report.md

Commit: [c7e446e2fd62472fd3f0e1dce95f0f2291cfcaeb](https://github.com/yandex/ClickHouse/commit/c7e446e2fd62472fd3f0e1dce95f0f2291cfcaeb)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Update issue templates

Commit: [6cf5a90c7b16bce1da9f593e90a2c13253c252af](https://github.com/yandex/ClickHouse/commit/6cf5a90c7b16bce1da9f593e90a2c13253c252af)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add documentation issue template

Commit: [8a3841724d8b1bed09c93e45793416327c2232fa](https://github.com/yandex/ClickHouse/commit/8a3841724d8b1bed09c93e45793416327c2232fa)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add performance issue template

Commit: [a4c1d64abac1c6efc2db8931aaf956500b2b05fb](https://github.com/yandex/ClickHouse/commit/a4c1d64abac1c6efc2db8931aaf956500b2b05fb)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Allowed to run test with the server run from source tree

Commit: [f2c2a2a9ea2eaab5f1eb52912235a012fd1a1858](https://github.com/yandex/ClickHouse/commit/f2c2a2a9ea2eaab5f1eb52912235a012fd1a1858)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed unit test

Commit: [9cf0c780fbbeb9ec26158ba18ba67ba81aec63b3](https://github.com/yandex/ClickHouse/commit/9cf0c780fbbeb9ec26158ba18ba67ba81aec63b3)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Better code

Commit: [457bc541eede10efed161b4c5ba1c291a2aabd82](https://github.com/yandex/ClickHouse/commit/457bc541eede10efed161b4c5ba1c291a2aabd82)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [2b0af524cbe8f5221618069aeb2b0a304395992d](https://github.com/yandex/ClickHouse/commit/2b0af524cbe8f5221618069aeb2b0a304395992d)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added a test [#2282](https://github.com/yandex/ClickHouse/issues/2282)

Commit: [8917572087db020a2c9c4ec180fb34c28b4714de](https://github.com/yandex/ClickHouse/commit/8917572087db020a2c9c4ec180fb34c28b4714de)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Speed up another test in cases when query_log is large (such as on production servers)

Commit: [55f11b367575bb9f174649afbf0180c4172a0883](https://github.com/yandex/ClickHouse/commit/55f11b367575bb9f174649afbf0180c4172a0883)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Speed up test for "text_log" in cases when text_log is large (such as on production servers)

Commit: [4d21f97d6aeefeafa92b76370e06d7a3ff48f9db](https://github.com/yandex/ClickHouse/commit/4d21f97d6aeefeafa92b76370e06d7a3ff48f9db)
Author: [Ivan](https://github.com/abyss7)
Message: Increase test timeout

Commit: [dcc6163d326965811f759b42032d9735afd3f998](https://github.com/yandex/ClickHouse/commit/dcc6163d326965811f759b42032d9735afd3f998)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added function "trap"

Commit: [c0e465f9f00116ce636f5e52932767c8290fcd65](https://github.com/yandex/ClickHouse/commit/c0e465f9f00116ce636f5e52932767c8290fcd65)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Stress test: more beautiful

Commit: [57c8091e5b680ab1d9e32b147e4abf0f2b17a273](https://github.com/yandex/ClickHouse/commit/57c8091e5b680ab1d9e32b147e4abf0f2b17a273)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Better stress test script

Commit: [5fcdd6f20b58ad2b0b5da4d4ab0373e4ceefe8eb](https://github.com/yandex/ClickHouse/commit/5fcdd6f20b58ad2b0b5da4d4ab0373e4ceefe8eb)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added stress test variant that is as simple to run as ./stress

Commit: [83c75ca2adfa34c256628077232f8131199accf5](https://github.com/yandex/ClickHouse/commit/83c75ca2adfa34c256628077232f8131199accf5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added a test (but it doesn't reproduce the issue [#6746](https://github.com/yandex/ClickHouse/issues/6746))

Commit: [0bca68e50b540608832b17202ceec6a2d7a5cba4](https://github.com/yandex/ClickHouse/commit/0bca68e50b540608832b17202ceec6a2d7a5cba4)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Style

Commit: [6d6851e747058c9fb2f0180900d6d0b735e2c1c3](https://github.com/yandex/ClickHouse/commit/6d6851e747058c9fb2f0180900d6d0b735e2c1c3)
Author: [alesapin](https://github.com/alesapin)
Message: Add capnp-library do build image

Commit: [d3b378ea10bb5df25e7f83dce9311c9c4c30e961](https://github.com/yandex/ClickHouse/commit/d3b378ea10bb5df25e7f83dce9311c9c4c30e961)
Author: [alesapin](https://github.com/alesapin)
Message: Fix flappy test (descrease number of iterations)

Commit: [06ea75f9fd5581fdeb10ab264ed73f159ab3797e](https://github.com/yandex/ClickHouse/commit/06ea75f9fd5581fdeb10ab264ed73f159ab3797e)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [e9875950a488eaae51488f69e8f2274ed4343b61](https://github.com/yandex/ClickHouse/commit/e9875950a488eaae51488f69e8f2274ed4343b61)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Make test timeout to be more significant

Commit: [48dce81e61699f956d1650a99056b53f2534027c](https://github.com/yandex/ClickHouse/commit/48dce81e61699f956d1650a99056b53f2534027c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Minor modifications after [#6413](https://github.com/yandex/ClickHouse/issues/6413)

Commit: [6d6c53d42bb251e185594ab971c23645ad35ca3b](https://github.com/yandex/ClickHouse/commit/6d6c53d42bb251e185594ab971c23645ad35ca3b)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Style

Commit: [f2d081a7857064e3af74e8e41f076810e302361b](https://github.com/yandex/ClickHouse/commit/f2d081a7857064e3af74e8e41f076810e302361b)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Addition to prev. revision

Commit: [3db38c690e5c012afa2a5623c50d4b10cbc8fbdf](https://github.com/yandex/ClickHouse/commit/3db38c690e5c012afa2a5623c50d4b10cbc8fbdf)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Changes to Benchmark after merge

Commit: [63c0070cd50aaa463cc00eedee4c43868ba8c69f](https://github.com/yandex/ClickHouse/commit/63c0070cd50aaa463cc00eedee4c43868ba8c69f)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed flacky test

Commit: [66203973f20856a7a7337eddf870eb9ec5a3c561](https://github.com/yandex/ClickHouse/commit/66203973f20856a7a7337eddf870eb9ec5a3c561)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [7985270624e877de15f958f0e3f19a23365f34de](https://github.com/yandex/ClickHouse/commit/7985270624e877de15f958f0e3f19a23365f34de)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Disable processors by default

Commit: [d654f2507eb0b1a1c60fb266b0b5812c79922330](https://github.com/yandex/ClickHouse/commit/d654f2507eb0b1a1c60fb266b0b5812c79922330)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed typo in test

Commit: [f10cf3b0829ab1643e3e8f5d88de5ec70fd9d2e4](https://github.com/yandex/ClickHouse/commit/f10cf3b0829ab1643e3e8f5d88de5ec70fd9d2e4)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [cd620d2de517acff4943443a25fac71ac063b068](https://github.com/yandex/ClickHouse/commit/cd620d2de517acff4943443a25fac71ac063b068)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed race condition in test (once again)

Commit: [75e124f3909d1a6820938986f9d7f82b05acb309](https://github.com/yandex/ClickHouse/commit/75e124f3909d1a6820938986f9d7f82b05acb309)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed misleading flag from CMake

Commit: [abdd70fcc4b02297618c8ed29751e6bbff917fa1](https://github.com/yandex/ClickHouse/commit/abdd70fcc4b02297618c8ed29751e6bbff917fa1)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed "splitted" build

Commit: [dae2aa61387a619cf5db94d8a046d5c3353fde37](https://github.com/yandex/ClickHouse/commit/dae2aa61387a619cf5db94d8a046d5c3353fde37)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed useless code

Commit: [84b0f709aa0c1b26d15e4de0abea45c865fb1934](https://github.com/yandex/ClickHouse/commit/84b0f709aa0c1b26d15e4de0abea45c865fb1934)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed useless code

Commit: [4366791b6305ca6f842d01c3b8d5edc60eda7450](https://github.com/yandex/ClickHouse/commit/4366791b6305ca6f842d01c3b8d5edc60eda7450)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'table-constraints' of https://github.com/NanoBjorn/ClickHouse into NanoBjorn-table-constraints

Commit: [3d8613f8dfe1750e7f7c5be0b71921db2b2cea2a](https://github.com/yandex/ClickHouse/commit/3d8613f8dfe1750e7f7c5be0b71921db2b2cea2a)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: More tests

Commit: [20b9af29f555f6e125c811e95cc19c39ca7857d1](https://github.com/yandex/ClickHouse/commit/20b9af29f555f6e125c811e95cc19c39ca7857d1)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: More tests

Commit: [6685365ab8c5b74f9650492c88a012596eb1b0c6](https://github.com/yandex/ClickHouse/commit/6685365ab8c5b74f9650492c88a012596eb1b0c6)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added optimized case

Commit: [341e2e4587a18065c2da1ca888c73389f48ce36c](https://github.com/yandex/ClickHouse/commit/341e2e4587a18065c2da1ca888c73389f48ce36c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Step 1: make it correct.

Commit: [ff9e92eab9ace56801f032e1bf3990217ee456d5](https://github.com/yandex/ClickHouse/commit/ff9e92eab9ace56801f032e1bf3990217ee456d5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Renamed function in test

Commit: [1222973cb3fa6e267c6bb7d29c44fef2beafe173](https://github.com/yandex/ClickHouse/commit/1222973cb3fa6e267c6bb7d29c44fef2beafe173)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Function "neighbor": merging [#5925](https://github.com/yandex/ClickHouse/issues/5925)

Commit: [99f4c9c8130a742d532294ce5edf2ac6cfd86a8c](https://github.com/yandex/ClickHouse/commit/99f4c9c8130a742d532294ce5edf2ac6cfd86a8c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Moved settings that were in a wrong place

Commit: [e3bd572fc75f20d83f421e00981524e6bcd1b8ba](https://github.com/yandex/ClickHouse/commit/e3bd572fc75f20d83f421e00981524e6bcd1b8ba)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed unused settings

Commit: [cff8ec43f992ca2daff2141ea15cd964c7401168](https://github.com/yandex/ClickHouse/commit/cff8ec43f992ca2daff2141ea15cd964c7401168)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Rename neighbour -> neighbor

Commit: [3a7e0beff639fe4555bc25c2c661ab65e4a8e213](https://github.com/yandex/ClickHouse/commit/3a7e0beff639fe4555bc25c2c661ab65e4a8e213)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [e8bc2189840613f6cbe1caaca987c293adff1b16](https://github.com/yandex/ClickHouse/commit/e8bc2189840613f6cbe1caaca987c293adff1b16)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Rewrite flappy test

Commit: [c38a8cb7555a4e55181e4bf8683192833956a968](https://github.com/yandex/ClickHouse/commit/c38a8cb7555a4e55181e4bf8683192833956a968)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [85d3ba099ff93e8edc696feaa93c579a6967ea86](https://github.com/yandex/ClickHouse/commit/85d3ba099ff93e8edc696feaa93c579a6967ea86)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added a comment

Commit: [ee89ee0218c86613db1c6856fda8fb3d1140b33b](https://github.com/yandex/ClickHouse/commit/ee89ee0218c86613db1c6856fda8fb3d1140b33b)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [833d6d60a67764ba688f4b8139a58475fb151e32](https://github.com/yandex/ClickHouse/commit/833d6d60a67764ba688f4b8139a58475fb151e32)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [bbf2911d61089d643524ef08672abf67a488677a](https://github.com/yandex/ClickHouse/commit/bbf2911d61089d643524ef08672abf67a488677a)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [19cb429b06d3ca454621f45caa8ac86e9331bcb5](https://github.com/yandex/ClickHouse/commit/19cb429b06d3ca454621f45caa8ac86e9331bcb5)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [ae7ae6d660361d25d12b0dc5f555ef924e1ffd9a](https://github.com/yandex/ClickHouse/commit/ae7ae6d660361d25d12b0dc5f555ef924e1ffd9a)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [a7fa71aaf1a19ddf2715acd2048a861afffa9e06](https://github.com/yandex/ClickHouse/commit/a7fa71aaf1a19ddf2715acd2048a861afffa9e06)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed flappy test

Commit: [a1560448d632ea5dbef1e9bbe2b66c03285598f5](https://github.com/yandex/ClickHouse/commit/a1560448d632ea5dbef1e9bbe2b66c03285598f5)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update formats.md

Commit: [51f6d9751156a506807aba97962b7ca52bbf425e](https://github.com/yandex/ClickHouse/commit/51f6d9751156a506807aba97962b7ca52bbf425e)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update formats.md

Commit: [b9870245fd5936f961487d19f53faef41a805119](https://github.com/yandex/ClickHouse/commit/b9870245fd5936f961487d19f53faef41a805119)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add upcoming ClickHouse Meetup in Munich

Commit: [f277d0ebbfb61850178684a44fd7535f5dbdf342](https://github.com/yandex/ClickHouse/commit/f277d0ebbfb61850178684a44fd7535f5dbdf342)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Style

Commit: [dd739960737efc0b48aa367f377e25e95b914684](https://github.com/yandex/ClickHouse/commit/dd739960737efc0b48aa367f377e25e95b914684)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added instruction for "list_backports" script

Commit: [074853ac0e4fbd2511b3392097ea8ed97c58d859](https://github.com/yandex/ClickHouse/commit/074853ac0e4fbd2511b3392097ea8ed97c58d859)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed flappy test

Commit: [d8683a33c86ad3a189fc15e61a91706746b98c11](https://github.com/yandex/ClickHouse/commit/d8683a33c86ad3a189fc15e61a91706746b98c11)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Lowered test scale because it is too slow in debug build

Commit: [06bb0af3869b44ceff7ce4c69c6a23fa3bb4942f](https://github.com/yandex/ClickHouse/commit/06bb0af3869b44ceff7ce4c69c6a23fa3bb4942f)
Author: [Nikolai Kochetov](https://github.com/KochetovNicolai)
Message: Fix build.

Commit: [f1fef3f1690d3f86716125eb439c34b0936ea137](https://github.com/yandex/ClickHouse/commit/f1fef3f1690d3f86716125eb439c34b0936ea137)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [e594c344f5ab9bba4ed810df9a8707c09508f161](https://github.com/yandex/ClickHouse/commit/e594c344f5ab9bba4ed810df9a8707c09508f161)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed idiotic error in system.parts

Commit: [f0a161787a740f2d69f3a026e71c26d434981b40](https://github.com/yandex/ClickHouse/commit/f0a161787a740f2d69f3a026e71c26d434981b40)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [e4dda4332ef2a9ed7e87b6c681dd2cdd74c534a4](https://github.com/yandex/ClickHouse/commit/e4dda4332ef2a9ed7e87b6c681dd2cdd74c534a4)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [4ded8deea299ac23e8a20bd1bad32d6cd5fff5c7](https://github.com/yandex/ClickHouse/commit/4ded8deea299ac23e8a20bd1bad32d6cd5fff5c7)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [d77c02ecf6ef230d453836e609daba391eb9b62e](https://github.com/yandex/ClickHouse/commit/d77c02ecf6ef230d453836e609daba391eb9b62e)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [943a7480b5829b17f20c47906c083832ff81a231](https://github.com/yandex/ClickHouse/commit/943a7480b5829b17f20c47906c083832ff81a231)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [56beb26e5dc65799897baf7feebd79c5924b2650](https://github.com/yandex/ClickHouse/commit/56beb26e5dc65799897baf7feebd79c5924b2650)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [04f553c628a061984a6846531f4af891f645a7ce](https://github.com/yandex/ClickHouse/commit/04f553c628a061984a6846531f4af891f645a7ce)
Author: [alesapin](https://github.com/alesapin)
Message: Fix images with coverage

Commit: [d7d11b85ca34a05320aa2e052af8a60a56a87713](https://github.com/yandex/ClickHouse/commit/d7d11b85ca34a05320aa2e052af8a60a56a87713)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Update version manually

Commit: [b808f2e2e8dca86c534b52c435b5f5a6f76362d2](https://github.com/yandex/ClickHouse/commit/b808f2e2e8dca86c534b52c435b5f5a6f76362d2)
Author: [alesapin](https://github.com/alesapin)
Message: Add metric log

Commit: [cf9b41549d4e390ed8fdef75ab024d9102255b28](https://github.com/yandex/ClickHouse/commit/cf9b41549d4e390ed8fdef75ab024d9102255b28)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: MetricLog: code cleanups; comments

Commit: [abfaa9620db612dab5d111c935eedd851a514e70](https://github.com/yandex/ClickHouse/commit/abfaa9620db612dab5d111c935eedd851a514e70)
Author: [Nikolai Kochetov](https://github.com/KochetovNicolai)
Message: Fix style.

Commit: [047a14a1893910b37b0fe287d012a715abd84f7a](https://github.com/yandex/ClickHouse/commit/047a14a1893910b37b0fe287d012a715abd84f7a)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: one more minor refactoring

Commit: [97d6f2218c6f2a6be7b78273eb42409ab737aabb](https://github.com/yandex/ClickHouse/commit/97d6f2218c6f2a6be7b78273eb42409ab737aabb)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: minor refactoring

Commit: [ddde50c542f1632d0ef7ce66c75b5dc10126fe9d](https://github.com/yandex/ClickHouse/commit/ddde50c542f1632d0ef7ce66c75b5dc10126fe9d)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: minor fixes in includes

Commit: [5c0c661b6e5a97bb57f9582dbee04ca33db4c021](https://github.com/yandex/ClickHouse/commit/5c0c661b6e5a97bb57f9582dbee04ca33db4c021)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add Paris Meetup link

Commit: [1edc2a264729e54bf791d5a092e5b472dc1d807a](https://github.com/yandex/ClickHouse/commit/1edc2a264729e54bf791d5a092e5b472dc1d807a)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add link to Hong Kong Meetup

Commit: [60504bc2c85928a0fe83240f2026b0705d7c56c3](https://github.com/yandex/ClickHouse/commit/60504bc2c85928a0fe83240f2026b0705d7c56c3)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Change logger_name column in text_log to LowCardinality [#6037](https://github.com/yandex/ClickHouse/issues/6037)

Commit: [67a20342cc68e94e344c75247fe598754d7646c0](https://github.com/yandex/ClickHouse/commit/67a20342cc68e94e344c75247fe598754d7646c0)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [c96fa2c08030ab0fdbd31f9b054bbaec51bbe13f](https://github.com/yandex/ClickHouse/commit/c96fa2c08030ab0fdbd31f9b054bbaec51bbe13f)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed build

Commit: [6ebd0029265b2766a4e20ce323c400da6e3cba31](https://github.com/yandex/ClickHouse/commit/6ebd0029265b2766a4e20ce323c400da6e3cba31)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed build

Commit: [1cd87078c2e363c9c77ced4b3c2ed509e0395385](https://github.com/yandex/ClickHouse/commit/1cd87078c2e363c9c77ced4b3c2ed509e0395385)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [2f73b72007fbd1fad61d62dad974d6e1fa9dddec](https://github.com/yandex/ClickHouse/commit/2f73b72007fbd1fad61d62dad974d6e1fa9dddec)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Style

Commit: [7e6b1333a1c3046e518d2713ca2724382d8b7bb6](https://github.com/yandex/ClickHouse/commit/7e6b1333a1c3046e518d2713ca2724382d8b7bb6)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Renamed Yandex CTO just in case

Commit: [98ea652ad63a2cc8037b50f1bf5eff432de0256c](https://github.com/yandex/ClickHouse/commit/98ea652ad63a2cc8037b50f1bf5eff432de0256c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Whitespaces

Commit: [86f321a7cdb56fd05ee407cb741bbc50ab36dbf1](https://github.com/yandex/ClickHouse/commit/86f321a7cdb56fd05ee407cb741bbc50ab36dbf1)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Whitespaces

Commit: [e40854d7fbe383ce39abefde03f4edc4a5c77693](https://github.com/yandex/ClickHouse/commit/e40854d7fbe383ce39abefde03f4edc4a5c77693)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [1437065c70d3586831df7b787894719f546d159c](https://github.com/yandex/ClickHouse/commit/1437065c70d3586831df7b787894719f546d159c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed tests and error messages [#6351](https://github.com/yandex/ClickHouse/issues/6351)

Commit: [40a3f779173c56c65fe2e8e770b1faf053bbdce2](https://github.com/yandex/ClickHouse/commit/40a3f779173c56c65fe2e8e770b1faf053bbdce2)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed duplicate include

Commit: [224ed0ca670ccc00921caa7f513b84bb0c28f9d9](https://github.com/yandex/ClickHouse/commit/224ed0ca670ccc00921caa7f513b84bb0c28f9d9)
Author: [Alexander Kuzmenkov](https://github.com/akuzm)
Message: Revert wrong merge commit.

This reverts commit 9cd9c694496d5ae793718c9577ec8bb3a5fedc77.

Commit: [fcb04828301d13f578d01ee8c813c3c07929e521](https://github.com/yandex/ClickHouse/commit/fcb04828301d13f578d01ee8c813c3c07929e521)
Author: [Vasily Nemkov](https://github.com/Enmk)
Message: Implement geohashesInBox function. [#6127](https://github.com/yandex/ClickHouse/issues/6127)

Commit: [8bf1af35367916edb5d30fec8a887a4efc40ccf0](https://github.com/yandex/ClickHouse/commit/8bf1af35367916edb5d30fec8a887a4efc40ccf0)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed trash symlinks [#6338](https://github.com/yandex/ClickHouse/issues/6338)

Commit: [2e870f62e44a1f07c5452c1cf3cdf68bef2ee5ec](https://github.com/yandex/ClickHouse/commit/2e870f62e44a1f07c5452c1cf3cdf68bef2ee5ec)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [19e11d6300741a6d222394c0118e126bb3101bc2](https://github.com/yandex/ClickHouse/commit/19e11d6300741a6d222394c0118e126bb3101bc2)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Updated changelog in docs

Commit: [ecb25067a5c50f95b00ff7418b00d6457ccc045f](https://github.com/yandex/ClickHouse/commit/ecb25067a5c50f95b00ff7418b00d6457ccc045f)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [c7ff51ab7bb56b426f9a13ab5effffbb3a3f8ec6](https://github.com/yandex/ClickHouse/commit/c7ff51ab7bb56b426f9a13ab5effffbb3a3f8ec6)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed russian changelog

Commit: [eadb6ef1a4afa9fb0cd8c8b76ea1dc424996948c](https://github.com/yandex/ClickHouse/commit/eadb6ef1a4afa9fb0cd8c8b76ea1dc424996948c)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Suppress PVS-Studio warning

Commit: [a19d05d6dfff3464edd344e616e815eaa65a2629](https://github.com/yandex/ClickHouse/commit/a19d05d6dfff3464edd344e616e815eaa65a2629)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed minor discrepancies

Commit: [95532f2d31a766323ba2453f2982604955304794](https://github.com/yandex/ClickHouse/commit/95532f2d31a766323ba2453f2982604955304794)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed minor discrepancies

Commit: [ef7d19e1433f18420f724c5418be87fa2330a160](https://github.com/yandex/ClickHouse/commit/ef7d19e1433f18420f724c5418be87fa2330a160)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: better JOIN exception messages [#5565](https://github.com/yandex/ClickHouse/issues/5565)

Commit: [d12018fddd73e7799c17437e8d1a90c208cd577c](https://github.com/yandex/ClickHouse/commit/d12018fddd73e7799c17437e8d1a90c208cd577c)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [cd0640073ee1fbbd62513b4173c5e9c54f51df3d](https://github.com/yandex/ClickHouse/commit/cd0640073ee1fbbd62513b4173c5e9c54f51df3d)
Author: [alesapin](https://github.com/alesapin)
Message: Update PVS-studio image

Commit: [7f68c619937fbcd9878c181f712130786a94fc9b](https://github.com/yandex/ClickHouse/commit/7f68c619937fbcd9878c181f712130786a94fc9b)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [09aaf21392bafba8f550b34afdefd91cf636a5cc](https://github.com/yandex/ClickHouse/commit/09aaf21392bafba8f550b34afdefd91cf636a5cc)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update CHANGELOG.md

Commit: [cc2744ebb9d427e6b1cac809c1ce02288aeba5f5](https://github.com/yandex/ClickHouse/commit/cc2744ebb9d427e6b1cac809c1ce02288aeba5f5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [ddf3466af3b399eca6b72bd11f540121e4f455b4](https://github.com/yandex/ClickHouse/commit/ddf3466af3b399eca6b72bd11f540121e4f455b4)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Avoid using #N in logs, because it will link bogus issues and pull requests when posted on GitHub

Commit: [3558d8f36084a9f7e3614e3bedd56073e6087593](https://github.com/yandex/ClickHouse/commit/3558d8f36084a9f7e3614e3bedd56073e6087593)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Deprecate gcc-7

Commit: [c47816f6ad80230226469783bcfc8df78b746e6a](https://github.com/yandex/ClickHouse/commit/c47816f6ad80230226469783bcfc8df78b746e6a)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Addition to prev. revision

Commit: [d4002a7188174039be276c029fb3fa47bce3e6cc](https://github.com/yandex/ClickHouse/commit/d4002a7188174039be276c029fb3fa47bce3e6cc)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Moved "ci" directory to "utils", because we are actually using different CI, but these scripts are still usable

Commit: [5414f29003f927d98d7fa00af86ebc7a39d7c7b0](https://github.com/yandex/ClickHouse/commit/5414f29003f927d98d7fa00af86ebc7a39d7c7b0)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [a3a15d3a9548c99c5235b796c07c1dbf35106057](https://github.com/yandex/ClickHouse/commit/a3a15d3a9548c99c5235b796c07c1dbf35106057)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Addition to prev. revision

Commit: [224bc4df9799df9513a276cdd618fbf4c51501ff](https://github.com/yandex/ClickHouse/commit/224bc4df9799df9513a276cdd618fbf4c51501ff)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Minor modifications + a comment [#3796](https://github.com/yandex/ClickHouse/issues/3796)

Commit: [367fc1c5c75c5059579198f3a86c64528b6a9b86](https://github.com/yandex/ClickHouse/commit/367fc1c5c75c5059579198f3a86c64528b6a9b86)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [491433b5c6e08acb9382f2a8257971daf211c3a4](https://github.com/yandex/ClickHouse/commit/491433b5c6e08acb9382f2a8257971daf211c3a4)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Updated performance test

Commit: [fe90d499a3bd81495ec747e7ab3a851dd61971f1](https://github.com/yandex/ClickHouse/commit/fe90d499a3bd81495ec747e7ab3a851dd61971f1)
Author: [alexey-milovidov](https://github.com/alexey-milovidov)
Message: Update index.html

Commit: [07273c79141294461dcea6cf129f727d141bd7d5](https://github.com/yandex/ClickHouse/commit/07273c79141294461dcea6cf129f727d141bd7d5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Instrumented ThreadPool

Commit: [9bd4d1ed3b652d571c2bb3881de68bde2bcffc6f](https://github.com/yandex/ClickHouse/commit/9bd4d1ed3b652d571c2bb3881de68bde2bcffc6f)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add meetup in Moscow

Commit: [591af05e5739d3ccd87ce2cd19d2651f3c14c5e5](https://github.com/yandex/ClickHouse/commit/591af05e5739d3ccd87ce2cd19d2651f3c14c5e5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed error with searching debug info

Commit: [bf524b44195b54dada0250b8b6fc3eed532d7af2](https://github.com/yandex/ClickHouse/commit/bf524b44195b54dada0250b8b6fc3eed532d7af2)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [63c57a4c269d37bdf41edacf528d3be30361acf5](https://github.com/yandex/ClickHouse/commit/63c57a4c269d37bdf41edacf528d3be30361acf5)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed flappy test

Commit: [e3df5a79b334f8e4a8ca5ad545f5f1b1fe900962](https://github.com/yandex/ClickHouse/commit/e3df5a79b334f8e4a8ca5ad545f5f1b1fe900962)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Updated test

Commit: [bb1909667cef0f78d292eddd7c97e98217e909ab](https://github.com/yandex/ClickHouse/commit/bb1909667cef0f78d292eddd7c97e98217e909ab)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [ae4ae9926dbf04f96c0f166ceeeab888ca334b25](https://github.com/yandex/ClickHouse/commit/ae4ae9926dbf04f96c0f166ceeeab888ca334b25)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed build with old gcc

Commit: [97e4219552f68a98239c41672cc4650876f9c490](https://github.com/yandex/ClickHouse/commit/97e4219552f68a98239c41672cc4650876f9c490)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Add link to Mountain View meetup

Commit: [24fd416084e7b1c8a03caad849b687c2a03af7b1](https://github.com/yandex/ClickHouse/commit/24fd416084e7b1c8a03caad849b687c2a03af7b1)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: remove unused ExpressionAnalyzer settings

Commit: [f79851591a5a9bb35f4c0658ff82292149d7d4b6](https://github.com/yandex/ClickHouse/commit/f79851591a5a9bb35f4c0658ff82292149d7d4b6)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: one more test

Commit: [a109339ce600835fb8437b47f8015c4e479c6311](https://github.com/yandex/ClickHouse/commit/a109339ce600835fb8437b47f8015c4e479c6311)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [fc0532a4517c0d8574c20b3b782f244077acf7c3](https://github.com/yandex/ClickHouse/commit/fc0532a4517c0d8574c20b3b782f244077acf7c3)
Author: [alesapin](https://github.com/alesapin)
Message: Add gcc-9 to build images

Commit: [ad6159f62cd12b173e78eb05a6c0aa42d3078025](https://github.com/yandex/ClickHouse/commit/ad6159f62cd12b173e78eb05a6c0aa42d3078025)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Remove past meetup

Commit: [ea6053eadc40b71408cc58f033c233c9a01df552](https://github.com/yandex/ClickHouse/commit/ea6053eadc40b71408cc58f033c233c9a01df552)
Author: [alesapin](https://github.com/alesapin)
Message: Do not report strange exception messages into JSON report from performance-test

Commit: [f6daa7e2dd11681486c396ffa4d3d198cd1b5688](https://github.com/yandex/ClickHouse/commit/f6daa7e2dd11681486c396ffa4d3d198cd1b5688)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [48a3c82f6f5c2f2df1b6eb0b970027c2deca0c0e](https://github.com/yandex/ClickHouse/commit/48a3c82f6f5c2f2df1b6eb0b970027c2deca0c0e)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed useless code from MySQLWireBlockOutputStream

Commit: [a1dacdd25f5db7cc28969b6123aad1020b8890a3](https://github.com/yandex/ClickHouse/commit/a1dacdd25f5db7cc28969b6123aad1020b8890a3)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed bad test

Commit: [c0e0166991489dea6eb1c678bc0eb2adb24848e4](https://github.com/yandex/ClickHouse/commit/c0e0166991489dea6eb1c678bc0eb2adb24848e4)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Updated contributors

Commit: [32a9b27876d2c1c409cac8026afe3c801d4207ec](https://github.com/yandex/ClickHouse/commit/32a9b27876d2c1c409cac8026afe3c801d4207ec)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Added a comment

Commit: [3988fe7fe4bb75a8ca87ecd8ac6d6060e6a88933](https://github.com/yandex/ClickHouse/commit/3988fe7fe4bb75a8ca87ecd8ac6d6060e6a88933)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed <name> from all performance tests [#6179](https://github.com/yandex/ClickHouse/issues/6179)

Commit: [febc935fa8cb0718e8ed0d046e59ef1e8659e15a](https://github.com/yandex/ClickHouse/commit/febc935fa8cb0718e8ed0d046e59ef1e8659e15a)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Revert "Removed <name> from all performance tests [#6179](https://github.com/yandex/ClickHouse/issues/6179)"

This reverts commit d61d489c2e14fc5fb16aee3be0254cf214a37818.

Commit: [97f11a6a3cdbda939ecbbd0a19e2fc44ba962499](https://github.com/yandex/ClickHouse/commit/97f11a6a3cdbda939ecbbd0a19e2fc44ba962499)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed typo [#6179](https://github.com/yandex/ClickHouse/issues/6179)

Commit: [d61d489c2e14fc5fb16aee3be0254cf214a37818](https://github.com/yandex/ClickHouse/commit/d61d489c2e14fc5fb16aee3be0254cf214a37818)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Removed <name> from all performance tests [#6179](https://github.com/yandex/ClickHouse/issues/6179)

Commit: [98c3ff92ae293a5326f188e6f8bba741e7040cf9](https://github.com/yandex/ClickHouse/commit/98c3ff92ae293a5326f188e6f8bba741e7040cf9)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed non-standard build

Commit: [0c7a6fc64bcc58a151a5b5dd59151c058b437486](https://github.com/yandex/ClickHouse/commit/0c7a6fc64bcc58a151a5b5dd59151c058b437486)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [363fca3895c30a195cb834533fa9c36493043302](https://github.com/yandex/ClickHouse/commit/363fca3895c30a195cb834533fa9c36493043302)
Author: [alesapin](https://github.com/alesapin)
Message: More integration fixes and disable kafka test test_kafka_settings_new_syntax

Commit: [74596ed8dcb5bc4422a1b88d6123a29ba0e9865a](https://github.com/yandex/ClickHouse/commit/74596ed8dcb5bc4422a1b88d6123a29ba0e9865a)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: improve test

Commit: [3eb29c195063a26b399dfe3a91ddb7dc68212c01](https://github.com/yandex/ClickHouse/commit/3eb29c195063a26b399dfe3a91ddb7dc68212c01)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Clarified comment

Commit: [1c615d263cb1001fdb95c9d99d3a67f24cdf673b](https://github.com/yandex/ClickHouse/commit/1c615d263cb1001fdb95c9d99d3a67f24cdf673b)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [fdac4f29623a32c9fa0ba5ad27ea24a51fd91279](https://github.com/yandex/ClickHouse/commit/fdac4f29623a32c9fa0ba5ad27ea24a51fd91279)
Author: [alesapin](https://github.com/alesapin)
Message: More fixes in integration tests configs

Commit: [858a8a083e72062c3cdfd054e461dbe299526e32](https://github.com/yandex/ClickHouse/commit/858a8a083e72062c3cdfd054e461dbe299526e32)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [21ef261702c0cf511a5b7d650f44bfb15d7a262f](https://github.com/yandex/ClickHouse/commit/21ef261702c0cf511a5b7d650f44bfb15d7a262f)
Author: [alesapin](https://github.com/alesapin)
Message: Fix some integration tests and make kafka tests endless

Commit: [c3f82f0b8ce8ca04d9fa04259d310efc70cf29e9](https://github.com/yandex/ClickHouse/commit/c3f82f0b8ce8ca04d9fa04259d310efc70cf29e9)
Author: [alesapin](https://github.com/alesapin)
Message: Fix indent and add init file

Commit: [50e3839157faf643698beea7a9913799cfaaa695](https://github.com/yandex/ClickHouse/commit/50e3839157faf643698beea7a9913799cfaaa695)
Author: [Artem Zuikov](https://github.com/4ertus2)
Message: NOT_UNBUNDLED -> UNBUNDLED

Commit: [e52653aa061ae224acc24a2e5717fe1850433d75](https://github.com/yandex/ClickHouse/commit/e52653aa061ae224acc24a2e5717fe1850433d75)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Miscellaneous

Commit: [9de5b0d21c5ccfda5905f9bcfd87f95ac0e32cea](https://github.com/yandex/ClickHouse/commit/9de5b0d21c5ccfda5905f9bcfd87f95ac0e32cea)
Author: [Alexey Milovidov](https://github.com/alexey-milovidov)
Message: Fixed error in test

Commit: [ba988735cc9fdade13640ae12a8d4c292f4d3f3c](https://github.com/yandex/ClickHouse/commit/ba988735cc9fdade13640ae12a8d4c292f4d3f3c)
Author: [alesapin](https://github.com/alesapin)
Message: Merge branch 'master' of github.com:yandex/ClickHouse

Commit: [db9f0cfba19093a2e11bb0fe76b8b7696158522a](https://github.com/yandex/ClickHouse/commit/db9f0cfba19093a2e11bb0fe76b8b7696158522a)
Author: [alesapin](https://github.com/alesapin)
Message: Add empty text_log file

Commit: [4b690d752b19ffa2f64da8b20708bf5ea1099d0e](https://github.com/yandex/ClickHouse/commit/4b690d752b19ffa2f64da8b20708bf5ea1099d0e)
Author: [Ivan Blinkov](https://github.com/blinkov)
Message: Replace YouTube link with human-readable one

https://www.youtube.com/c/ClickHouseDB

Commit: [6ff1dce79a3851ac5ad151966d70f219dea62c13](https://github.com/yandex/ClickHouse/commit/6ff1dce79a3851ac5ad151966d70f219dea62c13)
Author: [alesapin](https://github.com/alesapin)
Message: Backquote key in debsign

Commit: [58e03ad7b9fa54b069be71ebbdef206488100f38](https://github.com/yandex/ClickHouse/commit/58e03ad7b9fa54b069be71ebbdef206488100f38)
Author: [robot-clickhouse](https://github.com/robot-clickhouse)
Message: Auto version update to [19.13.1.1] [54425]

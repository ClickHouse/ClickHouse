---
title: 'How to speed up LZ4 decompression in ClickHouse?'
image: 'https://avatars.mds.yandex.net/get-yablogs/38241/file_1561390681698/orig'
date: '2019-06-25'
tags: ['performance', 'lz4', 'article', 'decompression']
---

When you run queries in [ClickHouse](https://clickhouse.tech/), you might notice that the profiler often shows the `LZ_decompress_fast` function near the top. What is going on? This question had us wondering how to choose the best compression algorithm.

ClickHouse stores data in compressed form. When running queries, ClickHouse tries to do as little as possible, in order to conserve CPU resources. In many cases, all the potentially time-consuming computations are already well optimized, plus the user wrote a well thought-out query. Then all that's left to do is to perform decompression.

[Read further](https://habr.com/en/company/yandex/blog/457612/)

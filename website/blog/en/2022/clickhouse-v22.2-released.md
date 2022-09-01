---
title: 'ClickHouse 22.2 Released'
image: 'https://blog-images.clickhouse.com/en/2022/clickhouse-v22-2/featured.jpg'
date: '2022-02-23'
author: 'Alexey Milovidov'
tags: ['company', 'community']
---

We prepared a new ClickHouse release 22.2, so it's nice if you have tried it on 2022-02-22. If not, you can try it today. This latest release includes 2,140 new commits from 118 contributors, including 41 new contributors:

> Aaron Katz, Andre Marianiello, Andrew, Andrii Buriachevskyi, Brian Hunter, CoolT2, Federico Rodriguez, Filippov Denis, Gaurav Kumar, Geoff Genz, HarryLeeIBM, Heena Bansal, ILya Limarenko, Igor Nikonov, IlyaTsoi, Jake Liu, JaySon-Huang, Lemore, Leonid Krylov, Michail Safronov, Mikhail Fursov, Nikita, RogerYK, Roy Bellingan, Saad Ur Rahman, W, Yakov Olkhovskiy, alexeypavlenko, cnmade, grantovsky, hanqf-git, liuneng1994, mlkui, s-kat, tesw yew isal, vahid-sohrabloo, yakov-olkhovskiy, zhifeng, zkun, zxealous, 박동철.

Let me tell you what is most interesting in 22.2...

## Projections are production ready

Projections allow you to have multiple data representations in the same table. For example, you can have data aggregations along with the raw data. There are no restrictions on which aggregate functions can be used - you can have count distinct, quantiles, or whatever you want. You can have data in multiple different sorting orders. ClickHouse will automatically select the most suitable projection for your query, so the query will be automatically optimized.

Projections are somewhat similar to Materialized Views, which also allow you to have incremental aggregation and multiple sorting orders. But unlike Materialized Views, projections are updated atomically and consistently with the main table. The data for projections is being stored in the same "data parts" of the table and is being merged in the same way as the main data.

The feature was developed by **Amos Bird**, a prominent ClickHouse contributor. The [prototype](https://github.com/ClickHouse/ClickHouse/pull/20202) has been available since Feb 2021, it has been merged in the main codebase by **Nikolai Kochetov** in May 2021 under experimental flag, and after 21 follow-up pull requests we ensured that it passed the full set of test suites and enabled it by default.

Read an example of how to optimize queries with projections [in our docs](https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid/#speedup-with-projections).

## Control of file creation and rewriting on data export

When you export your data with an `INSERT INTO TABLE FUNCTION` statement into `file`, `s3` or `hdfs` and the target file already exists, you can now control how to deal with it: you can append new data into the file if it is possible, rewrite it with new data, or create another file with a similar name like 'data.1.parquet.gz'. 

Some storage systems like `s3` and some formats like `Parquet` don't support data appending. In previous ClickHouse versions, if you insert multiple times into a file with Parquet data format, you will end up with a file that is not recognized by other systems. Now you can choose between throwing exceptions on subsequent inserts or creating more files.

So, new settings were introduced: `s3_truncate_on_insert`, `s3_create_new_file_on_insert`, `hdfs_truncate_on_insert`, `hdfs_create_new_file_on_insert`, `engine_file_allow_create_multiple_files`.

This feature [was developed](https://github.com/ClickHouse/ClickHouse/pull/33302) by **Pavel Kruglov**.

## Custom deduplication token

`ReplicatedMergeTree` and `MergeTree` types of tables implement block-level deduplication. When a block of data is inserted, its cryptographic hash is calculated and if the same block was already inserted before, then the duplicate is skipped and the insert query succeeds. This makes it possible to implement exactly-once semantics for inserts.

In ClickHouse version 22.2 you can provide your own deduplication token instead of an automatically calculated hash. This makes sense if you already have batch identifiers from some other system and you want to reuse them. It also makes sense when blocks can be identical but they should actually be inserted multiple times. Or the opposite - when blocks contain some random data and you want to deduplicate only by significant columns.

This is implemented by adding the setting `insert_deduplication_token`. The feature was contributed by **Igor Nikonov**. 

## DEFAULT keyword for INSERT

A small addition for SQL compatibility - now we allow using the `DEFAULT` keyword instead of a value in `INSERT INTO ... VALUES` statement. It looks like this: 

`INSERT INTO test VALUES (1, 'Hello', DEFAULT)`

Thanks to **Andrii Buriachevskyi** for this feature. 

## EPHEMERAL columns

A column in a table can have a `DEFAULT` expression like `c INT DEFAULT a + b`. In ClickHouse you can also use `MATERIALIZED` instead of `DEFAULT` if you want the column to be always calculated with the provided expression instead of allowing a user to insert data. And you can use `ALIAS` if you don't want the column to be stored at all but instead to be calculated on the fly if referenced.

Since version 22.2 a new type of column is added: `EPHEMERAL` column. The user can insert data into this column but the column is not stored in a table, it's ephemeral. The purpose of this column is to provide data to calculate other columns that can reference it with `DEFAULT` or `MATERIALIZED` expressions.

This feature was made by **Yakov Olkhovskiy**.

## Improvements for multi-disk configuration

You can configure multiple disks to store ClickHouse data instead of managing RAID and ClickHouse will automatically manage the data placement.

Since version 22.2 ClickHouse can automatically repair broken disks without server restart by downloading the missing parts from replicas and placing them on the healthy disks.

This feature was implemented by **Amos Bird** and is already being used for more than 1.5 years in production at Kuaishou.

Another improvement is the option to specify TTL MOVE TO DISK/VOLUME **IF EXISTS**. It allows replicas with non-uniform disk configuration and to have one replica to move old data to cold storage while another replica has all the data on hot storage. Data will be moved only on replicas that have the specified disk or volume, hence *if exists*. This was developed by **Anton Popov**.

## Flexible memory limits

We split per-query and per-user memory limits into a pair of hard and soft limits. The settings `max_memory_usage` and `max_memory_usage_for_user` act as hard limits. When memory consumption is approaching the hard limit, an exception will be thrown. Two other settings: `max_guaranteed_memory_usage` and `max_guaranteed_memory_usage_for_user` act as soft limits.

A query will be allowed to use more memory than a soft limit if there is available memory. But if there will be memory shortage (relative to the per-user hard limit or total per-server memory consumption), we calculate the "overcommit ratio" - how much more memory every query is consuming relative to the soft limit - and we will kill the most overcommitted query to let other queries run.
 
In short, your query will not be limited to a few gigabytes of RAM if you have hundreds of gigabytes available.

This experimental feature was implemented by **Dmitry Novik** and is continuing to be developed.

## Shell-style comments in SQL

Now we allow comments starting with `# ` or `#!`, similar to MySQL. The variant with `#!` allows using shell scripts with "shebang" interpreted by `clickhouse-local`.

This feature was contributed by **Aaron Katz**. Very nice.  


## And many more...

Maxim Kita, Danila Kutenin, Anton Popov, zhanglistar, Federico Rodriguez, Raúl Marín, Amos Bird and Alexey Milovidov have contributed a ton of performance optimizations for this release. We are obsessed with high performance, as usual. :)

Read the [full changelog](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md) for the 22.2 release and follow [the roadmap](https://github.com/ClickHouse/ClickHouse/issues/32513).

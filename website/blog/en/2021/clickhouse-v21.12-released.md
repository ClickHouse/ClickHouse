---
title: 'ClickHouse v21.12 Released'
image: 'https://blog-images.clickhouse.com/en/2021/clickhouse-v21-12/featured.jpg'
date: '2021-12-13'
author: '[Rich Raposa](https://github.com/rfraposa), [Alexey Milovidov](https://github.com/alexey-milovidov)'
tags: ['company', 'community']
---

We're continuing our monthly release cadence and blog updates at[ ClickHouse, Inc](https://clickhouse.com/blog/en/2021/clickhouse-inc/). Let's highlight some of these new exciting new capabilities in 21.12:

## Bool data type

ClickHouse now natively supports a `bool` data type. Supported input values are “true”/”false”, “TRUE”/”FALSE” and “1”/”0”. By default, the text representation for CSV/TSV output is “true” and “false” but can be adjusted to anything else using the settings `bool_true_representation` and `bool_false_representation` (for example, “yes” and “no”).

**How does this help you?**

Native boolean data types exist today in other databases that are often integrated with ClickHouse, such as PostgreSQL. The `bool` data type in ClickHouse will make it more compatible with existing code and ease migration from other databases.


## Partitions for File, URL, and HDFS storage

When using the table engines file, url, and hdfs ClickHouse now supports partitions. When creating a table you can specify the partition key using the `PARTITION BY` clause e.g. `CREATE TABLE hits_files (&lt;columns>) ENGINE=File(TabSeparated) PARTITION BY toYYYYMM(EventDate)`.

Similarly, when exporting data from ClickHouse using the file, url, and hdfs table functions you can now specify that the data is to be partitioned into multiple files using a `PARTITION BY` clause. For example, `INSERT INTO TABLE FUNCTION file(’&lt;path>/hits_{_partition_id}’, ‘TSV’, ‘&lt;table_format>’) PARTITION BY toYYYYMM(EventDate) VALUES &lt;values>’ will create as many files as there are unique month in the dataset.

The S3 table function has supported partitioned writes since ClickHouse 21.10.

**How does this help you?**

This feature makes using tables backed by files on local file systems and remote file systems accessed over HTTP and HDFS more convenient and flexible. Especially for large datasets you might not want all data to be in one big file. This feature allows you to split one table into smaller files any way you want e.g. by month.


## Table constraints

When creating tables, you can now (optionally) specify constraints. A constraint tells ClickHouse that a column has a specific relationship to another column in the same table. For example, a string column might store the prefix of another column. Then, when a select query is trying to calculate the prefix on the original column, ClickHouse will rewrite the query to use the prefix column.

For now, constraints are implemented as assumptions, that is, ClickHouse does not validate that they are correct and will not reject new data that violates them.

This feature is disabled by default. To turn it on, enable `optimize_using_constraints`, `optimize_substitute_columns` and/or `optimize_append_index`.

**How does this help you?**

Especially in large ClickHouse deployments with many complex tables it can be hard for users to always be up to date on the best way to query a given dataset. Constraints can help optimize queries without having to change the query structure itself. They can also make it easier to make changes to tables. For example, let’s say you have a table containing web requests and it includes a URL column that contains the full URL of each request. Many times, users will want to know the top level domain (.com, .co.uk, etc.), something ClickHouse provides the topLevelDomain function to calculate. If you discover that many people are using this function you might decide to create a new materialized column that pre-calculates the top level domain for each record. Rather than tell all your users to change their queries you can use a table constraint to tell ClickHouse that each time a user tries to call the topLevelDomain function the request should be rewritten to use the new materialized column.


## Read large remote files in chunks

When reading large files in Parquet, ORC, and Arrow format using the s3, url, and hdfs table functions, ClickHouse will now automatically choose whether to read the entire file at once or read parts of it incrementally. This is now enabled by default and the setting `remote_read_min_bytes_for_seek` controls when to switch from reading it all to reading in chunks. The default is 1MiB.

**How does this help our ClickHouse Users?**

In previous versions, when reading files from remote locations with the s3, url, and hdfs table functions, ClickHouse would always read the entire file into memory. This works well when the files are small but will cause excessive memory usage or not work at all when the files are large. With this change, ClickHouse will read large files in chunks to keep memory usage in check and is now able to read even very large files. 

ClickHouse Keeper is 100% feature complete. More updates to come in the coming weeks around where and how you can test and provide feedback for us!

Release 21.12

Release Date: 2021-12-13

Release Notes: [21.12](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md)

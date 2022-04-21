---
title: 'ClickHouse v21.11 Released'
image: 'https://blog-images.clickhouse.com/en/2021/clickhouse-v21-11/featured-dog.jpg'
date: '2021-11-11'
author: 'Rich Raposa, Alexey Milovidov'
tags: ['company', 'community']
---

We're continuing our monthly release cadence and blog updates at[ ClickHouse, Inc](https://clickhouse.com/blog/en/2021/clickhouse-inc/). The 21.11 release includes asynchronous inserts, interactive mode, UDFs, predefined connections, and compression gains. Thank you to the 142 committers and 4337 commits for making this release possible. 

Let's highlight some of these new exciting new capabilities in 21.11:

## Async Inserts

New asynchronous INSERT mode allows to accumulate inserted data and store it in a single batch utilizing less disk resources(IOPS) enabling support of high rate of INSERT queries. On a client it can be enabled by setting `async_insert` for `INSERT` queries with data inlined in a query or in a separate buffer (e.g. for `INSERT` queries via HTTP protocol). If `wait_for_async_insert` is true (by default) the client will wait until data will be flushed to the table. On the server-side it can be tuned by the settings `async_insert_threads`, `async_insert_max_data_size` and `async_insert_busy_timeout_ms`. 

**How does this help our ClickHouse Users?**

A notable pain point for users was around having to insert data in large batches and performance can sometimes be hindered. What if you have a monitoring use case and you want to do 1M records per second into ClickHouse; you would do large 100k record batches, but if you have 1,000 clients shipping data then that was hard to collect these batches to insert into ClickHouse. Historically to solve for this you might have to use Kafka or buffer tables to help with the balancing and insertion of data.

Now, we've introduced this new mode of Async inserts where you can do a high rate of small inserts concurrently and ClickHouse will automatically group them together into batches and insert it into the table automatically. Every client will get an acknowledgement that the data was inserted successfully.

## Local Interactive Mode

We have added interactive mode for `clickhouse-local` so that you can just run `clickhouse-local` to get a command line ClickHouse interface without connecting to a server and process data from files and external data sources.

**How does this help our ClickHouse Users?**

What if you have an ad-hoc use case that you want to run analytics on a local file with ClickHouse? Historically, you'd have to spin up an empty ClickHouse server and connect it to the external data source that you were interested in running the query on e.g. S3, HDFS, URL's. Now with ClickHouse Local you can just run it just like a ClickHouse Client and have the same full interactive experience without any additional overhead steps around setup and ingestion of data to try out your idea or hypothesis. Hope you enjoy!

## Executable UDFs

Added support for executable (scriptable) user defined functions. These are UDFs that can be written in any programming language. 

**How does this help our ClickHouse Users?**

We added UDFs in our 21.10 release. Similar to our October release we're continuing to innovate around the idea of making it more user friendly to plug in tools into ClickHouse as functions. This could be you doing an ML inference in your Python script and now you can define it as a function as available in SQL. Or, what if you wanted to do a DNS lookup? You have a domain name in a ClickHouse table and want to convert to an IP address with some function. Now just plug in an external script and this will go process and convert the domain names into IP addresses.

## Predefined Connections

Allow predefined connections to external data sources. This allows to avoid specifying credentials or addresses while using external data sources, they can be referenced by names instead. 

**How does this help our ClickHouse Users?**

You're just trying to connect ClickHouse to another data source to load data, like MySQL for example, how do you do that? Before this feature you would have to handle all the credentials for MySql, use the MySQL table functions, know the user and password permissions to access certain tables, etc. Now you have a predefined required parameters inside the ClickHouse configuration and the user can just refer to this by a name e.g. MongoDB, HDFS, S3, MySQL and it's a one-time configuration going forward. 

## Compression

Add support for compression and decompression for `INTO OUTFILE` and `FROM INFILE` (with autodetect or with additional optional parameter).

**How does this help our ClickHouse Users?**

Are you just looking to import and export data into ClickHouse more easily if you have compressed data? Before this feature you had to manually specify compression of input and output data into ClickHouse and even for stream insertion you'd still have to manage the decompression there too. Now, you can just write it as a file e.g. mytable.csv.gz --- and, go!

In the last month, we've added new free Training modules including a What's New in 21.11. Take the lesson [here](https://clickhouse.com/learn/lessons/whatsnew-clickhouse-21.11/).

## ClickHouse Release Notes 

Release 21.11

Release Date: 2021-11-09

Release Notes: [21.11](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md)

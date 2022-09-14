---
title: 'Evolution of Data Structures in Yandex.Metrica'
image: 'https://blog-images.clickhouse.com/en/2016/evolution-of-data-structures-in-yandex-metrica/main.jpg'
date: '2016-12-13'
tags: ['Yandex.Metrica', 'data structures', 'LSM tree', 'columnar storage']
author: 'Alexey Milovidov'
---

[Yandex.Metrica](https://metrica.yandex.com/) takes in a stream of data representing events that took place on sites or on apps. Our task is to keep this data and present it in an analyzable form. The real challenge lies in trying to determine what form the processed results should be saved in so that they are easy to work with. During the development process, we had to completely change our approach to data storage organization several times. We started with MyISAM tables, then used LSM-trees and eventually came up with column-oriented database, ClickHouse.

At its founding, Metrica was designed as an offshoot of Yandex.Direct, the search ads service. MySQL tables with MyISAM engine were used in Direct to store statistics and it was natural to use same approach in Metrica. Initially Yandex.Metrica for websites had more than 40 “fixed” report types (for example, the visitor geography report), several in-page analytics tools (like click maps), Webvisor (tool to study individual user actions in great detail), as well as the separate report constructor. But with time to keep up with business goals the system had to become more flexible and provide more customization opportunities for customers. Nowadays instead of using fixed reports Metrica allows to freely add new dimensions (for example, in a keyword report you can break data down further by landing page), segment and compare (between, let's say, traffic sources for all visitors vs. visitors from Moscow), change your set of metrics, etc. These features demanded a completely different approach to data storage than what we used with MyISAM, we will further discuss this transition from technical perspective.

## MyISAM

Most SELECT queries that fetch data for reports are made with the conditions WHERE CounterID = AND Date BETWEEN min_date AND max_date. Sometimes there is also filter by region, so it made sense to use complex primary key to turn this into primary key range is read. So table schema for Metrica looks like this: CounterID, Date, RegionID -> Visits, SumVisitTime, etc. Now we'll take a look at what happens when it comes in.

A MyISAM table is comprised of a data file and an index file. If nothing was deleted from the table and the rows did not change in length during updating, the data file will consist of serialized rows arranged in succession in the order that they were added. The index (including the primary key) is a B-tree, where the leaves contain offsets in the data file. When we read index range data, a lot of offsets in the data file are taken from the index. Then reads are issued for this set of offsets in the data file.

Let's look at the real-life situation when the index is in RAM (key cache in MySQL or system page cache), but the table data is not cached. Let's assume that we are using HDDs. The time it takes to read data depends on the volume of data that needs to be read and how many Seek operations need to be run. The number of Seek's is determined by the locality of data on the disk.

Data locality illustrated:
![Data locality](https://blog-images.clickhouse.com/en/2016/evolution-of-data-structures-in-yandex-metrica/1.jpg)

Metrica events are received in almost the same order in which they actually took place. In this incoming stream, data from different counters is scattered completely at random. In other words, incoming data is local by time, but not local by CounterID. When writing to a MyISAM table, data from different counters is also placed quite randomly. This means that to read the data report, you will need to perform about as many random reads as there are rows that we need in the table.

A typical 7200 rpm hard disk can perform 100 to 200 random reads per second. A RAID, if used properly, can handle the same amount multiplied by number of disks in it. One five-year-old SSD can perform 30,000 random reads per second, but we cannot afford to keep our data on SSD. So in this case, if we needed to read 10,000 rows for a report, it would take more than 10 seconds, which would be totally unacceptable.

InnoDB is much better suited to reading primary key ranges since it uses [a clustered primary key](https://en.wikipedia.org/wiki/Database_index#Clustered) (i.e., the data is stored in an orderly manner on the primary key). But InnoDB was impossible to use due to its slow write speed. If this reminds you of [TokuDB](https://www.percona.com/software/mysql-database/percona-tokudb), then read on.

It took a lot of tricks like periodic table sorting, complicated manual partitioning schemes, and keeping data in generations to keep Yandex.Metrica working on MyISAM. This approach also had a lot of lot of operational drawbacks, for example slow replication, consistency, unreliable recovery, etc. Nevertheless, as of 2011, we stored more than 580 billion rows in MyISAM tables.

## Metrage and OLAPServer

Metrage is an implementation of [LSM Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree), a fairly common data structure that works well for workloads with intensive stream of writes and mostly primary key reads, like Yandex.Metrica has. LevelDB did not exist in 2010 and TokuDB was proprietary at the time.

![LSM Tree](https://blog-images.clickhouse.com/en/2016/evolution-of-data-structures-in-yandex-metrica/2.jpg)

In Metrage arbitrary data structures (fixed at compile time) can be used as “rows” in it. Every row is a key, value pair. A key is a structure with comparison operations for equality and inequality. The value is an arbitrary structure with operations to update (to add something) and merge (to aggregate or combine with another value). In short, it's a CRDT. Data is located pretty locally on the hard disk, so the primary key range reads are quick. Blocks of data are effectively compressed even with fast algorithms because of ordering (in 2010 we used QuickLZ, since 2011 - LZ4). Storing data in a systematic manner enables us to use a sparse index.

Since reading is not performed very often (even though lot of rows are read when it does) the increase in latency due to having many chunks and decompressing the data block does not matter. Reading extra rows because of the index sparsity also does not make a difference.

After transferring reports from MyISAM to Metrage, we immediately saw an increase in Metrica interface speed. Whereas earlier the 90% of page-title reports loaded in 26 seconds, with Metrage they loaded in 0.8 seconds (total time, including time to process all database queries and follow-up data transformations). The time it takes Metrage itself to process queries (for all reports) is as follows according to percent: average = 6 ms, 90tile = 31 ms, 99tile = 334 ms.

We've been using Metrage for five years and it has proved to be a reliable solution. As of 2015 we stored 3.37 trillion rows in Metrage and used 39 * 2 servers for this.

Its advantages were simplicity and effectiveness, which made it a far better choice for storing data than MyISAM. Though the system still had one huge drawback: it really only works effectively with fixed reports. Metrage aggregates data and saves aggregated data. But in order to do this, you have to list all the ways in which you want to aggregate data ahead of time. So if we do this in 40 different ways, it means that Metrica will contain 40 types of reports and no more.

To mitigate this we had to keep for a while a separate storage for custom report wizard, called OLAPServer. It is a simple and very limited implementation of a column-oriented database. It supports only one table set in compile time — a session table. Unlike Metrage, data is not updated in real-time, but rather a few times per day. The only data type supported is fixed-length numbers of 1-8 bytes, so it wasn“t suitable for reports with other kinds of data, for example URLs.

## ClickHouse

Using OLAPServer, we developed an understanding of how well column-oriented DBMS's handle ad-hoc analytics tasks with non-aggregated data. If you can retrieve any report from non-aggregated data, then it begs the question of whether data even needs to be aggregated in advance, as we did with Metrage.

![](https://blog-images.clickhouse.com/en/2016/evolution-of-data-structures-in-yandex-metrica/3.gif)

On the one hand, pre-aggregating data can reduce the volume of data that is used at the moment when the report page is loading. On the other hand, though, aggregated data doesn't solve everything. Here are the reasons why:

-   you need to have a list of reports that your users need ahead of time; in other words, the user can't put together a custom report
-   when aggregating a lot of keys, the amount of data is not reduced and aggregation is useless; when there are a lot of reports, there are too many aggregation options (combinatorial explosion)
-   when aggregating high cardinality keys (for example, URLs) the amount of data does not decrease by much (by less than half)
due to this, the amount of data may not be reduced, but actually grow during aggregation
-   users won't view all the reports that we calculate for them (in other words, a lot of the calculations prove useless)
-   it's difficult to maintain logical consistency when storing a large number of different aggregations

As you can see, if nothing is aggregated and we work with non-aggregated data, then it's possible that the volume of computations will even be reduced. But only working with non-aggregated data imposes very high demands on the effectiveness of the system that executes the queries.

So if we aggregate the data in advance, then we should do it constantly (in real time), but asynchronously with respect to user queries. We should really just aggregate the data in real time; a large portion of the report being received should consist of prepared data.

If data is not aggregated in advance, all the work has to be done at the moment the user request it (i.e. while they wait for the report page to load). This means that many billions of rows need to be processed in response to the user's query; the quicker this can be done, the better.

For this you need a good column-oriented DBMS. The market didn‘t have any column-oriented DBMS's that would handle internet-analytics tasks on the scale of Runet (the Russian internet) well enough and would not be prohibitively expensive to license.

Recently, as an alternative to commercial column-oriented DBMS's, solutions for efficient ad-hoc analytics of data in distributed computing systems began appearing: Cloudera Impala, Spark SQL, Presto, and Apache Drill. Although such systems can work effectively with queries for internal analytical tasks, it is difficult to imagine them as the backend for the web interface of an analytical system accessible to external users.

At Yandex, we developed and later opensourced our own column-oriented DBMS — ClickHouse. Let's review the basic requirements that we had in mind before we proceeded to development.

**Ability to work with large datasets.** In current Yandex.Metrica for websites, ClickHouse is used to store all data for reports. As of November, 2016, the database is comprised of 18.3 trillion rows. It‘s made up of non-aggregated data that is used to retrieve reports in real-time. Every row in the largest table contains over 200 columns.

**The system should scale linearly.** ClickHouse allows you to increase the size of cluster by adding new servers as needed. For example, Yandex.Metrica's main cluster has increased from 60 to 426 servers in three years. In the aim of fault tolerance, our servers are spread across different data centers. ClickHouse can use all hardware resources to process a single query. This way more than 2 terabyte can be processed per second.

**High efficiency.** We especially pride ourselves on our database's high performance. Based on the results of internal tests, ClickHouse processes queries faster than any other system we could acquire. For example, ClickHouse works an average of 2.8-3.4 times faster than Vertica. With ClickHouse there is no one silver bullet that makes the system work so quickly.

**Functionality should be sufficient for Web analytics tools.** The database supports the SQL language dialect, subqueries and JOINs (local and distributed). There are numerous SQL extensions: functions for web analytics, arrays and nested data structures, higher-order functions, aggregate functions for approximate calculations using sketching, etc. By working with ClickHouse, you get the convenience of a relational DBMS.

ClickHouse was initially developed by the Yandex.Metrica team. Furthermore, we were able to make the system flexible and extensible enough that it can be successfully used for different tasks. Although the database can run on large clusters, it can be installed on one server or even on a virtual machine. There are now more than a dozen different ClickHouse applications within our company.

ClickHouse is well equipped for creating all kinds of analytical tools. Just consider: if the system can handle the challenges of Yandex.Metrica, you can be sure that ClickHouse will cope with other tasks with a lot of performance headroom to spare.

ClickHouse works well as a time series database; at Yandex it is commonly used as the backend for Graphite instead of Ceres/Whisper. This lets us work with more than a trillion metrics on a single server.

ClickHouse is used by analytics for internal tasks. Based on our experience at Yandex, ClickHouse performs at about three orders of magnitude higher than traditional methods of data processing (scripts on MapReduce). But this is not a simple quantitative difference. The fact of the matter is that by having such a high calculation speed, you can afford to employ radically different methods of problem solving.

If an analyst has to make a report and they are competent at their job, they won't just go ahead and construct one report. Rather, they will start by retrieving dozens of other reports to better understand the nature of the data and test various hypotheses. It is often useful to look at data from different angles in order to posit and check new hypotheses, even if you don't have a clear goal.

This is only possible if the data analysis speed allows you to conduct online research. The faster queries are executed, the more hypotheses you can test. Working with ClickHouse, one even gets the sense that they are able to think faster.

In traditional systems, data is like a dead weight, figuratively speaking. You can manipulate it, but it takes a lot of time and is inconvenient. If your data is in ClickHouse though, it is much more malleable: you can study it in different cross-sections and drill down to the individual rows of data.

## Conclusions

Yandex.Metrica has become the second largest web-analytics system in the world. The volume of data that Metrica takes in grew from 200 million events a day in 2009 to more than 25 billion in 2016. In order to provide users with a wide variety of options while still keeping up with the increasing workload, we've had to constantly modify our approach to data storage.

Effective hardware utilization is very important to us. In our experience, when you have a large volume of data, it's better not to worry as much about how well the system scales and instead focus on how effectively each unit of resource is used: each processor core, disk and SSD, RAM, and network. After all, if your system is already using hundreds of servers, and you have to work ten times more efficiently, it is unlikely that you can just proceed to install thousands of servers, no matter how scalable your system is.

To maximize efficiency, it's important to customize your solution to meet the needs of specific type of workload. There is no data structure that copes well with completely different scenarios. For example, it's clear that key-value databases don't work for analytical queries. The greater the load on the system, the narrower the specialization required. One should not be afraid to use completely different data structures for different tasks.

We were able to set things up so that Yandex.Metrica's hardware was relatively inexpensive. This has allowed us to offer the service free of charge to even very large sites and mobile apps, even larger than Yanex‘s own, while competitors typically start asking for a paid subscription plan.

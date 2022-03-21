---
title: 'How to Update Data in ClickHouse'
date: '2016-11-20'
image: 'https://blog-images.clickhouse.com/en/2016/how-to-update-data-in-clickhouse/main.jpg'
tags: ['features', 'update', 'delete', 'CollapsingMergeTree', 'partitions']
---

There is no UPDATE or DELETE commands in ClickHouse at the moment. And that's not because we have some religious believes. ClickHouse is performance-oriented system; and data modifications are hard to store and process optimally in terms of performance.

But sometimes we have to modify data. And sometimes data should be updated in realtime. Don't worry, we have these cases covered.

## Work with Partitions

Data in MergeTree engine family is partitioned by partition_key engine parameter. MergeTree split all the data by this partition key. Partition size is one month.

That's very useful in many terms. Especially when we're talking about data modification.

## Yandex.Metrica "hits" Table

Let's look at an example on Yandex.Metrica server mtlog02-01-1 which store some Yandex.Metrica data for year 2013. Table we are looking at contains user events we call “hits”. This is the engine description for hits table:

``` text
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{layer}-{shard}/hits', -- zookeeper path
    '{replica}', -- settings in config describing replicas
    EventDate, -- partition key column
    intHash32(UserID), -- sampling key
    (CounterID, EventDate, intHash32(UserID), WatchID), -- index
    8192 -- index granularity
)
```

You can see that the partition key column is EventDate. That means that all the data will be splitted by months using this column.

With this SQL we can get partitions list and some stats about current partitions:

```sql
SELECT 
    partition, 
    count() as number_of_parts, 
    formatReadableSize(sum(bytes)) as sum_size 
FROM system.parts 
WHERE 
    active 
    AND database = 'merge' 
    AND table = 'hits' 
GROUP BY partition 
ORDER BY partition;
```
```text
┌─partition─┬─number_of_parts─┬─sum_size───┐
│ 201306    │               1 │ 191.34 GiB │
│ 201307    │               4 │ 537.86 GiB │
│ 201308    │               6 │ 608.77 GiB │
│ 201309    │               5 │ 658.68 GiB │    
│ 201310    │               5 │ 768.74 GiB │
│ 201311    │               5 │ 654.61 GiB │
└───────────┴─────────────────┴────────────┘
```
There are 6 partitions with a few parts in each of them. Each partition is around 600 Gb of data. Partition is strictly one piece of data for partition key, here we can see that it is months. Part is one piece of data inside partition. Basically it's one node of LSMT structure, so there are not so many of them, especially for old data. If there are too many of them, they merge and form bigger ones.

## Partition Operations

There is a nice set of operations to work with partitions:

-   `DETACH PARTITION` - Move a partition to the 'detached' directory and forget it.
-   `DROP PARTITION` - Delete a partition.
-   `ATTACH PART|PARTITION` -- Add a new part or partition from the 'detached' directory to the table.
-   `FREEZE PARTITION` - Create a backup of a partition.
-   `FETCH PARTITION` - Download a partition from another server.

We can do any data management operations on partitions level: move, copy and delete. Also, special DETACH and ATTACH operations are created to simplify data manipulation. DETACH detaches partition from table, moving all data to detached directory. Data is still there and you can copy it anywhere but detached data is not visible on request level. ATTACH is the opposite: attaches data from detached directory so it become visible.

This attach-detach commands works almost in no time so you can make your updates almost transparently to database clients.

Here is the plan how to update data using partitions:

-   Create modified partition with updated data on another table
-   Copy data for this partition to detached directory
-   `DROP PARTITION` in main table
-   `ATTACH PARTITION` in main table

Partition swap especially useful for huge data updates with low frequency. But they're not so handy when you need to update a lot of data in real time.

## Update Data on the Fly

In Yandex.Metrica we have user sessions table. Each row is one session on a website: some pages checked, some time spent, some banners clicked. This data is updated every second: user on a website view more pages, click more buttons, and do other things. Site owner can see that actions in Yandex.Metrica interface in real time.

So how do we do that?

We update data not by updating that data, but adding more data about what have changed. This is usually called CRDT approach, and there is an article on Wikipedia about that.

It was created to solve conflict problem in transactions but this concept also allows updating data. We use our own data model with this approach. We call it Incremental Log.

## Incremental Log

Let's look at an example.

Here we have one session information with user identifier UserID, number of page viewed PageViews, time spent on site in seconds Duration. There is also Sign field, we describe it later.
``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
And let's say we calculate some metrics over this data.

-   `count()`- number of sessions
-   `sum(PageViews)`- total number of pages all users checked
-   `avg(Duration)` - average session duration, how long user usually spent on the website

Let's say now we have update on that: user checked one more page, so we should change PageViews from 5 to 6 and Duration from 146 to 185.

We insert two more rows:
``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

First one is delete row. It's exactly the same row what we already have there but with Sign set to -1. Second one is updated row with all data set to new values.

After that we have three rows of data:
``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The most important part is modified metrics calculation. We should update our queries like this:

``` text
 -- number of sessions
count() -> sum(Sign)
 -- total number of pages all users checked
sum(PageViews) -> sum(Sign * PageViews)
 -- average session duration, how long user usually spent on the website
avg(Duration) -> sum(Sign * Duration) / sum(Sign)
```

You can see that it works as expected over this data. Deleted row 'hide' old row, same values come with + and - signs inside aggregation and annihilate each other.

Moreover, it works totally fine with changing keys for grouping. If we want to group data by PageViews, all data for PageView = 5 will be 'hidden' for this rows.

There are some limitations with this approach:

-   It works only for metrics which can be presented through this Sign operations. It covers most cases, but it's not possible to calculate min or max values. There is an impact to uniq calculations also. But it's fine at least for Yandex.Metrica cases, and there are a lot of different analytical calculations;
-   You need to remember somehow old value in external system doing updates, so you can insert this 'delete' rows;
-   Some other effects; there is a [great answer](https://groups.google.com/forum/#!msg/clickhouse/VixyOUD-K68/Km8EpkCyAQAJ) on Google Groups.

## CollapsingMergeTree

ClickHouse has support of Incremental Log model in Collapsing engines family.

If you use Collapsing family, 'delete' row and old 'deleted' rows will collapse during merge process. Merge is a background process of merging data into larger chunks. Here is a great article about merges and LSMT structures.

For most cases 'delete' and 'deleted' rows will be removed in terms of days. What's important here is that you will not have any significant overhead on data size. Using Sign field on selects still required.

Also there is FINAL modifier available over Collapsing family. Using FINAL guarantees that user will see already collapsing data, thus using Sign field isn't required. FINAL usually make tremendous performance degradation because ClickHouse have to group data by key and delete rows during SELECT execution. But it's useful when you want to check your queries or if you want to see raw, unaggregated data in their final form.

## Future Plans

We know that current feature set is not enough. There are some cases which do not fit to limitations. But we have huge plans, and here are some insights what we've preparing:

-   Partitions by custom key: current partitioning scheme is binded to months only. We will remove this limitation and it will be possible to create partitions by any key. All partition operations like FETCH PARTITION will be available.
-   UPDATE and DELETE: there are a lot of issues with updates and deletes support. Performance degradation, consistency guarantees, distributed queries and more. But we believe that if you need to update few rows of data in your dataset, it should not be painful. It will be done.


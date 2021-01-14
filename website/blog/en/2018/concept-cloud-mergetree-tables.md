---
title: 'Concept: "Cloud" MergeTree Tables'
image: 'https://blog-images.clickhouse.tech/en/2018/concept-cloud-mergetree-tables/main.jpg'
date: '2018-11-23'
tags: ['concept', 'MergeTree', 'future', 'sharding']
---

The main property of the MergeTree cloud tables is the absence of manual control over the sharding scheme of data on a cluster. The data in the cloud tables are distributed around the cluster on its own, while at the same time providing the locality property for a certain key.

## Requirements

1. Creating a cloud table makes it visible on all nodes of the cluster. No need to manually create a separate Distributed table and local tables on each node.
2. When ingesting data to a cloud table, while the table is very small, data is distributed across several cluster servers, but as data grows, more servers are involved (for example, starting from gigabytes per server). The user can create a small table and it should not be too cumbersome; but when creating a table, we do not know in advance how much data will be loaded into it.
3. The user specifies a sharding key (arbitrary tuple). Data for the sharding key range (in lexicographical order) is located on some servers. Very small ranges are located on several servers and to access it is enough to read data from a single server, while sufficiently large ranges are spread across all servers. For example, if we are talking about web analytics the sharding key might start with CounterID, the website identifier. Data on a large site like https://yandex.ru should be spread across all servers in the cluster, while data on a small site should be located on only a few servers. Physical explanation: the cluster should scale to simultaneously provide throughput for heavy queries and to handle high QPS of light queries, and for light queries, the latency should not suffer. In general, this is called data locality.
4. The ability for heavy queries to use all the servers in the cluster, rather than 1 / N, where N is the replication coefficient. Thus, one server can contain multiple replicas of different shards.
5. When replacing the server with an empty one (node recovery), the data restore must be parallelized in some way. At least the reads should be spread over different servers to avoid overloading individual servers.
6. On each local server, reading the range of the primary key should be touching not a very large number of file ranges or not too small file ranges (minimizing disk seeks).
7. (Optional) The ability to use individual disks instead of RAID, but at the same time preserving throughput when reading medium-sized primary key ranges and preserving QPS when reading small-sized ranges.
8. The ability to create multiple tables with a common sharding scheme (co-sharding).
9. Rebalancing data when adding new servers; creation of additional replicas with long unavailability of old servers.
10. SELECT queries should not require synchronous requests to the coordinator. No duplicates or missing data visible by SELECT queries during data rebalancing operations.
11. SELECT queries must choose large enough subset of servers considering conditions on sharding key and knowledge of the current sharding scheme.
12. The ability to efficiently distribute data across servers with uneven available disk space.
13. Atomicity of INSERT on a cluster.

Out of scope and will not be considered:

1. Erasure data encoding for replication and recovery.
2. Data storage on systems with different disks - HDD and SSD. An example is storing fresh data on an SSD.

## General Considerations

A similar problem usually (in Map-Reduce or blob-storage systems) is solved by organizing data in chunks. Chunks are located on the nodes of the cluster. Mappings: table or file -> chunks, chunk -> nodes, are stored in the master, which itself can be replicated. The master observes the liveliness of nodes and maintains a reasonable replication level of all chunks.

Difficulties arise when there are too many chunks: in this case, the master does not cope with the storage of metadata and with the load. It becomes necessary to make complicated metadata sharding.

In our case, it may seem tempting to solve a problem in a similar way, where instead of a chunk, an instance of a MergeTree type table containing the data range is used. Chunks in other systems are called “tablets” or “regions”. But there are many problems with this. The number of chunks on one server cannot be large, because then the property is violated - minimizing the number of seeks when reading data ranges. The problem also arises from the fact that each MergeTree table itself is rather cumbersome and consists of a large number of files. On the other hand, tables with a size of one terabyte are more or less normal if the data locality property is maintained. That is if several such tables on one server begin to be used only for not too small data ranges.

A variety of options can be used for sharding data, including:
Sharding according to some formula with a small number of parameters. Examples are simple hashing, consistent hashing (hash ring, rendezvous hashing, jump consistent hashing, sumbur). The practice of using in other systems shows that in its pure form this approach does not work well, because the sharding scheme is poorly controlled. Fits fine, for example, for caches. It can also be used as part of another algorithm.

The opposite option is that the data is divided into shards using an explicitly specified table. The table may contain key ranges (or, in another case, hash ranges from keys) and their corresponding servers. This gives a much greater degree of freedom in choosing when and how to transfer data. But at the same time, to scale the cluster, the size of the table has to be dynamically expanded, breaking the existing ranges.

One of the combined options is that the mapping is made up of two parts: first, the set of various keys is divided into some pre-fixed not too few and not too many “virtual shards” (you can also call “logical shards”, “mini-shards”). This number is several times larger than the hypothetical cluster size in the number of servers. Further, the second mapping explicitly specifies the location of each mini-shard on the servers, and this second mapping can be controlled arbitrarily.

The complexity of this approach is that partitioning hash ranges gives uniformity, but does not give locality of data for range queries; whereas when splitting by key ranges, it is difficult to choose a uniform distribution in advance since we do not know what the distribution of data will be to the keys. That is, the approach with the choice of a pre-fixed split into mini-shards does not work if data locality is required.

It turns out that the only acceptable approach in our case is partitioning by key ranges, which can change dynamically (repartitioned). At the same time, for more convenience, manageability, and uniformity of data distribution, the number of partitioning elements can be slightly larger than the number of servers, and the mapping from the partitioning element into servers can be changed separately.

## Possible Implementation

Each ClickHouse server can participate in a certain cloud. The cloud is identified by a text string. The membership of a node in the cloud can be ensured by creating a certain type of database on the node (IDatabase). Thus, one node can be registered in several clouds. Registry of the nodes registered in the cloud is maintained in the coordinator.

Cloud nodes are selected to accommodate the replicas of the shards of cloud tables. The node also sends some additional information to the coordinator for its selection when placing data: the path that determines the locality in the network (for example, data center and rack), the amount of disk space, etc.

The cloud table is created in the corresponding database registered in the cloud. The table is created on any server and is visible in all databases registered in the cloud.

Sharding key is set for cloud table on it“s creation, an arbitrary tuple. Sometimes it is practical that the sharding key matches the primary key (example - (CounterID, Date, UserID)), sometimes it makes sense that it is different (for example, the DateTime primary key, sharding key - UserID).

Sharding is a composition of several mappings:

1. The set of all possible tuples, the values ​​of the sharding key, is mapped onto many half-intervals that break the half-interval [0, 1). Initially, this number is the size of the partition, it is equal to 1. That is, all values ​​are mapped into a single semi-interval, the whole set [0, 1). Then, as the amount of data in the table increases, the semi-intervals, the split elements, can be divided approximately in half by the median of the distribution of values ​​in lexicographical order.
2. For each half-interval splitting, several cloud servers are selected and remembered in some way, on which replicas of the corresponding data will be located. The choice is made based on the location of servers on the network (for example, at least two replicas in different data centers and all replicas in different racks), the number of replicas already created on this server (choose servers with the minimum) and the amount of free space (from various servers just select the server with the maximum amount of free space).

As a result, this composition forms a mapping from the sharding key into several replica servers.

It is assumed that in the course of work both parts of this mapping may change.

The result of mapping 1 can be called the “virtual shard” or “logical shard”. In the process of work, virtual shards can be divided in half. Going in the opposite direction is impossible - the number of virtual shards can only grow. It is assumed that even for tables occupying the entire cluster, the number of virtual shards will be several times larger than the number of servers (for example, it may be greater by 10 times the replication ratio). Data ranges occupying at least a tenth of all data should be spread across all servers to ensure throughput of heavy queries. The mapping as a whole is specified by the set of boundary values ​​for the sharding key. This set is small (roughly kilobytes) and stored in the coordinator.

The mapping of virtual shards on real servers can change arbitrarily: the number of replicas can increase when servers are not available for a long time or increase and then decrease to move replicas between servers.
## How to Satisfy All Requirements

List items below correspond to the requirement numbers above:

1. IDatabase synchronously goes to the coordinator to get or change the list of tables. The list of cloud tables is stored in the coordinator in the node corresponding to the cloud. That is, all the tables in the cloud are visible on each server entering the cloud.
2. It is ensured by the fact that initially the partition consists of a single element, but begins to break up further with increasing data volume. Each replica responsible for the local storage of this data can initiate the splitting, once the criterion for the data volume has been reached. Multiple replicas may decide to do this competitively, and the decision is made using atomic CAS. To have fewer problems, it is possible to randomize somewhat the moment of deciding repartition. The criterion when it is necessary to additionally break virtual shards turns out to be non-trivial. For example, you can break up to the number of servers * the replication rate quite soon, by growing a shard to several gigabytes. But it is already worth breaking shards even when shards are 1 / N in size from the server size (for example, around a terabyte). In coordinator, you should store the last and previous splits immediately and do not do the splitting too often.
3. It is ensured by the fact that the number of virtual shards will be several times (user-defined) more than the number of servers. Note: for additional data spreading, you can impose some spreading transformation on the sharding key. Not thought out. For example, instead of a key (CounterID, Date, UserID) use for sharding (hash (UserID)% 10, CounterID, Date, UserID). But in this case, even small CounterIDs will fall into 10 ranges.
4. Similarly.
5. If several virtual shards are located on a single server, their replicas will be spread over a larger number of servers, and during recovery, there will be more fanout.
6. Small requests will use one shard. While large requests will use several shards on the same server. But since each shard will be somewhat smaller, the data in the MergeTree table will probably be presented by a smaller set of parts. For example, we now have a maximum part size of 150 GiB, and for large tables, many such large chunks are formed in one partition. And if there are several tables, there will be a smaller number of large chunks in each. On the other hand, when inserting data, a larger number of small pieces will be generated on each server. And these small parts will cause an increase in the number of seeks. But not much, as the fresh data will be in the page cache. That is why too many virtual shards per server might not work well.
7. Pretty hard. You can have groups of neighboring shards on different disks of the same server. But then reading of medium size ranges will not be parallelized (since the whole range will be on one disk). In RAID, the problem is solved by the fact that the size of the chunk is relatively small (typically 1 megabyte). It would be possible to come up with a separate distribution of data in different pieces on different disks. But it is too difficult to design and implement carefully. Probably it“s better not to do the whole thing, and as a minimum, make it so that when on the JBOD server, one server disk is selected for the location of one shard.
8. It is possible to identify the sharding scheme with a string, which may be common to different tables. The criterion for splitting shards is determined based on the total amount of data for all tables with the same sharding scheme.
9. It is solved completely by changing the mapping of virtual shards on the servers. This mapping can be controlled independently of everything else.
10. Servers can cache the sharding map (both parts of it) for a while and update it usually asynchronously. When rebalancing data due to the splitting of virtual shards, you should keep the old data for a longer time. Similarly, when transferring replicas between servers. Upon request, the initiator server also asks if the remote server has the necessary data: data for the required shard according to the sharding scheme that is cached by the initiator server. For the query, one live replica of each shard is selected, on which there is data. If suddenly there were none, then it is worthwhile to update the sharding map synchronously, as for some reason all the replicas were transferred somewhere.
11. It is trivial.
12. It is solved on the basis that more than one shard accounts for one server and the fact that the distribution of shards replicas among servers is more or less arbitrary and can take into account the amount of disk space.
## Issues

To ingest data into a table, you can send an INSERT query to any server. The data will be divided into ranges and recorded on the desired servers. At the same time, it is synchronously ensured that we use a fresh sharding map - it is requested before the data is inserted and it is checked that it is not out of date, simultaneously with the commit in ZK.

When a SELECT query is used, if the old sharding map was used, the latest data will not be visible. Therefore, the asynchronous update interval of the sharding map for SELECT should be made customizable, and an option should be added to synchronously use the latest sharding map.

For fairly large tables, it turns out that an INSERT request breaks the data into many small pieces and writes to all servers (example: with 500 servers, you need to commit 5000 replicas of shards). This should work since the probability of inaccessibility or inhibition of all replicas of one shard is still low. But it will work slowly and, possibly, unstable. With a lot of INSERTs, there will be a terrible load on the coordinator. Although it can withstand one INSERT per second normally. To achieve high throughput of INSERTs, it is sufficient to simply make them parallel, but with the same low frequency of INSERTs in general. However, this is still a big problem.

There are the following possible solutions:

1. You can add something to the beginning of the sharding key. For example, Date % 10 or toMinute. Then INSERTs will touch fewer shards (in the typical case when recent data is inserted), but at the same time during some time intervals, some shards will be hotter than others. Normally, if it reduces the number of active shards, for example, from 5000 on INSERT to 500. It is also very inconvenient for users.
2. You can come up with some kind of incomprehensible sharding scheme, where the fresh data first falls into some fresh shard where it is not clear where from where it is then lazily overwritten. A fresh shard is essentially a distributed queue. At the same time, a fresh shard with SELECT is always requested. Not so good. And still, it contradicts the atomicity of these transfers of data, visible at SELECT. Alternatively, you could relax the requirements if you allow SELECT not to see some of the fresh data.
It looks like it“s generally not working well at a cluster size of over 500 servers.
Another problem is that to properly spread the ranges of the primary key, the number of virtual shards must be no less than the number of servers squared. And this is too much.
How to Get Around These Issues
For sharding, you can add some more intermediate mappings. There are the following options:
1. Splitting each shard into a set of shards in an arbitrary way. For example, 10 pieces. This is equivalent to adding a random number 0.N-1 to the beginning of the sharding key, which means nothing. Then with INSERT, you can only insert into one randomly selected shard, or a minimum sized shard, or some kind of round-robin; and as a result, INSERT becomes easier. But this increases the fanout of all point SELECTs. For convenience, such a partition can be done dynamically - only large enough shards can be divided in such a way (this will help avoid excessive splitting of old shards in the case when the sharding key starts with Date and the data is inserted in the Date order) or do such a partition starting from the situation when the number of shards is large enough (restriction on top of fanout INSERT requests).
An additional advantage: in the case of servers with JBOD, it is possible to prefer to place such second-level shards on the disks of one server, which half emulates RAID-0.
But there is a serious drawback: there is no possibility to do local IN / JOIN. For example, this possibility is assumed if the sharding key is hash (UserID), and we do JOIN by UserID. It would be possible to avoid this drawback by always placing all the “symmetric” shards on one server.
2. A mapping that spreads the data while keeping the number of virtual shards. The essence of this mapping is as follows:
    - The spreading factor is set, for example, `N = 10.` As the very first mapping, 10 times more ranges are generated. For example, if we want to end up with 7 shards, then we divide the data into 70 ranges.
    - Then these ranges are renumbered in a circle with numbers from 0.6 and the ranges with the same number will fall into one shard, as a result, there will be 7 shards again.
    - The continuous analogue of this mapping: `x in [0, 1) -> fractional_part (x * N)`, multiplication by N on a circle.

If you draw it on the picture in Cartesian coordinates, you get a “saw” with 10 teeth.

After this, it becomes obvious that this mapping simultaneously spreads the data and preserves its locality.

See also: [Arnold's cat map](https://en.wikipedia.org/wiki/Arnold%27s_cat_map).

But what is described here does not exactly work. First, until a sufficient amount of data has been accumulated, it is impossible to create a uniform division into parts (there is no place to count quantiles). Secondly, according to such a simple scheme, it is impossible to divide the intervals.

There is an option in which, instead of dividing a range in half, it uses splitting into 4 parts, which are then mapped into two shards. It is also not clear how this will work.

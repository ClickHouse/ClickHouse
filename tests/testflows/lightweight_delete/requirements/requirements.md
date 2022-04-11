# SRS023 ClickHouse Lightweight Delete
# Software Requirements Specification

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Requirements](#requirements)
  * 2.1 [`DELETE` Statement](#delete-statement)
    * 2.1.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteStatement](#rqsrs-023clickhouselightweightdeletedeletestatement)
  * 2.2 [Delete Zero Rows](#delete-zero-rows)
    * 2.2.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteZeroRows](#rqsrs-023clickhouselightweightdeletedeletezerorows)
  * 2.3 [Delete One Row](#delete-one-row)
    * 2.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteOneRow](#rqsrs-023clickhouselightweightdeletedeleteonerow)
  * 2.4 [Delete All Rows](#delete-all-rows)
    * 2.4.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteAllRows](#rqsrs-023clickhouselightweightdeletedeleteallrows)
  * 2.5 [Delete Small Subset of Rows](#delete-small-subset-of-rows)
    * 2.5.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteSmallSubsetOfRows](#rqsrs-023clickhouselightweightdeletedeletesmallsubsetofrows)
  * 2.6 [Delete Large Subset of Rows](#delete-large-subset-of-rows)
    * 2.6.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeleteLargeSubsetOfRows](#rqsrs-023clickhouselightweightdeletedeletelargesubsetofrows)
  * 2.7 [One Partition and Part](#one-partition-and-part)
    * 2.7.1 [RQ.SRS-023.ClickHouse.LightweightDelete.OnePartitionWithPart](#rqsrs-023clickhouselightweightdeleteonepartitionwithpart)
  * 2.8 [Partition With Many Parts](#partition-with-many-parts)
    * 2.8.1 [RQ.SRS-023.ClickHouse.LightweightDelete.PartitionWithManyParts](#rqsrs-023clickhouselightweightdeletepartitionwithmanyparts)
  * 2.9 [Multiple Partitions and One Part](#multiple-partitions-and-one-part)
    * 2.9.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultiplePartitionsAndOnePart](#rqsrs-023clickhouselightweightdeletemultiplepartitionsandonepart)
  * 2.10 [Multiple Parts And Partitions](#multiple-parts-and-partitions)
    * 2.10.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultiplePartsAndPartitions](#rqsrs-023clickhouselightweightdeletemultiplepartsandpartitions)
  * 2.11 [All Rows From Half of the Parts](#all-rows-from-half-of-the-parts)
    * 2.11.1 [RQ.SRS-023.ClickHouse.LightweightDelete.AllRowsFromHalfOfTheParts](#rqsrs-023clickhouselightweightdeleteallrowsfromhalfoftheparts)
  * 2.12 [Very Large Part](#very-large-part)
    * 2.12.1 [RQ.SRS-023.ClickHouse.LightweightDelete.VeryLargePart](#rqsrs-023clickhouselightweightdeleteverylargepart)
  * 2.13 [Very Small Part](#very-small-part)
    * 2.13.1 [RQ.SRS-023.ClickHouse.LightweightDelete.VerySmallPart](#rqsrs-023clickhouselightweightdeleteverysmallpart)
  * 2.14 [Encrypted Disk](#encrypted-disk)
    * 2.14.1 [RQ.SRS-023.ClickHouse.LightweightDelete.EncryptedDisk](#rqsrs-023clickhouselightweightdeleteencrypteddisk)
  * 2.15 [Replicated Tables](#replicated-tables)
    * 2.15.1 [Eventual Consistency](#eventual-consistency)
      * 2.15.1.1 [RQ.SRS-023.ClickHouse.LightweightDelete.EventualConsistency](#rqsrs-023clickhouselightweightdeleteeventualconsistency)
    * 2.15.2 [Rows Removed From Replica](#rows-removed-from-replica)
      * 2.15.2.1 [RQ.SRS-023.ClickHouse.LightweightDelete.RowsRemovedFromReplica](#rqsrs-023clickhouselightweightdeleterowsremovedfromreplica)
    * 2.15.3 [Multiple Replicas](#multiple-replicas)
      * 2.15.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleReplicas](#rqsrs-023clickhouselightweightdeletemultiplereplicas)
    * 2.15.4 [Replication Queue](#replication-queue)
      * 2.15.4.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ReplicationQueue](#rqsrs-023clickhouselightweightdeletereplicationqueue)
    * 2.15.5 [Replication Stuck](#replication-stuck)
      * 2.15.5.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ReplicationStuck](#rqsrs-023clickhouselightweightdeletereplicationstuck)
  * 2.16 [Sharded Tables](#sharded-tables)
    * 2.16.1 [Multiple Shards](#multiple-shards)
      * 2.16.1.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleShards](#rqsrs-023clickhouselightweightdeletemultipleshards)
  * 2.17 [Alter Table with Parts & Partitions](#alter-table-with-parts-partitions)
    * 2.17.1 [RQ.SRS-023.ClickHouse.LightweightDelete.AlterTableWithParts&Partitions](#rqsrs-023clickhouselightweightdeletealtertablewithpartspartitions)
  * 2.18 [TTL](#ttl)
    * 2.18.1 [RQ.SRS-023.ClickHouse.LightweightDelete.TTL](#rqsrs-023clickhouselightweightdeletettl)
  * 2.19 [Column TTL](#column-ttl)
    * 2.19.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ColumnTTL](#rqsrs-023clickhouselightweightdeletecolumnttl)
  * 2.20 [Invalid Syntax](#invalid-syntax)
    * 2.20.1 [RQ.SRS-023.ClickHouse.LightweightDelete.InvalidSyntax.NoWhere](#rqsrs-023clickhouselightweightdeleteinvalidsyntaxnowhere)
    * 2.20.2 [RQ.SRS-023.ClickHouse.LightweightDelete.InvalidSyntax.EmptyWhere](#rqsrs-023clickhouselightweightdeleteinvalidsyntaxemptywhere)
  * 2.21 [Supported Table Engines](#supported-table-engines)
    * 2.21.1 [RQ.SRS-023.ClickHouse.LightweightDelete.SupportedTableEngines](#rqsrs-023clickhouselightweightdeletesupportedtableengines)
  * 2.22 [Immediate Removal For Selects](#immediate-removal-for-selects)
    * 2.22.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ImmediateRemovalForSelects](#rqsrs-023clickhouselightweightdeleteimmediateremovalforselects)
  * 2.23 [Multiple Deletes](#multiple-deletes)
    * 2.23.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes](#rqsrs-023clickhouselightweightdeletemultipledeletes)
    * 2.23.2 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.Limitations](#rqsrs-023clickhouselightweightdeletemultipledeleteslimitations)
    * 2.23.3 [Concurrent Deletes](#concurrent-deletes)
      * 2.23.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.ConcurrentDelete](#rqsrs-023clickhouselightweightdeletemultipledeletesconcurrentdelete)
      * 2.23.3.2 [RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.ConcurrentDeleteOverlap](#rqsrs-023clickhouselightweightdeletemultipledeletesconcurrentdeleteoverlap)
  * 2.24 [Synchronous Operation](#synchronous-operation)
    * 2.24.1 [RQ.SRS-023.ClickHouse.LightweightDelete.SynchronousOperationOnSingleNode](#rqsrs-023clickhouselightweightdeletesynchronousoperationonsinglenode)
  * 2.25 [Efficient Physical Data Removal](#efficient-physical-data-removal)
    * 2.25.1 [RQ.SRS-023.ClickHouse.LightweightDelete.EfficientPhysicalDataRemoval](#rqsrs-023clickhouselightweightdeleteefficientphysicaldataremoval)
  * 2.26 [Performance](#performance)
    * 2.26.1 [`DELETE` vs `SELECT`](#delete-vs-select)
      * 2.26.1.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance](#rqsrs-023clickhouselightweightdeleteperformance)
    * 2.26.2 [Concurrent Queries](#concurrent-queries)
      * 2.26.2.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.ConcurrentQueries](#rqsrs-023clickhouselightweightdeleteperformanceconcurrentqueries)
    * 2.26.3 [Post Delete `SELECT`s](#post-delete-selects)
      * 2.26.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.PostDelete](#rqsrs-023clickhouselightweightdeleteperformancepostdelete)
    * 2.26.4 [Large Number of Partitions](#large-number-of-partitions)
      * 2.26.4.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.LargeNumberOfPartitions](#rqsrs-023clickhouselightweightdeleteperformancelargenumberofpartitions)
    * 2.26.5 [Large Number of Parts in Partitions](#large-number-of-parts-in-partitions)
      * 2.26.5.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.LargeNumberOfPartsInPartitions](#rqsrs-023clickhouselightweightdeleteperformancelargenumberofpartsinpartitions)
  * 2.27 [Acceptance Criteria Using Reference `OnTime` Dataset](#acceptance-criteria-using-reference-ontime-dataset)
    * 2.27.1 [`OnTime` Dataset](#ontime-dataset)
    * 2.27.2 [`INSERT` Reference Queries](#insert-reference-queries)
    * 2.27.3 [`SELECT` Reference Queries](#select-reference-queries)
      * 2.27.3.1 [Query 1: Average number of flights per month](#query-1-average-number-of-flights-per-month)
      * 2.27.3.2 [Query 2: The number of flights per day from the year 2000 to 2008](#query-2-the-number-of-flights-per-day-from-the-year-2000-to-2008)
      * 2.27.3.3 [Query 3: The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2000-2008](#query-3-the-number-of-flights-delayed-by-more-than-10-minutes-grouped-by-the-day-of-the-week-for-2000-2008)
      * 2.27.3.4 [Query 4: The number of delays by the airport for 2000-2008](#query-4-the-number-of-delays-by-the-airport-for-2000-2008)
      * 2.27.3.5 [Query 5: The number of delays by carrier for 2007](#query-5-the-number-of-delays-by-carrier-for-2007)
      * 2.27.3.6 [Query 6: The percentage of delays by carrier for 2007](#query-6-the-percentage-of-delays-by-carrier-for-2007)
      * 2.27.3.7 [Query 7: The previous request for a broader range of years, 2000-2008](#query-7-the-previous-request-for-a-broader-range-of-years-2000-2008)
      * 2.27.3.8 [Query 8: Percentage of flights delayed for more than 10 minutes, by year](#query-8-percentage-of-flights-delayed-for-more-than-10-minutes-by-year)
      * 2.27.3.9 [Query 9: The most popular destinations by the number of directly connected cities for various year ranges](#query-9-the-most-popular-destinations-by-the-number-of-directly-connected-cities-for-various-year-ranges)
      * 2.27.3.10 [Query 10: Flights per year](#query-10-flights-per-year)
    * 2.27.4 [`DELETE` Reference Queries](#delete-reference-queries)
      * 2.27.4.1 [Query 1: Deleting All Rows In a Single Partition](#query-1-deleting-all-rows-in-a-single-partition)
      * 2.27.4.2 [Query 2: Delete All Rows In Various Partitions](#query-2-delete-all-rows-in-various-partitions)
      * 2.27.4.3 [Query 3: Delete Some Rows In All Partitions (Large Granularity)](#query-3-delete-some-rows-in-all-partitions-large-granularity)
      * 2.27.4.4 [Query 4: Delete Some Rows In All Partitions (Small Granularity)](#query-4-delete-some-rows-in-all-partitions-small-granularity)
      * 2.27.4.5 [Query 5: Delete Some Rows In One Partition (Very Small Granularity)](#query-5-delete-some-rows-in-one-partition-very-small-granularity)
    * 2.27.5 [Acceptance performance](#acceptance-performance)
      * 2.27.5.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.Inserts](#rqsrs-023clickhouselightweightdeleteperformanceacceptanceontimedatasetinserts)
      * 2.27.5.2 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.DeleteQueryExecutionTime](#rqsrs-023clickhouselightweightdeleteperformanceacceptanceontimedatasetdeletequeryexecutiontime)
      * 2.27.5.3 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.ConcurrentSelectsAndDeletes](#rqsrs-023clickhouselightweightdeleteperformanceacceptanceontimedatasetconcurrentselectsanddeletes)
      * 2.27.5.4 [RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.ConcurrentInsertsAndDeletes](#rqsrs-023clickhouselightweightdeleteperformanceacceptanceontimedatasetconcurrentinsertsanddeletes)
  * 2.28 [Immutable Parts And Garbage Collection](#immutable-parts-and-garbage-collection)
    * 2.28.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ImmutablePartsAndGarbageCollection](#rqsrs-023clickhouselightweightdeleteimmutablepartsandgarbagecollection)
  * 2.29 [Compatibility](#compatibility)
    * 2.29.1 [Concurrent Operations](#concurrent-operations)
      * 2.29.1.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentOperations](#rqsrs-023clickhouselightweightdeletecompatibilityconcurrentoperations)
      * 2.29.1.2 [Concurrent Inserts & Deletes On Many Parts](#concurrent-inserts-deletes-on-many-parts)
        * 2.29.1.2.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentInserts&DeletesOnManyParts](#rqsrs-023clickhouselightweightdeletecompatibilityconcurrentinsertsdeletesonmanyparts)
      * 2.29.1.3 [Concurrent Inserts & Deletes Of The Same Data](#concurrent-inserts-deletes-of-the-same-data)
        * 2.29.1.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentInserts&DeletesOfTheSameData](#rqsrs-023clickhouselightweightdeletecompatibilityconcurrentinsertsdeletesofthesamedata)
      * 2.29.1.4 [Concurrent Delete & Alter Delete](#concurrent-delete-alter-delete)
        * 2.29.1.4.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentDelete&AlterDelete](#rqsrs-023clickhouselightweightdeletecompatibilityconcurrentdeletealterdelete)
    * 2.29.2 [Projections](#projections)
      * 2.29.2.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.Projections](#rqsrs-023clickhouselightweightdeletecompatibilityprojections)
    * 2.29.3 [Views](#views)
      * 2.29.3.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.Views](#rqsrs-023clickhouselightweightdeletecompatibilityviews)
  * 2.30 [Hard Restarts](#hard-restarts)
    * 2.30.1 [RQ.SRS-023.ClickHouse.LightweightDelete.HardRestarts](#rqsrs-023clickhouselightweightdeletehardrestarts)
  * 2.31 [Non Corrupted Server State](#non-corrupted-server-state)
    * 2.31.1 [RQ.SRS-023.ClickHouse.LightweightDelete.NonCorruptedServerState](#rqsrs-023clickhouselightweightdeletenoncorruptedserverstate)
  * 2.32 [Server Restart](#server-restart)
    * 2.32.1 [RQ.SRS-023.ClickHouse.LightweightDelete.ServerRestart](#rqsrs-023clickhouselightweightdeleteserverrestart)
  * 2.33 [Non Deterministic Functions](#non-deterministic-functions)
    * 2.33.1 [RQ.SRS-023.ClickHouse.LightweightDelete.NonDeterministicFunctions](#rqsrs-023clickhouselightweightdeletenondeterministicfunctions)
  * 2.34 [Lack of Disk Space](#lack-of-disk-space)
    * 2.34.1 [RQ.SRS-023.ClickHouse.LightweightDelete.LackOfDiskSpace](#rqsrs-023clickhouselightweightdeletelackofdiskspace)
  * 2.35 [Multidisk Configurations](#multidisk-configurations)
    * 2.35.1 [RQ.SRS-023.ClickHouse.LightweightDelete.MultidiskConfigurations](#rqsrs-023clickhouselightweightdeletemultidiskconfigurations)
  * 2.36 [S3 Disks](#s3-disks)
    * 2.36.1 [RQ.SRS-023.ClickHouse.LightweightDelete.S3Disks](#rqsrs-023clickhouselightweightdeletes3disks)
  * 2.37 [Backups](#backups)
    * 2.37.1 [RQ.SRS-023.ClickHouse.LightweightDelete.Backups](#rqsrs-023clickhouselightweightdeletebackups)
  * 2.38 [Drop Empty Part](#drop-empty-part)
    * 2.38.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DropEmptyPart](#rqsrs-023clickhouselightweightdeletedropemptypart)
  * 2.39 [Deletes per Second](#deletes-per-second)
    * 2.39.1 [RQ.SRS-023.ClickHouse.LightweightDelete.DeletesPerSecond](#rqsrs-023clickhouselightweightdeletedeletespersecond)
  * 2.40 [Upgrade Server](#upgrade-server)
    * 2.40.1 [RQ.SRS-023.ClickHouse.LightweightDelete.UpgradeServer](#rqsrs-023clickhouselightweightdeleteupgradeserver)



## Introduction

This software requirements specification covers requirements related to [ClickHouse] lightweight delete
functionality that implements `DELETE` command with standard SQL semantics
including immediate removal of rows from the subsequent `SELECT` results and
efficient removal of physical data from the tables.

## Requirements

### `DELETE` Statement

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteStatement
version: 1.0

[ClickHouse] SHALL support standard `DELETE` statement to remove data that SHALL have the following syntax

```sql
DELETE FROM <table> WHERE <condition>
```

where the `WHERE` condition SHALL be any condition expressible in a `WHERE` clause used in the `SELECT` statements
and all the rows that match the `WHERE` condition SHALL be removed.

Examples:

* Delete a child organization and its data
  ```sql
  DELETE * FROM example_table WHERE mspOrganizationId = 123 and has(organizationIds,456)
  ```
* Delete all records for a specific Identity
  ```sql
  DELETE * FROM example_table WHERE has(origin_ids, 123)
  ```
* Delete all records for certain types of destinations
  ```sql
  DELETE * FROM example_table WHERE has(allCategories, 123)
  ```

### Delete Zero Rows

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteZeroRows
version: 1.0

[ClickHouse]'s `DELETE` statement SHALL remove zero rows if `WHERE` condition does not match any row.

### Delete One Row

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteOneRow
version: 1.0

[ClickHouse]'s `DELETE` statement SHALL remove one row if `WHERE` condition matches one specific row.

### Delete All Rows

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteAllRows
version: 1.0

[ClickHouse]'s `DELETE` statement SHALL all rows if `WHERE` condition matches every row.

### Delete Small Subset of Rows

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteSmallSubsetOfRows
version: 1.0

[ClickHouse]'s `DELETE` statement SHALL remove rows where the `WHERE` condition matches only matches small subset of rows.

### Delete Large Subset of Rows

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeleteLargeSubsetOfRows
version: 1.0

[ClickHouse]'s `DELETE` statement SHALL remove rows where the `WHERE` condition matches only matches large subset of rows.

### One Partition and Part

#### RQ.SRS-023.ClickHouse.LightweightDelete.OnePartitionWithPart
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data in a partition with one part.

### Partition With Many Parts

#### RQ.SRS-023.ClickHouse.LightweightDelete.PartitionWithManyParts
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data in a partition with many small parts.

### Multiple Partitions and One Part

#### RQ.SRS-023.ClickHouse.LightweightDelete.MultiplePartitionsAndOnePart
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data from multiple partitions with one part each.

### Multiple Parts And Partitions

#### RQ.SRS-023.ClickHouse.LightweightDelete.MultiplePartsAndPartitions
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data from multiple parts and partitions.

### All Rows From Half of the Parts

#### RQ.SRS-023.ClickHouse.LightweightDelete.AllRowsFromHalfOfTheParts
version: 1.0

[ClickHouse] SHALL support `DELETE` removing all rows from half of the parts.

### Very Large Part

#### RQ.SRS-023.ClickHouse.LightweightDelete.VeryLargePart
version: 1.0

[ClickHouse] SHALL support `DELETE` removing rows in a very large part.

### Very Small Part

#### RQ.SRS-023.ClickHouse.LightweightDelete.VerySmallPart
version: 1.0

[ClickHouse] SHALL support `DELETE` removing rows in a very small part.

### Encrypted Disk

#### RQ.SRS-023.ClickHouse.LightweightDelete.EncryptedDisk
version: 1.0

[ClickHouse] SHALL support `DELETE` removing rows from table which is stored on encrypted disk.

### Replicated Tables

#### Eventual Consistency

##### RQ.SRS-023.ClickHouse.LightweightDelete.EventualConsistency
version: 1.0

[ClickHouse] `DELETE` operations SHALL replicate in an eventually consistent manner between replicas.

#### Rows Removed From Replica

##### RQ.SRS-023.ClickHouse.LightweightDelete.RowsRemovedFromReplica
version: 1.0

[ClickHouse] SHALL support `DELETE` removing rows from a part where the rows have already
been removed from another replica.

#### Multiple Replicas

##### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleReplicas
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data from multiple replicas.

#### Replication Queue

##### RQ.SRS-023.ClickHouse.LightweightDelete.ReplicationQueue
version: 1.0

[ClickHouse] SHALL push `DELETE` statements to the replication queue.

#### Replication Stuck

##### RQ.SRS-023.ClickHouse.LightweightDelete.ReplicationStuck
version: 1.0

[ClickHouse] SHALL reject new `DELETE` statements when the replication queue is full or connection to zookeeper is lost.

### Sharded Tables

#### Multiple Shards

##### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleShards
version: 1.0

[ClickHouse] SHALL support `DELETE` removing data from multiple shards.

### Alter Table with Parts & Partitions

#### RQ.SRS-023.ClickHouse.LightweightDelete.AlterTableWithParts&Partitions
version: 1.0

[ClickHouse] SHALL support using parts with deleted rows in all `ALTER TABLE` operations that target parts or partitions.
`ALTER TABLE` operations:

* `DETACH PART|PARTITION`
* `DROP PART|PARTITION`
* `DROP DETACHED PART|PARTITION`
* `ATTACH PART|PARTITION`
* `REPLACE PARTITION`
* `FREEZE PARTITION`
* `UNFREEZE PARTITION`
* `FETCH PART|PARTITION`
* `MOVE PART|PARTITION`
* `UPDATE IN PARTITION`
* `DELETE IN PARTITION`
* `ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN`

### TTL

#### RQ.SRS-023.ClickHouse.LightweightDelete.TTL
version: 1.0

[ClickHouse] SHALL support using parts with deleted rows in tiered storage TTL moves and deletes.

### Column TTL

#### RQ.SRS-023.ClickHouse.LightweightDelete.ColumnTTL
version: 1.0

[ClickHouse] SHALL support using parts with deleted rows in column TTL operations.

### Invalid Syntax

#### RQ.SRS-023.ClickHouse.LightweightDelete.InvalidSyntax.NoWhere
version: 1.0

[ClickHouse] SHALL return an error when using `DELETE` statement with no `WHERE` clause.

#### RQ.SRS-023.ClickHouse.LightweightDelete.InvalidSyntax.EmptyWhere
version: 1.0

[ClickHouse] SHALL return an error when using `DELETE` statement with empty `WHERE` clause.

### Supported Table Engines

#### RQ.SRS-023.ClickHouse.LightweightDelete.SupportedTableEngines
version: 1.0

[ClickHouse] SHALL support using the `DELETE` statement on all MergeTree table engines:

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

### Immediate Removal For Selects

#### RQ.SRS-023.ClickHouse.LightweightDelete.ImmediateRemovalForSelects
version: 1.0

[ClickHouse] SHALL immediately remove all rows for subsequent `SELECT`s after `DELETE` statement is executed
and the subsequent `SELECT` statements SHALL not apply the original `WHERE` conditions specified in the `DELETE`.

For example,

```sql
SELECT count() FROM table;
DELETE FROM table WHERE <conditions which may expensive to calculate>;
SELECT count() FROM table;  -- deleted rows are not returned
```

### Multiple Deletes

#### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes
version: 1.0

[ClickHouse] SHALL support using multiple `DELETE` statements on the same table.

#### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.Limitations
version: 1.0

[ClickHouse] SHALL have the same limitations on the number of `DELETE`s as for the number of `INSERT`s.

#### Concurrent Deletes

##### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.ConcurrentDelete
version: 1.0

[ClickHouse] `DELETE` statement SHALL perform correctly when there are multiple concurrent `DELETE` statements.

##### RQ.SRS-023.ClickHouse.LightweightDelete.MultipleDeletes.ConcurrentDeleteOverlap
version: 1.0

[ClickHouse] `DELETE` statement SHALL perform correctly when there are multiple concurrent `DELETE` statements targetting the same rows.

### Synchronous Operation

#### RQ.SRS-023.ClickHouse.LightweightDelete.SynchronousOperationOnSingleNode
version: 1.0

[ClickHouse] SHALL support synchronous operation of the `DELETE` statement on a single node.

### Efficient Physical Data Removal

#### RQ.SRS-023.ClickHouse.LightweightDelete.EfficientPhysicalDataRemoval
version: 1.0

[ClickHouse] SHALL support efficient removal of physical data from the tables that had rows 
deleted using the `DELETE` statement.

### Performance

#### `DELETE` vs `SELECT`

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance
version: 1.0

[ClickHouse] SHALL have similar performance between `DELETE` and `SELECT` statements using the same condition
and SHALL use table primary key and secondary indexes if present.

#### Concurrent Queries

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.ConcurrentQueries
version: 1.0

[ClickHouse] SHALL not have major degradation in query response times during the deletion operation.

#### Post Delete `SELECT`s

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.PostDelete
version: 1.0

[ClickHouse] `SELECT` statement performance SHALL not degrade or degrade insignificantly on tables that contain rows deleted
using the `DELETE` statement.

#### Large Number of Partitions

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.LargeNumberOfPartitions
version: 1.0

[ClickHouse] `DELETE` statement SHALL have acceptable performance when tables have a very large number of partitions.

#### Large Number of Parts in Partitions

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.LargeNumberOfPartsInPartitions
version: 1.0

[ClickHouse] `DELETE` statement SHALL have acceptable performance when tables have a very large number of parts in partitions.

### Acceptance Criteria Using Reference `OnTime` Dataset

#### `OnTime` Dataset

For the acceptance criteria, the OnTime database SHALL be used that is available for download at the following location:

https://clickhouse.tech/docs/en/getting-started/example-datasets/ontime/

The reference table SHALL be preloaded with data between 1987 and 2019, inclusively.

Schema:

```sql
CREATE TABLE `ontime`
(
    `Year`                            UInt16,
    `Quarter`                         UInt8,
    `Month`                           UInt8,
    `DayofMonth`                      UInt8,
    `DayOfWeek`                       UInt8,
    `FlightDate`                      Date,
    `Reporting_Airline`               String,
    `DOT_ID_Reporting_Airline`        Int32,
    `IATA_CODE_Reporting_Airline`     String,
    `Tail_Number`                     Int32,
    `Flight_Number_Reporting_Airline` String,
    `OriginAirportID`                 Int32,
    `OriginAirportSeqID`              Int32,
    `OriginCityMarketID`              Int32,
    `Origin`                          FixedString(5),
    `OriginCityName`                  String,
    `OriginState`                     FixedString(2),
    `OriginStateFips`                 String,
    `OriginStateName`                 String,
    `OriginWac`                       Int32,
    `DestAirportID`                   Int32,
    `DestAirportSeqID`                Int32,
    `DestCityMarketID`                Int32,
    `Dest`                            FixedString(5),
    `DestCityName`                    String,
    `DestState`                       FixedString(2),
    `DestStateFips`                   String,
    `DestStateName`                   String,
    `DestWac`                         Int32,
    `CRSDepTime`                      Int32,
    `DepTime`                         Int32,
    `DepDelay`                        Int32,
    `DepDelayMinutes`                 Int32,
    `DepDel15`                        Int32,
    `DepartureDelayGroups`            String,
    `DepTimeBlk`                      String,
    `TaxiOut`                         Int32,
    `WheelsOff`                       Int32,
    `WheelsOn`                        Int32,
    `TaxiIn`                          Int32,
    `CRSArrTime`                      Int32,
    `ArrTime`                         Int32,
    `ArrDelay`                        Int32,
    `ArrDelayMinutes`                 Int32,
    `ArrDel15`                        Int32,
    `ArrivalDelayGroups`              Int32,
    `ArrTimeBlk`                      String,
    `Cancelled`                       UInt8,
    `CancellationCode`                FixedString(1),
    `Diverted`                        UInt8,
    `CRSElapsedTime`                  Int32,
    `ActualElapsedTime`               Int32,
    `AirTime`                         Nullable(Int32),
    `Flights`                         Int32,
    `Distance`                        Int32,
    `DistanceGroup`                   UInt8,
    `CarrierDelay`                    Int32,
    `WeatherDelay`                    Int32,
    `NASDelay`                        Int32,
    `SecurityDelay`                   Int32,
    `LateAircraftDelay`               Int32,
    `FirstDepTime`                    String,
    `TotalAddGTime`                   String,
    `LongestAddGTime`                 String,
    `DivAirportLandings`              String,
    `DivReachedDest`                  String,
    `DivActualElapsedTime`            String,
    `DivArrDelay`                     String,
    `DivDistance`                     String,
    `Div1Airport`                     String,
    `Div1AirportID`                   Int32,
    `Div1AirportSeqID`                Int32,
    `Div1WheelsOn`                    String,
    `Div1TotalGTime`                  String,
    `Div1LongestGTime`                String,
    `Div1WheelsOff`                   String,
    `Div1TailNum`                     String,
    `Div2Airport`                     String,
    `Div2AirportID`                   Int32,
    `Div2AirportSeqID`                Int32,
    `Div2WheelsOn`                    String,
    `Div2TotalGTime`                  String,
    `Div2LongestGTime`                String,
    `Div2WheelsOff`                   String,
    `Div2TailNum`                     String,
    `Div3Airport`                     String,
    `Div3AirportID`                   Int32,
    `Div3AirportSeqID`                Int32,
    `Div3WheelsOn`                    String,
    `Div3TotalGTime`                  String,
    `Div3LongestGTime`                String,
    `Div3WheelsOff`                   String,
    `Div3TailNum`                     String,
    `Div4Airport`                     String,
    `Div4AirportID`                   Int32,
    `Div4AirportSeqID`                Int32,
    `Div4WheelsOn`                    String,
    `Div4TotalGTime`                  String,
    `Div4LongestGTime`                String,
    `Div4WheelsOff`                   String,
    `Div4TailNum`                     String,
    `Div5Airport`                     String,
    `Div5AirportID`                   Int32,
    `Div5AirportSeqID`                Int32,
    `Div5WheelsOn`                    String,
    `Div5TotalGTime`                  String,
    `Div5LongestGTime`                String,
    `Div5WheelsOff`                   String,
    `Div5TailNum`                     String
) ENGINE = MergeTree
      PARTITION BY Year
      ORDER BY (IATA_CODE_Reporting_Airline, FlightDate)
      SETTINGS index_granularity = 8192;
```

#### `INSERT` Reference Queries

For each month number in the year 2020 do the following:

```sql
INSERT INTO ontime SELECT * FROM ontime_base WHERE Month = {number}
```

#### `SELECT` Reference Queries

##### Query 1: Average number of flights per month

```sql
SELECT avg(c1)
FROM
(
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
);
```

##### Query 2: The number of flights per day from the year 2000 to 2008

```sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

##### Query 3: The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2000-2008

```sql
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY DayOfWeek
ORDER BY c DESC;
```

##### Query 4: The number of delays by the airport for 2000-2008

```sql
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2000 AND Year<=2008
GROUP BY Origin
ORDER BY c DESC
LIMIT 10;
```

##### Query 5: The number of delays by carrier for 2007

```sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*)
FROM ontime
WHERE DepDelay>10 AND Year=2007
GROUP BY Carrier
ORDER BY count(*) DESC;
```

##### Query 6: The percentage of delays by carrier for 2007

```sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year=2007
GROUP BY Carrier
ORDER BY c3 DESC
```

##### Query 7: The previous request for a broader range of years, 2000-2008

```sql
SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay>10)*100 AS c3
FROM ontime
WHERE Year>=2000 AND Year<=2008
GROUP BY Carrier
ORDER BY c3 DESC;
```

##### Query 8: Percentage of flights delayed for more than 10 minutes, by year

```sql
SELECT Year, avg(DepDelay>10)*100
FROM ontime
GROUP BY Year
ORDER BY Year;
```

##### Query 9: The most popular destinations by the number of directly connected cities for various year ranges

```sql
SELECT DestCityName, uniqExact(OriginCityName) AS u
FROM ontime
WHERE Year >= 2000 and Year <= 2010
GROUP BY DestCityName
ORDER BY u DESC LIMIT 10;
```

##### Query 10: Flights per year

```sql
SELECT Year, count(*) AS c1
FROM ontime
GROUP BY Year;
```

#### `DELETE` Reference Queries

##### Query 1: Deleting All Rows In a Single Partition

```sql
DELETE * FROM ontime WHERE Year = 1990
```

##### Query 2: Delete All Rows In Various Partitions

```sql
DELETE * FROM ontime WHERE Year % 2 = 0 
```

##### Query 3: Delete Some Rows In All Partitions (Large Granularity)

```sql
DELETE * FROM ontime WHERE Month = 2 
```

##### Query 4: Delete Some Rows In All Partitions (Small Granularity)

```sql
DELETE * FROM ontime WHERE DayofMonth = 2 
```

##### Query 5: Delete Some Rows In One Partition (Very Small Granularity)

```sql
DELETE * FROM ontime WHERE FlightDate = '2020-01-01'
```

#### Acceptance performance
##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.Inserts
version: 1.0

[ClickHouse] SHALL have a similar ingestion performance when the reference dataset table 
has deleted rows vs no deleted rows when using [insert reference queries].

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.DeleteQueryExecutionTime
version: 1.0

[ClickHouse] SHALL execute each query in the [delete reference queries] set against the reference dataset table within 2 sec.

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.ConcurrentSelectsAndDeletes
version: 1.0

[ClickHouse] SHALL keep reference dataset table usable while the [delete reference queries]
are being executed concurrently with the [select reference queries]. 
No major degradation in query response time SHALL be seen.

##### RQ.SRS-023.ClickHouse.LightweightDelete.Performance.Acceptance.OnTimeDataset.ConcurrentInsertsAndDeletes
version: 1.0

[ClickHouse] SHALL not slow down or lockup data ingestion into the reference dataset table
when [delete reference queries] are executed concurrently with the [insert reference queries].

### Immutable Parts And Garbage Collection

#### RQ.SRS-023.ClickHouse.LightweightDelete.ImmutablePartsAndGarbageCollection
version: 1.0

[ClickHouse] parts affected by the `DELETE` statement SHALL stay immutable and
the deleted rows SHALL be garbage collected in a scheduled merge.

### Compatibility

#### Concurrent Operations

##### RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentOperations
version: 1.0

[ClickHouse] `DELETE` statement SHALL perform correctly with the other concurrent database operations.
The cluster SHALL remain usable and not slow down during the deletion operation.
Examples of operations,

* `INSERT`
* `SELECT`
* `ALTER DELETE`
* `ALTER UPDATE`
* `ALTER ADD/REMOVE/MODIFY COLUMN`
* Background merge
* Replication
* TTL moves
* TTL deletes
* Column TTL

##### Concurrent Inserts & Deletes On Many Parts

######  RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentInserts&DeletesOnManyParts
version: 1.0

[ClickHouse] SHALL execute `INSERT` and `DELETE` statements concurrently when `INSERT` creates many parts.

##### Concurrent Inserts & Deletes Of The Same Data

######  RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentInserts&DeletesOfTheSameData
version: 1.0

[ClickHouse] SHALL execute `INSERT` and `DELETE` statements that use the same data in the order they were ran.

##### Concurrent Delete & Alter Delete

######  RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.ConcurrentDelete&AlterDelete
version: 1.0

[ClickHouse] SHALL support executing `DELETE` and `ALTER TABLE DELETE` statements concurrently.

#### Projections

##### RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.Projections
version: 1.0

[ClickHouse] `DELETE` statement SHALL be compatible with tables that have one or more projections.

#### Views

##### RQ.SRS-023.ClickHouse.LightweightDelete.Compatibility.Views
version: 1.0

[ClickHouse] `DELETE` statement SHALL be compatible with tables that have one or more views. 
Including 

* normal
* materialized
* live
* window

### Hard Restarts

#### RQ.SRS-023.ClickHouse.LightweightDelete.HardRestarts
version: 1.0

[ClickHouse] SHALL either finish the `DELETE` or return the system to before the `DELETE` started after a hard restart.

### Non Corrupted Server State

#### RQ.SRS-023.ClickHouse.LightweightDelete.NonCorruptedServerState
version: 1.0

[ClickHouse] SHALL prevent server state from being corrupted if the server crashes during a `DELETE`.

### Server Restart

#### RQ.SRS-023.ClickHouse.LightweightDelete.ServerRestart
version: 1.0

[ClickHouse] SHALL keep rows deleted after a server restart.

### Non Deterministic Functions

#### RQ.SRS-023.ClickHouse.LightweightDelete.NonDeterministicFunctions
version: 1.0

[ClickHouse] SHALL support delete statement with non deterministic functions in the `WHERE` condition.

### Lack of Disk Space

#### RQ.SRS-023.ClickHouse.LightweightDelete.LackOfDiskSpace
version: 1.0

[ClickHouse] SHALL reserve space to avoid breaking in the middle.

### Multidisk Configurations

#### RQ.SRS-023.ClickHouse.LightweightDelete.MultidiskConfigurations
version: 1.0

[ClickHouse] SHALL store the masks used for `DELETE` on the same disks as the parts.

### S3 Disks

#### RQ.SRS-023.ClickHouse.LightweightDelete.S3Disks
version: 1.0

[ClickHouse] SHALL support using `DELETE` on S3 disks.

### Backups

#### RQ.SRS-023.ClickHouse.LightweightDelete.Backups
version: 1.0

[ClickHouse] SHALL keep masks during backups and finish executing any running `DELETE` queries.

### Drop Empty Part

#### RQ.SRS-023.ClickHouse.LightweightDelete.DropEmptyPart
version: 1.0

[ClickHouse] SHALL schedule dropping the part if all of the rows are deleted from the table.

### Deletes per Second

#### RQ.SRS-023.ClickHouse.LightweightDelete.DeletesPerSecond
version: 1.0

[ClickHouse] SHALL only run a few `DELETE` statements per second.

### Upgrade Server

#### RQ.SRS-023.ClickHouse.LightweightDelete.UpgradeServer
version: 1.0

[ClickHouse] SHALL require servers to upgrade when delete mask format changes.

[SRS]: #srs
[select reference queries]: #select-reference-queries
[delete reference queries]: #delete-reference-queries
[insert reference queries]: #insert-reference-queries
[ClickHouse]: https://clickhouse.tech

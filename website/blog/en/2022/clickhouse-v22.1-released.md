---
title: 'What''s New in ClickHouse 22.1'
image: 'https://blog-images.clickhouse.com/en/2022/clickhouse-v22-1/featured.jpg'
date: '2022-01-26'
author: 'Alexey Milovidov'
tags: ['company', 'community']
---

22.1 is our first release in the new year. It includes 2,599 new commits from 133 contributors, including 44 new contributors:

> 13DaGGeR, Adri Fernandez, Alexey Gusev, Anselmo D. Adams, Antonio Andelic, Ben, Boris Kuschel, Christoph Wurm, Chun-Sheng, Li, Dao, DimaAmega, Dmitrii Mokhnatkin, Harry-Lee, Justin Hilliard, MaxTheHuman, Meena-Renganathan, Mojtaba Yaghoobzadeh, N. Kolotov, Niek, Orkhan Zeynalli, Rajkumar, Ryad ZENINE, Sergei Trifonov, Suzy Wang, TABLUM.IO, Vitaly Artemyev, Xin Wang, Yatian Xu, Youenn Lebras, dalei2019, fanzhou, gulige, lgbo-ustc, minhthucdao, mreddy017, msirm, olevino, peter279k, save-my-heart, tekeri, usurai, zhoubintao, 李扬.

Don't forget to run `SELECT * FROM system.contributors` on your production server!

Let's describe the most important new features in 22.1.

## Schema Inference

Let's look at the following query as an example:

```
SELECT * FROM url('https://datasets.clickhouse.com/github_events_v2.native.xz', Native,
$$
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4,
                    'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8,
                    'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11,
                    'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15,
                    'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19,
                    'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9,
                'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    body String,
    path String,
    position Int32,
    line Int32,
    ref LowCardinality(String),
    ref_type Enum('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    creator_user_login LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    locked UInt8,
    assignee LowCardinality(String),
    assignees Array(LowCardinality(String)),
    comments UInt32,
    author_association Enum('NONE' = 0, 'CONTRIBUTOR' = 1, 'OWNER' = 2, 'COLLABORATOR' = 3, 'MEMBER' = 4, 'MANNEQUIN' = 5),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    requested_reviewers Array(LowCardinality(String)),
    requested_teams Array(LowCardinality(String)),
    head_ref LowCardinality(String),
    head_sha String,
    base_ref LowCardinality(String),
    base_sha String,
    merged UInt8,
    mergeable UInt8,
    rebaseable UInt8,
    mergeable_state Enum('unknown' = 0, 'dirty' = 1, 'clean' = 2, 'unstable' = 3, 'draft' = 4),
    merged_by LowCardinality(String),
    review_comments UInt32,
    maintainer_can_modify UInt8,
    commits UInt32,
    additions UInt32,
    deletions UInt32,
    changed_files UInt32,
    diff_hunk String,
    original_position UInt32,
    commit_id String,
    original_commit_id String,
    push_size UInt32,
    push_distinct_size UInt32,
    member_login LowCardinality(String),
    release_tag_name String,
    release_name String,
    review_state Enum('none' = 0, 'approved' = 1, 'changes_requested' = 2, 'commented' = 3, 'dismissed' = 4, 'pending' = 5)
$$)
```

In this query we are importing data with the `url` table function. Data is posted on an HTTP server in a `.native.xz` file. The most annoying part of this query is that we have to specify the data structure and the format of this file.

In the new ClickHouse release 22.1 it becomes much easier:

```
SELECT * FROM url('https://datasets.clickhouse.com/github_events_v2.native.xz')
```

Cannot be more easy! How is that possible?

Firstly, we detect the data format automatically from the file extension. Here it is `.native.xz`, so we know that the data is compressed by `xz` (LZMA2) compression and is represented in `Native` format. The `Native` format already contains all information about the types and names of the columns, and we just read and use it.

It works for every format that contains information about the data types: `Native`, `Avro`, `Parquet`, `ORC`, `Arrow` as well as `CSVWithNamesAndTypes`, `TSVWithNamesAndTypes`.

And it works for every table function that reads files: `s3`, `file`, `hdfs`, `url`, `s3Cluster`, `hdfsCluster`.

A lot of magic happens under the hood. It does not require reading the whole file in memory. For example, Parquet format has metadata at the end of file. So, we read the header first to find where the metadata is located, then do a range request to read the metadata about columns and their types, then continue to read the requested columns. And if the file is small, it will be read with a single request.

If you want to extract the structure from the file without data processing, the DESCRIBE query is available:

```
DESCRIBE url('https://datasets.clickhouse.com/github_events_v2.native.xz')
```

Data structure can be also automatically inferred from `JSONEachRow`, `CSV`, `TSV`, `CSVWithNames`, `TSVWithNames`, `MsgPack`, `Values` and `Regexp` formats.

For `CSV`, either Float64 or String is inferred. For `JSONEachRow` the inference of array types is supported, including multidimensional arrays. Arrays of non-uniform types are mapped to Tuples. And objects are mapped to the `Map` data type.

If a format does not have column names (like `CSV` without a header), the names `c1`, `c2`, ... are used.

File format is detected from the file extension: `csv`, `tsv`, `native`, `parquet`, `pb`, `ndjson`, `orc`... For example, `.ndjson` file is recognized as `JSONEachRow` format and `.csv` is recognized as header-less `CSV` format in ClickHouse, and if you want `CSVWithNames` you can specify the format explicitly.

We support "schema on demand" queries. For example, the autodetected data types for `TSV` format are Strings, but you can refine the types in your query with the `::` operator:

```
SELECT c1 AS domain, uniq(c2::UInt64), count() AS cnt
  FROM file('hits.tsv')
  GROUP BY domain ORDER BY cnt DESC LIMIT 10
```

As a bonus, `LineAsString` and `RawBLOB` formats also get type inference. Try this query to see how I prefer to read my favorite website:

```
SELECT extractTextFromHTML(*)
    FROM url('https://news.ycombinator.com/', LineAsString);
```

Schema autodetection also works while creating `Merge`, `Distributed` and `ReplicatedMegreTree` tables. When you create the first replica, you have to specify the table structure. But when creating all the subsequent replicas, you only need `CREATE TABLE hits
ENGINE = ReplicatedMegreTree(...)` without listing the columns - the definition will be copied from another replica.

This feature is implemented by **Pavel Kruglov** with the inspiration of initial work by **Igor Baliuk** and with additions by **ZhongYuanKai**.

## Realtime Resource Usage In clickhouse-client

`clickhouse-client` is my favorite user interface for ClickHouse. It is an example of how friendly every command line application should be.

Now it shows realtime CPU and memory usage for the query directly in the progress bar:

![resource usage](https://blog-images.clickhouse.com/en/2022/clickhouse-v22-1/progress.png)

For distributed queries, we show both total memory usage and max memory usage per host.

This feature was made possible by implementation of distributed metrics forwarding by **Dmitry Novik**. I have added this small visualization to clickhouse-client, and now it is possible to add similar info in every client using native ClickHouse protocol. 

## Parallel Query Processing On Replicas

ClickHouse is a distributed MPP DBMS. It can scale up to use all CPU cores on one server and scale out to use computation resources of multiple shards in a cluster.

But each shard usually contains more than one replica. And by default ClickHouse is using the resources of only one replica on every shard. E.g. if you have a cluster of 6 servers with 3 shards and two replicas on each, a query will use just three servers instead of all six.

There was an option to enable `max_parallel_replicas`, but that option required specifying a "sampling key", it was inconvenient to use and did not scale well.

Now we have a setting to enable the new parallel processing algorithm: `allow_experimental_parallel_reading_from_replicas`. If it is enabled, replicas will *dynamically* select and distribute the work across them.

It works perfectly even if replicas have lower or higher amounts of computation resources. And it gives a complete result even if some replicas are stale.

This feature was implemented by **Nikita Mikhaylov**

## Service Discovery

When adding or removing nodes in a cluster, now you don't have to edit the config on every server. Just use automatic cluster and servers will register itself: 

```
<allow_experimental_cluster_discovery>1
</allow_experimental_cluster_discovery>

<remote_servers>
    <auto_cluster>
        <discovery>
            <path>/clickhouse/discovery/auto_cluster</path>
            <shard>1</shard>
        </discovery>
    </auto_cluster>
</remote_servers>
```

There is no need to edit the config when adding new replicas!

This feature was implemented by **Vladimir Cherkasov**.

## Sparse Encoding For Columns

If a column contains mostly zeros, we can encode it in sparse format
and automatically optimize calculations!

It is a special column encoding, similar to `LowCardinality`, but it's completely transparent and works automatically.

```
CREATE TABLE test.hits ...
ENGINE = MergeTree ORDER BY ...
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9
```

It allows compressing data better and optimizes computations, because data in sparse columns will be processed directly in sparse format in memory.

Sparse or full format is selected based on column statistics that is calculated on insert and updated on background merges.

Developed by **Anton Popov**.

We also want to make LowCardinality encoding automatic, stay tuned!

## Diagnostic Tool For ClickHouse

It is a gift from the Yandex Cloud team. They have a tool to collect a report about ClickHouse instances to provide all the needed information for support. They decided to contribute this tool to open-source!

You can find the tool here: [utils/clickhouse-diagnostics](https://github.com/ClickHouse/ClickHouse/tree/master/
utils/clickhouse-diagnostics)

Developed by **Alexander Burmak**. 

## Integrations

Plenty of new integrations were added in 22.1:

Integration with **Hive** as a foreign table engine for SELECT queries, contributed by **Taiyang Li** and reviewed by **Ksenia Sumarokova**.

Integration with **Azure Blob Storage** similar to S3, contributed by **Jakub Kuklis** and reviewed by **Ksenia Sumarokova**.

Support for **hdfsCluster** table function similar to **s3Cluster**, contributed by **Zhichang Yu** and reviewed by **Nikita Mikhailov**.

## Statistical Functions

I hope you have always dreamed of calculating the Cramer's V and Theil's U coefficients in ClickHouse, because now we have these functions for you and you have to deal with it.

```
:) SELECT cramersV(URL, URLDomain) FROM test.hits

0.98

:) SELECT cramersV(URLDomain, ResolutionWidth) FROM test.hits

0.27
```

It can calculate some sort of dependency between categorical (discrete) values. You can imagine it like this: there is a correlation function `corr` but it is only applicable for linear dependencies; there is a rank correlation function `rankCorr` but it is only applicable for ordered values. And now there are a few functions to calculate *something* for discrete values.

Developers: **Artem Tsyganov**, **Ivan Belyaev**, **Alexey Milovidov**.


## ... And Many More

Read the [full changelog](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md) for the 22.1 release and follow [the roadmap](https://github.com/ClickHouse/ClickHouse/issues/32513).

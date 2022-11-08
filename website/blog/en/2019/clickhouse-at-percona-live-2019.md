---
title: 'ClickHouse at Percona Live 2019'
image: 'https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/main.jpg'
date: '2019-06-04'
tags: ['Percona Live','USA','Texas','Austin', 'events', 'conference']
---

This year American episode of [Percona Live](https://www.percona.com/live/19/) took place in nice waterfront location in Austin, TX, which welcomed open source database experts with pretty hot weather. ClickHouse community is undeniably growing and it became a common database product to give a talk about or at least compare or refer to, while just [two short years ago](../2017/clickhouse-at-percona-live-2017.md) it was more like “wth is ClickHouse?”.

Alexey Rubin from VirtualHealth compared two column-oriented databases: ClickHouse and MariaDB Column Store. Bottom line was no surprise, ClickHouse is noticeably faster and MariaDB is more familiar for MySQL users, details were useful though.
![Alexey Rubin from VirtualHealth](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/1.jpg)

Alexey Milovidov from Yandex have demonstrated how exactly ClickHouse became even faster in recent releases.
![Alexey Milovidov from Yandex](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/2.jpg)

Alexander Zaitsev and Robert Hodges from Altinity have given an entry level tutorial to ClickHouse, which included loading in demo dataset and going through realistic queries against it with some extra variation demonstrating possible query optimization techniques. [Slides](https://www.percona.com/live/19/sites/default/files/slides/Making%20HTAP%20Real%20with%20TiFlash%20--%20A%20TiDB%20Native%20Columnar%20Extension%20-%20FileId%20-%20174070.pdf). Also Altinity was sponsoring the ClickHouse booth in Expo Hall which became an easy spot for people interested in ClickHouse to chat outside of talks.
![Alexander Zaitsev and Robert Hodges from Altinity](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/3.jpg)

Ruoxi Sun from PingCAP introduced TiFlash, column-oriented add-on to TiDB for analytics based on ClickHouse source code. Basically it provides [MergeTree](/docs/en/engines/table-engines/mergetree-family/mergetree/)-like table engine that is hooked up to TiDB replication and has in-memory row-friendly cache for recent updates. Unfortunately, PingCAP has no plans  to bring TiFlash to opensource at the moment. [Slides](https://www.percona.com/live/19/sites/default/files/slides/Making%20HTAP%20Real%20with%20TiFlash%20--%20A%20TiDB%20Native%20Columnar%20Extension%20-%20FileId%20-%20174070.pdf).
![Ruoxi Sun from PingCAP](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/4.jpg)

ClickHouse has also been covered in talk by Jervin Real and Francisco Bordenave from Percona with overview of moving and replicating data around MySQL-compatible storage solutions. [Slides](https://www.percona.com/live/19/sites/default/files/slides/Replicating%20MySQL%20Data%20to%20TiDB%20For%20Real-Time%20Analytics%20-%20FileId%20-%20187672.pdf).
![Jervin Real](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/5.jpg)

ClickHouse represented columnar storage systems in venture beyond relational by Marcos Albe from Percona.
![Marcos Albe from Percona](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/6.jpg)

Jervin Real from Percona have demonstrated real case study of applying ClickHouse in practice. It heavily involved manual partitions manipulation, hopefully audience have understood that it is an option, but not exactly a best practice for most use cases. [Slides](https://www.percona.com/live/19/sites/default/files/slides/Low%20Cost%20Transactional%20and%20Analytics%20With%20MySQL%20and%20Clickhouse,%20Have%20Your%20Cake%20and%20Eat%20It%20Too!%20-%20FileId%20-%20187674.pdf).
![Jervin Real from Percona](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/7.jpg)

Evgeny Potapov from ITSumma went through modern options for time-series storage and once more confirmed ClickHouse is leading the way in this field as well.
![Evgeny Potapov from ITSumma](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/8.jpg)

Event location in the center of US provided equal opportunities for peoplefrom East and West Coast to show up, but presence of people from other countries was also quite noticeable. The content they all brought in was top notch as usual.
![The venue](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/9.jpg)

Austin after the Event.
![Austin](https://blog-images.clickhouse.com/en/2019/clickhouse-at-percona-live-2019/10.jpg)

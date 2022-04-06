---
title: 'ClickHouse Moscow Meetup October 19, 2021'
image: 'https://blog-images.clickhouse.com/en/2021/clickhouse-october-moscow-meetup/featured.jpg'
date: '2021-11-11'
author: 'Rich Raposa'
tags: ['company', 'community']
---

ClickHouse organized an online Meetup on October 19, 2021, hosted by our very own co-founder and CTO, Alexey Milovidov. There are a lot of new features to discuss in the 21.10 version of ClickHouse, along with many more new features coming up on the roadmap.

There were over 200 attendees in person for the Meetup and 3,853 viewers online, and we want to thank everyone who attended live. You can watch the recording of the Meetup on YouTube [here](https://www.youtube.com/watch?v=W6h3_xykd2Y).

Alexey Milovidov, Chief Technology Officer, welcomed and updated the community on ClickHouse Inc.'s latest news. Maksim Kita, Sr. Software Engineer at ClickHouse, started with a discussion on the new User Defined Functions (UDFs) available in 21.10. UDFs can be defined as lambda expressions using the CREATE FUNCTION command. For example:

```
CREATE FUNCTION a_plus_b AS (a, b) -> a + b
```

In addition to UDFs, there are two new table engines - Executable and ExecutablePool - that can stream records via stdin and stdout through custom scripts written in whatever language you prefer. For details, be sure to check out our [new training lesson on What's New in ClickHouse 21.10](https://clickhouse.com/learn/lessons/whatsnew-clickhouse-21.10/).

You can now encrypt your data stored on S3, HDFS, external disks, or on a local disk. ClickHouse developers Vitaly Baranov and Artur Filatenkov discussed the details and benefits of encrypting your data at rest in ClickHouse. Vitaly presented the new full disk encryption feature and Arthur presented column-level encryption.

![Disk Encryption Performance](https://blog-images.clickhouse.com/en/2021/clickhouse-october-moscow-meetup/disk-encryption-performance.jpg)

![Arthur Filatenkov](https://blog-images.clickhouse.com/en/2021/clickhouse-october-moscow-meetup/arthur-filatenkov.jpg)

Alexey then spent 40 minutes discussing some of the amazing new features on the ClickHouse roadmap, including:

* ClickHouse Keeper: a new C++ coordination system for ClickHouse designed as an alternative to ZooKeeper
* Support for working with semi-structured data, including JSON objects with arbitrary nested objects
* Asynchronous insert mode - now you can insert data without batching!

After the talk, Alexey took questions from users on:

* How to parse User-Agent in ClickHouse 
* Is it true that ClickHouse developers have a ClickHouse tattoo

![YAML Configuration](https://blog-images.clickhouse.com/en/2021/clickhouse-october-moscow-meetup/yaml-configuration.jpg)

* If you are excited about ClickHouse, be sure to join us on [Telegram](https://t.me/clickhouse_en)
* We also have a community Slack workspace be sure to join [here](https://clickhousedb.slack.com/).
* If you are new to ClickHouse and want to see it in action, check out our [Getting Started lesson](https://clickhouse.com/learn/lessons/gettingstarted/).

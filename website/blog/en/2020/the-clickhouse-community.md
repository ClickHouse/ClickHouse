---
title: 'The ClickHouse Community'
image: 'https://blog-images.clickhouse.com/en/2020/the-clickhouse-community/clickhouse-community-history.png'
date: '2020-12-10'
author: 'Robert Hodges'
tags: ['community', 'open source', 'telegram', 'meetup']
---

One of the great “features” of ClickHouse is a friendly and welcoming community. In this article we would like to outline how the ClickHouse community arose, what it is today, and how you can get involved. There is a role for everyone, from end users to contributors to corporate friends. Our goal is to make the community welcoming to every person who wants to join.

But first, let’s review a bit of history, starting with how ClickHouse first developed at [Yandex](https://yandex.com/company/).

## Origins at Yandex

ClickHouse began as a solution for web analytics in [Yandex Metrica](https://metrica.yandex.com/about?). Metrica is a popular service for analyzing website traffic that is now #2 in the market behind Google Analytics. In 2008 [Alexey Milovidov](https://github.com/alexey-milovidov), an engineer on the Metrica team, was looking for a database that could create reports on metrics like number of page views per day, unique visitors, and bounce rate, without aggregating the data in advance. The idea was to provide a wide range of metric data and let users ask any question about them.

This is a classic problem for data warehouses. However, Alexey could not find one that met Yandex requirements, specifically large datasets, linear scaling, high efficiency, and compatibility with SQL tools. In a nutshell: like MySQL but for analytic applications. So Alexey wrote one. It started as a prototype to do GROUP BY operations.

The prototype evolved into a full solution with a name, ClickHouse, short for “Clickstream Data Warehouse”. Alexey added additional features including SQL support and the MergeTree engine. The SQL dialect was superficially similar to MySQL, [which was also used in Metrica](/blog/en/2016/evolution-of-data-structures-in-yandex-metrica/) but could not handle query workloads without complex pre-aggregation. By 2011 ClickHouse was in production for Metrica.

Over the next 5 years Alexey and a growing team of developers extended ClickHouse to cover new use cases. By 2016 ClickHouse was a core Metrica backend service. It was also becoming entrenched as a data warehouse within Yandex, extending to use cases like service monitoring, network flow logs, and event management. ClickHouse had evolved from the original one-person project to business critical software with a full team of a dozen engineers led by Alexey.

By 2016, ClickHouse had an 8 year history and was ready to become a major open source project.  Here’s a timeline that tracks major developments as a time series.

<img src="https://blog-images.clickhouse.com/en/2020/the-clickhouse-community/clickhouse-community-history.png"/>

## ClickHouse goes open source

Yandex open sourced ClickHouse under an Apache 2.0 license in 2016. There were numerous reasons for this step.

* Promote adoption within Yandex by making it easier for internal departments to get builds.
* Ensure that ClickHouse would continue to evolve by creating a community to nurture it.
* Motivate developers to contribute to and use ClickHouse due to the open source “cool” factor.
* Improve ClickHouse quality by making the code public. Nobody wants their name visible on bad code. ;-)
* Showcase Yandex innovation to a worldwide audience.

Alexey and the development team moved ClickHouse code to a Github repo under the Yandex organization and began issuing community builds as well as accepting external contributions. They simultaneously began regular meetups to popularize ClickHouse and build a community around it. The result was a burst of adoption across multiple regions of the globe.

ClickHouse quickly picked up steam in Eastern Europe. The first ClickHouse meetups started in 2016 and have grown to include 200 participants for in-person meetings and up to 400 for online meetings.  ClickHouse is now widely used in start-ups in Russia as well as other Eastern European countries. Developers located in Eastern Europe continue to supply more contributions to ClickHouse than any other region.

ClickHouse also started to gain recognition in the US and Western Europe. [CloudFlare](https://www.cloudflare.com/) published a widely read blog article about [their success using ClickHouse for DNS analytics](https://blog.cloudflare.com/how-cloudflare-analyzes-1m-dns-queries-per-second/).

ClickHouse also took off in China. The first meetup in China took place in 2018 and attracted enormous interest. In-person meetups included over 400 participants.  Online meetings have reached up to 1000 online viewers.

In 2019 a further step occurred as ClickHouse moved out from under the Yandex Github organization into a separate [ClickHouse organization](https://github.com/ClickHouse). The new organization includes ClickHouse server code plus core ecosystem projects like the cpp and ODBC drivers.

ClickHouse community events shifted online following world-wide disruptions due to COVID-19, but growth in usage continued. One interesting development has been the increasing number of startups using ClickHouse as a backend. Many of these are listed on the [ClickHouse Adopters](/docs/en/introduction/adopters/) page. Also, additional prominent companies like eBay, Uber, and Flipcart went public in 2020 with stories of successful ClickHouse usage.

## The ClickHouse community today

As of 2020 the ClickHouse community includes developers and users from virtually every region of the globe. Yandex engineers continue to supply a majority of pull requests to ClickHouse itself. Altinity follows in second place with contributions to ClickHouse core and ecosystem projects. There is also substantial in-house development on ClickHouse (e.g. on private forks) within Chinese internet providers.

The real success, however, has been the huge number of commits to ClickHouse core from people in outside organizations. The following list shows the main outside contributors:

* Azat Khuzhin
* Amos Bird
* Winter Zhang
* Denny Crane
* Danila Kutenin
* Hczhcz
* Marek Vavruša
* Guillaume Tassery
* Sundy Li
* Mikhail Shiryaev
* Nicolae Vartolomei
* Igor Hatarist
* Andrew Onyshchuk
* BohuTANG
* Yu Zhi Chang
* Kirill Shvakov
* Alexander Krasheninnikov
* Simon Podlipsky
* Silviu Caragea
* Flynn ucasFL
* [And over 550 more...](https://github.com/ClickHouse/ClickHouse/graphs/contributors)

ClickHouse ecosystem projects are also growing rapidly. Here is a selected list of active Github projects that help enable ClickHouse applications, sorted by number of stars.

* [sqlpad/sqlpad](https://github.com/sqlpad/sqlpad) — Web-based SQL editor that supports ClickHouse
* [mindsdb/mindsdb](https://github.com/mindsdb/mindsdb) — Predictive AI layer for databases with ClickHouse support
* [x-ream/sqli](https://github.com/x-ream/sqli) — ORM SQL interface
* [tricksterproxy/trickster](https://github.com/tricksterproxy/trickster) — HTTP reverse proxy cache and time series dashboard accelerator
* [ClickHouse/clickhouse-go](https://github.com/ClickHouse/clickhouse-go) — Golang driver for ClickHouse
* [gohouse/gorose](https://github.com/gohouse/gorose) — A mini database ORM for Golang
* [ClickHouse/clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) — JDBC driver for ClickHouse
* [brockercap/Bifrost](https://github.com/brokercap/Bifrost) — Middleware to sync MySQL binlog to ClickHouse
* [mymarilyn/clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) — ClickHouse Python driver with native interface support
* [Vertamedia/clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana) — Grafana datasource for ClickHouse
* [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse) — PHP ClickHouse client
* [AlexAkulov/clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup) — ClickHouse backup and restore using cloud storage
* [And almost 1200 more...](https://github.com/search?o=desc&p=1&q=clickhouse&s=stars&type=Repositories)

## Resources

With the community growth numerous resources are available to users. At the center is the [ClickHouse org on Github](https://github.com/ClickHouse), which hosts [ClickHouse server code](https://github.com/ClickHouse/ClickHouse).  ClickHouse server documentation is available at the [clickhouse.com](/) website. It has [installation instructions](/docs/en/getting-started/install/) and links to ClickHouse community builds for major Linux distributions as well as Mac, FreeBSD, and Docker.

In addition, ClickHouse users have a wide range of ways to engage with the community and get help on applications.  These include both chat applications as well as meetups.  Here are some links to get started.

* Yandex Meetups — Yandex has regular in-person and online international and Russian-language meetups. Video recordings and online translations are available at the official [YouTube channel](https://www.youtube.com/c/ClickHouseDB/videos). Watch for announcements on the [clickhouse.com](/) site and [Telegram](https://t.me/clickhouse_ru).
* [SF Bay Area ClickHouse Meetup](https://www.meetup.com/San-Francisco-Bay-Area-ClickHouse-Meetup/) — The largest US ClickHouse meetup, with meetings approximately every 2 months.
* Chinese meetups occur at regular intervals with different sponsors.  Watch for announcements on clickhouse.com.
* Telegram - By far the largest forum for ClickHouse. It is the best place to talk to ClickHouse devs. There are two groups.
* [ClickHouse не тормозит](https://t.me/clickhouse_ru) (“ClickHouse does not slow down”) - Russian language Telegram group with 4,629 members currently.
* [ClickHouse](https://t.me/clickhouse_en) — English language group with 1,286 members.
* [ClickHouse Community Slack Channel](http://clickhousedb.slack.com) — Public channel for Slack users. It currently has 551 members.
* [ClickHouse.com.cn](http://clickhouse.com.cn/) — Chinese language site for ClickHouse-related announcements and questions.
* [Conference Presentations](https://github.com/ClickHouse/clickhouse-presentations) — ClickHouse developers like to talk and do so whenever they can.  Many recent presentations are stored in Github.  Also, look for ClickHouse presentations at Linux Foundation conferences, Data Con LA, Percona Live, and many other venues where there are presentations about data.
* Technical webinars — Altinity has a large library of technical presentations on ClickHouse and related applications on the [Altinity Youtube channel](https://www.youtube.com/channel/UCE3Y2lDKl_ZfjaCrh62onYA/featured).

If you know of additional resources please bring them to our attention.

## How you can contribute to ClickHouse

We welcome users to join the ClickHouse community in every capacity. There are four main ways to participate.

### Use ClickHouse and share your experiences

Start with the documentation. Download ClickHouse and try it out. Join the chat channels. If you encounter bugs, [log issues](https://github.com/ClickHouse/ClickHouse/issues) so we can get them fixed. Also, it’s easy to make contributions to the documentation if you have basic Github and markdown skills. Press the pencil icon on any page of the clickhouse.com website to edit pages and automatically generate pull requests to merge your changes.

If your company has deployed ClickHouse and is comfortable talking about it, please don't be shy. Add them to the [ClickHouse Adopters](/docs/en/introduction/adopters/) page so that others can learn from your experience.

### Become a ClickHouse developer

Write code to make ClickHouse better. Here are your choices.

* ClickHouse server — Start with the [“For Beginners” documentation](/docs/en/development/developer-instruction/) to learn how to build ClickHouse and submit PRs. Check out the current ClickHouse issues if you are looking for work. PRs that follow the development standards will be merged faster.

* Ecosystem projects — Most projects in the ClickHouse ecosystem accept PRs.  Check with each project for specific practices.

ClickHouse is also a great target for research problems.  Overall the years many dozens of university CS students have worked on ClickHouse features. Alexey Milovidov maintains an especially rich set of [project suggestions for students](https://github.com/ClickHouse/ClickHouse/issues/15065). Join Telegram and ask for help if you are interested.  Both Yandex and Altinity also offer internships.

## Where we go from here

ClickHouse has grown enormously from its origins as a basic prototype in 2008 to the popular SQL data warehouse users see today. Our community is the rock that will enable ClickHouse to become the default data warehouse worldwide. We are working together to create an inclusive environment where everyone feels welcome and has an opportunity to contribute. We welcome you to join!

This article was written with kind assistance from Alexey Milovidov, Ivan Blinkov, and Alexander Zaitsev.

_2020-12-11 [Robert Hodges](https://github.com/hodgesrm)_

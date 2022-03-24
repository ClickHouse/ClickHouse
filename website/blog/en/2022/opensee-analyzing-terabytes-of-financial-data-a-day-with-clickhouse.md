---
title: 'Opensee: Analyzing Terabytes of Financial Data a Day With ClickHouse'
image: 'https://blog-images.clickhouse.com/en/2022/opensee/featured.png'
date: '2022-02-22'
author: 'Christophe Rivoire, Elena Bessis'
tags: ['company', 'community']
---

We’d like to welcome Christophe Rivoire (UK Country Manager) and Elena Bessis (Product Marketing Assistant) from Opensee as guests to our blog. Today, they’re telling us how their product, powered by ClickHouse, allows financial institutions’ business users to directly harness 100% of their vast quantities of data instantly and on demand, with no size limitations. 

Opensee is a financial technology company providing real time self-service analytics solutions to financial institutions, which help them turn their big data challenges into a competitive advantage — unlocking vital opportunities led by business users. Opensee, formerly ICA, was started by a team of financial industry and technology experts frustrated that no simple big data analytics solution enabled them to dive deeper into all their data easily and efficiently, or perform what-if analysis on the hundreds of terabytes of data they were handling. 

So they built their own. 


## ClickHouse For Trillions Of Financial Data Points

Financial institutions have always been storing a lot of data (customer data, risk data, transaction data...) for their own decision processes and for regulatory reasons. Since the financial crisis, regulators all around the world have been significantly increasing the reporting requirements, insisting on longer historical ranges and deeper granularity. This combination has generated an exponential amount of data, which has forced financial institutions to review and upgrade their infrastructure. Opensee offers a solution to navigate all these very large data cubes, based on millions, billions or even trillions of data points. In order to build it, a data storage system capable of scaling horizontally with data and with fast OLAP query response time was required. In 2016, after thorough evaluation, Opensee concluded ClickHouse was the obvious solution. 

There are many use cases that involve storing and leveraging massive amounts of data on a daily basis, but Opensee built from the strength of their own expertise, evaluating risk linked to activities in the financial market. There are various types of risks (market risk, credit risk, liquidity risk…) and all of them need to aggregate a lot of data in order to calculate linear or non-linear indicators, both business and regulatory, and analyze all those numbers on the fly.

!["Dashboard in Opensee for a Market Risk use case"](https://blog-images.clickhouse.com/en/2022/opensee/dashboard.png)
_Dashboard in Opensee for a Market Risk use case_


## ClickHouse for Scalability, Granularity, Speed and Cost Control

Financial institutions have sometimes believed that their ability to craft efficient storage solutions like data lakes for their vast amounts of data, typically built on a Hadoop stack, would make real-time analytics available. Unfortunately, many of these systems are too slow for at-scale analytics. 

Running a query on a Hadoop data lake is just not an option for users with real-time needs! Banks experimented with different types of analytical layers between the data lakes and the users, in order to allow access to their stored data and to run analytics, but ran into new challenges: in-memory computing solutions have a lack of scalability and high hardware costs. Others tried query accelerators but were forced to analyze only prepared data (pre-aggregated or specifically indexed data), losing the granularity which is always required to understand things like daily changes. More recently, financial institutions have been contemplating cloud database management systems, but for very large datasets and calculations the speed of these services is far from what ClickHouse can achieve for their specific use cases. 

Ultimately, none of these technologies could simultaneously combine scalability, granularity, speed and cost control, forcing financial institutions into a series of compromises. With Opensee, there is no need to compromise: the platform leverages ClickHouse's capacity to handle the huge volume that data lakes require and the fast response that in-memory databases can give, without the need to pre-aggregate the data. 



!["Dashboard in Opensee for a Market Risk use case"](https://blog-images.clickhouse.com/en/2022/opensee/pivot-table.png)
_Pivot table from the Opensee UI on a liquidity use case_


## Opensee Architecture 

Opensee provides a series of APIs which allows users to fully abstract all the complexity and in particular the physical data model. These APIs are typically used for data ingestion, data query, model management, etc. Thanks to Opensee’s low-code API, users don’t need to access data through complex quasi-SQL queries, but rather through simple business queries that are optimized by Opensee to deliver performance. Opensee’s back end, which provides indirect access to Clickhouse, is written in Scala, while PostgreSQL contains all the configuration and context data that must be managed transactionally. Opensee also provides various options for front ends (dedicated Opensee web or rich user interface, Excel, others…) to interact with the data, navigate through the cube and leverage functionality like data versioning — built for the financial institution’s use. 



!["Dashboard in Opensee for a Market Risk use case"](https://blog-images.clickhouse.com/en/2022/opensee/architecture-chart.png)
_Opensee architecture chart_


## Advantages of ClickHouse

For Opensee, the most valuable feature is horizontal scalability, the capability to shard the data. Next comes the very fast dictionary lookup, rapid calculations with vectorization and the capability to manage array values. In the financial industry, where time series or historical data is everywhere, this capacity to calculate vectors and manage array values is critical.	

On top of being a solution that is extremely fast and efficient, other advantages include:


- distributed and replicated, with high availability and a performant map/reduce system
- wide range of features fit for analytics
- really good and extensive format support (csv, json, parquet, orc, protobuf ....)
- very rapid evolutions through the high contributions of a wide community to a very popular Open Source technology

On top of these native ClickHouse strengths and functionalities, Opensee has developed a lot of other functionalities dedicated to financial institutions. To name only a few, a data versioning mechanism has been created allowing business users to either correct on the fly inaccurate data or simulate new values. This ‘What If’ simulation feature can be used to add, amend or delete transactions,with full auditability and traceability, without deleting any data.

Another key feature is a Python processor which is available to define  more complex calculations. Furthermore, the abstraction model layer has been built to remove the complexity of the physical data model for the users and optimize the queries. And, last but not least, in terms of visualization, a UI dedicated to financial institutions has been developed with and for its users.		


## Dividing Hardware Costs By 10+

The cost efficiency factor is a key improvement for large financial institutions typically using in-memory computing technology. Dividing by ten (and sometimes more) the hardware cost is no small achievement! Being able to use very large datasets on standard servers on premise or in the cloud is a big achievement. With Opensee powered by ClickHouse, financial institutions are able to alleviate critical limitations of their existing solutions, avoiding legacy compromises and a lack of flexibility. Finally, these organizations are able to provide their users a turn-key solution to analyze all their data sets, which used to be siloed, in one single place, one single data model, one single infrastructure, and all of that in real time, combining very granular and very long historical ranges.

## About Opensee

Opensee empowers financial data divers to analyze deeper and faster. Headquartered in Paris, with offices in London and New York, Opensee is working with a trusted client base across global Tier 1 banks, asset managers, hedge funds and trading platforms.

For more information please visit [www.opensee.io](http://www.opensee.io) or follow them on [LinkedIn](https://www.linkedin.com/company/opensee-company) and [Twitter](https://twitter.com/opensee_io).

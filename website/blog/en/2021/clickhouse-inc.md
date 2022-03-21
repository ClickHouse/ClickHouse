---
title: 'Introducing ClickHouse, Inc.'
image: 'https://blog-images.clickhouse.com/en/2021/clickhouse-inc/home.png'
date: '2021-09-20'
author: 'Alexey Milovidov'
tags: ['company', 'incorporation', 'yandex', 'community']
---

Today I’m happy to announce **ClickHouse Inc.**, the new home of ClickHouse. The development team has moved from Yandex and joined ClickHouse Inc. to continue building the fastest (and the greatest) analytical database management system. The company has received nearly $50M in Series A funding led by Index Ventures and Benchmark with participation by Yandex N.V. and others. I created ClickHouse, Inc. with two co-founders, [Yury Izrailevsky](https://www.linkedin.com/in/yuryizrailevsky/) and [Aaron Katz](https://www.linkedin.com/in/aaron-katz-5762094/). I will continue to lead the development of ClickHouse as Chief Technology Officer (CTO), Yury will run product and engineering, and Aaron will be CEO.

## History of ClickHouse

I started developing ClickHouse more than ten years ago, and it has never been an easy ride. The idea of ClickHouse came up while I was working in Yandex as a developer of a real-time web analytics system. My team and I faced multiple data processing challenges that often required custom data structures and sophisticated algorithms, creative solutions and tradeoffs, deep understanding of domain area, hardware, and math. All these years, I often went to bed with endless thoughts about how we could solve yet another data processing challenge. I love data and processing in extreme constraints, where you have to think about bytes and nanoseconds to save petabytes and seconds. The ClickHouse team shares this passion: in my opinion, this is the main reason for ClickHouse’s success.

In 2009 we started ClickHouse as an experimental project to check the hypothesis if it's viable to generate analytical reports in real-time from non-aggregated data that is also constantly added in real-time. It took three years to prove this hypothesis, and in 2012 ClickHouse launched in production for the first time. Unlike custom data structures used before, ClickHouse was applicable more generally to work as a database management system. After several years I found that most departments in my company were using ClickHouse, and it made me wonder: maybe ClickHouse is too good to run only inside Yandex? Then we released it in [open source](https://github.com/ClickHouse/ClickHouse) in 2016.

## ClickHouse in Open Source

Making ClickHouse open source was also not an easy decision, but now I see: doing open source is hard, but it is a big win. While it takes a tremendous effort and responsibility to maintain a popular open-source product, for us, the benefits outweigh all the costs. Since we published ClickHouse, it has been deployed in production in thousands of companies across the globe for a wide range of use cases, from agriculture to self-driving cars. In 2019 we spent over a third of our time abroad organizing various ClickHouse events and speaking at external conferences, and we’re thrilled to see you all again in person once travel restrictions become less severe. The feedback and contributions from our community are priceless, and we improve the quality of implementation, the feature completeness, and making product decisions with the help of our community. One of our main focuses is to make ClickHouse welcoming for contributors by making the source code easy to read and understand, with the processes easy to follow. For me, ClickHouse is a showcase so everyone can learn the ideas in data processing.

I like to present ClickHouse as the answer to many questions in software engineering. What is better: vectorization or JIT compilation? Look at ClickHouse; it is using both. How to write the code in modern C++ in a safe way? Ok, look at the testing infrastructure in ClickHouse. How to optimize the memcpy function? What is the fastest way to transform a Unix timestamp to date in a custom timezone? I can do multiple-hour talks about these topics, and thanks to the open-source, everyone can read the code, run ClickHouse and validate our claims.

## Technical Advantage

The most notable advantage of ClickHouse is its extremely high query processing speed and data storage efficiency. What is unique about ClickHouse performance? It is difficult to answer because there is no single "[silver bullet](https://www.youtube.com/watch?v=ZOZQCQEtrz8)". The main advantage is attention to details of the most extreme production workloads. We develop ClickHouse from practical needs. It has been created to solve the needs of Metrica, one of the [most widespread](https://w3techs.com/technologies/overview/traffic_analysis) web analytics services in the world. So ClickHouse is capable of processing 100+ PBs of data with more than 100 billion records inserted every day. One of the early adopters, Cloudflare, uses ClickHouse to process a large portion of all HTTP traffic on the internet with 10+ million records per second. As ClickHouse developers, we don’t consider the task solved if there is room for performance improvement.

Query processing performance is not only about speed. It opens new possibilities. In previous generation data warehouses, you cannot run interactive queries without pre-aggregation; or you cannot insert new data in real time while serving interactive queries; or you cannot just store all your data. With ClickHouse, you can keep all records as long as you need and make interactive real-time reporting across the data. Before using ClickHouse, it was difficult to imagine that analytical data processing could be so easy and efficient: there is no need for a dozen pre-aggregating and tiering services (e.g. Druid), no need to place huge data volumes in RAM (e.g. Elastic), and no need to maintain daily/hourly/minutely tables (e.g. Hadoop, Spark).

Most other database management systems don’t even permit benchmarks (through the infamous "DeWitt clause"). But we don’t fear benchmarks; we [collect them](https://github.com/ClickHouse/ClickHouse/issues/22398). ClickHouse documentation has [links](/docs/en/getting-started/example-datasets/) to publicly available datasets up to multiple terabytes in size from various domain areas. We encourage you to try ClickHouse, do some experiments on your workload, and find ClickHouse faster than others. And if not, we encourage you to publish the benchmark, and we will make ClickHouse better!

Lastly, ClickHouse was purpose-built from the beginning to:

— Be easy to install and use. It runs everywhere, from your laptop to the cloud
— Be highly reliable and scale both vertically and horizontally
— Provide SQL with many practical and convenient extensions
— Integrate with foreign data sources and streams

## ClickHouse Spinout From Yandex

Yandex N.V. is the largest internet company in Europe and employs over 14,000 people. They develop search, advertisement, and e-commerce services, ride tech and food tech solutions, self-driving cars... and also ClickHouse with a team of 15 engineers. It is hard to believe that we have managed to build a world-class leading analytical DBMS with such a small team while leveraging the global community. While this was barely enough to keep up with the development of the open-source product, everyone understands that the potential of ClickHouse technology highly outgrows such a small team.

We decided to unite the resources: take the team of core ClickHouse developers, bring in a world-class business team led by [Aaron Katz](https://www.linkedin.com/in/aaron-katz-5762094/) and a cloud engineering team led by [Yury Izrailevsky](https://www.linkedin.com/in/yuryizrailevsky/), keep the power of open source, add the investment from the leading VC funds, and make an international company 100% focused on ClickHouse. I’m thrilled to announce ClickHouse, Inc.

## What’s Next?

Companies love ClickHouse because it gives tremendous improvements in data processing efficiency. But it is mostly about the core technology, the database server itself. We want to make ClickHouse suitable for all kinds of companies and enterprises, not just tech-savvy internet companies who are fine with managing their clusters. We want to lower the learning curve, make ClickHouse compliant with enterprise standards, make ClickHouse service to be instantly available in the cloud in a serverless way, make auto-scaling easy, and much more.

Our mission is to make ClickHouse the first choice of analytical database management systems. Whenever you think about data analytics, ClickHouse should be the obvious preferred solution. I see how many companies already benefit from ClickHouse and I'm very eager to make it even more widespread and universally accepted across the world. Now we have the best engineers and the best entrepreneurs together and we are ready for the mission.


_2021-09-20, [Alexey Milovidov](https://github.com/alexey-milovidov)_

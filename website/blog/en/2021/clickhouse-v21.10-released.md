---
title: 'ClickHouse v21.10 Released'
image: 'https://blog-images.clickhouse.com/en/2021/clickhouse-v21-10/featured.jpg'
date: '2021-10-14'
author: 'Rich Raposa, Alexey Milovidov'
tags: ['company', 'community']
---

We're excited to share with you our first release since [announcing ClickHouse, Inc](https://clickhouse.com/blog/en/2021/clickhouse-inc/). The 21.10 release includes new contributions from multiple contributors including many in our community, and we are grateful for your ongoing ideas, development, and support. Our Engineering team continues to be laser-focused on providing our community and users with the fastest and most scalable OLAP DBMS available while implementing many new features. In the 21.10 release, we have a wonderful 79 contributors with 1255 commits across 211 pull requests - what an amazing community and we cherish your contributions.

Let's highlight some of these new exciting new capabilities in 21.10:

* User-defined functions (UDFs) can now be [created as lambda expressions](https://clickhouse.com/docs/en/sql-reference/functions/#higher-order-functions). For example, `CREATE FUNCTION plus_one as (a) -> a + 1`
* Two new table engines: Executable and ExecutablePool which allow you to stream the results of a query to a custom shell script
* Instead of logging every query (which can be a lot of logs!), you can now log a random sample of your queries. The number of queries logged is determined by defining a specified probability between 0.0 (no queries logged) and 1.0 (all queries logged) using the new `log_queries_probability` setting.
* Positional arguments are now available in your GROUP BY, ORDER BY and LIMIT BY clauses. For example, `SELECT foo, bar, baz FROM my_table ORDER BY 2,3` orders the results by whatever the bar and baz columns (no need to specify column names twice!)

We're also thrilled to announce some new free training available to you in our Learn ClickHouse portal: [https://clickhouse.com/learn/lessons/whatsnew-clickhouse-21.10/](https://clickhouse.com/learn/lessons/whatsnew-clickhouse-21.10/)

We're always listening for new ideas, and we're happy to welcome new contributors to the ClickHouse project. Whether for submitting code or improving our documentation and examples, please get involved by sending us a pull request or submitting an issue. Our beginner developers contribution guide will help you get started: [https://clickhouse.com/docs/en/development/developer-instruction/](https://clickhouse.com/docs/en/development/developer-instruction/)


## ClickHouse Release Notes 

Release 21.10

Release Date: 2021-10-17

Release Notes: [21.10](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md)

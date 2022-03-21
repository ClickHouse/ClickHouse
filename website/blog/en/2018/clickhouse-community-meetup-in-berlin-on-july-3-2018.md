---
title: 'ClickHouse Community Meetup in Berlin on July 3, 2018'
image: 'https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-berlin-on-july-3-2018/main.jpg'
date: '2018-07-05'
tags: ['meetup', 'Berlin', 'Germany', 'events']
---

Just a few months ago Brenno Oliveira from Delivery Hero has dropped us an email saying that they want to host a meetup of ClickHouse community in their HQ and together we made it happen. Actually, renting a suitable room is one of the main limiting factors on how often ClickHouse meetups can happen worldwide and it was very kind of Delivery Hero to provide it for free. Bringing interesting speakers was the easy part as there are more and more companies adopting ClickHouse and willing to share their stories. Being an open-source product has its advantages after all. About 50 people have shown up from 75 sign-ups, which is way above the typical rate.

To get started Alexander Zaitsev from Altinity gave an overview of ClickHouse for those who are not that familiar with the technology yet. He was using use cases from his personal experience and their clients as examples. Here are [the slides](https://presentations.clickhouse.com/meetup16/introduction.pdf), unfortunately, no video this time.

Gleb Kanterov talking about the usage of ClickHouse for experimentation metrics at Spotify:
![Gleb Kanterov Spotify](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-berlin-on-july-3-2018/1.jpg)

![Gleb Kanterov Spotify](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-berlin-on-july-3-2018/2.jpg)

Spotify relies heavily on what Google Cloud Platform provides, but nevertheless found a spot in their infrastructure where only ClickHouse appeared to satisfy the requirements. Gleb Kanterov has demonstrated their approach to conducting experiments and measuring if they are worth being promoted to production solutions. Using ClickHouse has allowed them to build a framework scalable to thousands of metrics, which in the end makes them move even faster and break fewer things. Checking out [full slides](https://presentations.clickhouse.com/meetup16/spotify.pdf) is highly recommended and here are a few quotes:

-   **Requirements**
    - Serve 100-s of QPS with sub-second latency
    - We know in advance what are queries and data
    - Maintain 10x metrics with the same cost
    - Thousands of metrics
    - Billions of rows per day in each of 100-s of tables
    - Ready to be used out of the box
    - Leverage existing infrastructure as much as feasible
    - Hide unnecessary complexity from internal users
-   **Why ClickHouse?**
    - Build proof of concept using various OLAP storages (ClickHouse, Druid, Pinot,...)
    - ClickHouse has the most simple architecture
    - Powerful SQL dialect close to Standard SQL
    - A comprehensive set of built-in functions and aggregators
    - Was ready to be used out of the box
    - Superset integration is great
    - Easy to query using clickhouse-jdbc and jooq

The last talk by Alexey Milovidov was pretty technical and mostly intended for a deeper understanding of what's going on inside ClickHouse, see [the slides](https://presentations.clickhouse.com/meetup16/internals.pdf). There were many experienced users in the audience who didn't mind staying late to hear that and ask very relevant questions. Actually, we had to leave the building way before people were out of topics to discuss.

If your company regularly hosts technical meetups and you are looking for interesting topics to talk about, ClickHouse might be in pretty high demand. Feel free to write ClickHouse team via [this form](http://clickhouse.com/#meet) if you are interested to host a similar event in your city and we'll find a way to cooperate and bring in other ClickHouse community members.

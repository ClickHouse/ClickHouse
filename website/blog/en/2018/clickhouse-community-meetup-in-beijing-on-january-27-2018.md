---
title: 'ClickHouse Community Meetup in Beijing on January 27, 2018'
image: 'https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/main.jpg'
date: '2018-02-08'
tags: ['meetup', 'Beijing', 'China', 'events', 'Asia']
---

Last year there has been an OLAP algorithm contest in China organized by Analysys. The team who have shown the top results and won the competition has been using ClickHouse as the core of their solution. Other teams were mostly using different technologies and didn't really know much about ClickHouse at a time. When the final results were published, many people in China who participated in or were aware of this competition became really eager to learn more about ClickHouse. This spike of interest about ClickHouse in China has eventually lead to the first Chinese ClickHouse Community Meetup that has taken place in Beijing.

Welcome word by William Kwok, CTO of Analysys, who personally played a huge role in making this event possible:
![William Kwok, CTO of Analysys](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/1.jpg)

It was probably the most intense ClickHouse Meetup compared to all previous ones worldwide. The main part of the event took over 6 hours non-stop and there were also either pre-meetup and after-party on the same day. Well over 150 people have shown up on Saturday to participate.

Audience listening for ClickHouse introduction by Alexey Milovidov:
![ClickHouse introduction by Alexey Milovidov](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/2.jpg)

Alexey Milovidov has started the main meetup session with an introductory talk about ClickHouse, it's usage inside Yandex and history that lead to becoming an open-source analytical DBMS ([slides](https://presentations.clickhouse.com/meetup12/introduction/)).

Alexander Zaitsev's practical talk about migrating to ClickHouse:
![Alexander Zaitsev](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/3.jpg)

Alexander Zaitsev has shared his vast experience in migrating to ClickHouse. LifeStreet, advertisement company where he works, was one of the first companies outside of Yandex which switched to ClickHouse from other analytical DBMS in production. Later on, Alexander also co-founded Altinity, a company that specializes in helping others to migrate to ClickHouse and then effectively use it to achieve their business goals. The talk has covered many specific topics that are important for those who are in the middle of such migration or just considering it ([Slides](https://presentations.clickhouse.com/meetup12/migration.pptx)).

Alexey Zatelepin explaining how ClickHouse sparse index works and other implementation details:
![Alexey Zatelepin](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/4.jpg)

Alexey Zatelepin's technical talk was focused on providing engineers some insights on why ClickHouse is that fast in OLAP workloads and how to leverage its design and core features as a primary index, replication, and distributed tables to achieve great performance and reliability ([slides](https://presentations.clickhouse.com/meetup12/internals.pdf)).

Jack Gao gives an extensive overview of ClickHouse and it's use cases in Chinese:
![Jack Gao](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/5.jpg)

As we have learned during meet up and the rest of our business trip, actually there are many companies in China that are already using or seriously evaluating ClickHouse to use either part of their products or for internal analytics. Three of them are doing this long and extensively enough to give a full talk about their progress and experience.

In China, in general, and especially in Beijing the knowledge of English is not really common. Chinese people working in the IT industry have to know English well enough to read documentation, but it does not really imply that they can talk or understand verbal English well. So the talks by representatives of local companies were in Chinese.

Jack Gao, ex-DBA and now an analyst at Sina (major social network) have dedicated a significant part of his talk to go over fundamental topics essential to most ClickHouse users. It partially overlapped with previous talks, but this time in Chinese. Also, he covered not only use case of ClickHouse in Sina but also other publicly known cases by other companies. Considering the reaction of the audience, it has been the most useful talk of the whole meetup, because of the widely useful content, lack of language barrier, and excellent execution of presentation. We even had to sacrifice initially scheduled a short break to give Jack some additional time ([slides](https://presentations.clickhouse.com/meetup12/power_your_data.pdf)).

Yang Xujun from Dataliance / UltraPower, which provides outsourced data analysis platform to telecom companies in China, have demonstrated why they decided to move away from reports prepared offline in Apache Hadoop / Spark and exported to MySQL towards ClickHouse. In short: Hadoop is too slow and cumbersome ([slides](https://presentations.clickhouse.com/meetup12/telecom.pdf)).

It might sound obvious, but the huge Chinese population generates insane amounts of data to store and process. So IT companies operating mostly on the local Chinese market are often handling amounts of information comparable to even the largest global companies.

Kent Wang from Splunk Shanghai R&D center has demonstrated the current state of ClickHouse integration into Splunk ecosystem. Basically, they have plugged ClickHouse into their system via JDBC driver to allow data from ClickHouse to be easily accessed in Splunk UI and dashboards. Last spring ClickHouse team actually had a friendly visit to Splunk office in San Francisco to discuss potential points of interaction and exchange experience, so it was great to hear that there's some real progress in that direction ([slides](https://presentations.clickhouse.com/meetup12/splunk.pdf)).

The last talk was for the most tenacious ClickHouse users. Alexey Milovidov has announced some recently released features and improvements and shared what's coming next either in the short and long term [slides](https://presentations.clickhouse.com/meetup12/news_and_plans/).

Here is an over 5 hours long video recording of main meetup session:

![iframe](https://www.youtube.com/embed/UXw8izZGPGk)

If you are from China or at least can read Chinese, you might consider joining the **[Chinese ClickHouse User Group](http://www.clickhouse.com.cn/)**.

{## Likely outdated in favor of YouTube

There is an over 5 hours long video recording of main meetup session, but it'll take a bit of effort to get access to it (especially if you are not from China): http://m.zm518.cn/zhangmen/livenumber/share/entry/?liveId=1460023&sharerId=6fd3bac16125e71d69-899&circleId=b0b78915b2edbfe6c-78f7&followerId=&timestamp=1517022274560
You'll need to install WeChat (probably one of the most popular messengers in the world, everyone in China has it) on your smartphone: Android or iOS. https://play.google.com/store/apps/details?id=com.tencent.mm https://itunes.apple.com/ru/app/wechat/id414478124?mt=8
On the first launch, WeChat will ask to confirm your phone number via SMS, read some digits via a microphone and accept the user agreement. Go through this.
On your computer, click the red button in the middle of the video behind the link above. It'll show a QR code. Now in WeChat in the top-right corner, there's the “+” button which opens a menu that has a “Scan QR code” item. Use it to scan QR code from your computer screen, then press the “Sign in” button on the smartphone. Now the video on the computer automatically becomes playable.
If you are from China or at least can read Chinese, you might consider joining the Chinese ClickHouse User Group.

ClickHouse Community Meetup afterparty.
##}

Pre-meetup meeting of speakers and most active ClickHouse users in China:
![Pre-meetup meeting](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/6.jpg)

ClickHouse Community Meetup afterparty:
![ClickHouse Community Meetup afterparty](https://blog-images.clickhouse.com/en/2018/clickhouse-community-meetup-in-beijing-on-january-27-2018/7.jpg)

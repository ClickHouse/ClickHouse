---
title: 'Package Repository Behind CDN'
image: 'https://blog-images.clickhouse.com/en/2020/package-repository-behind-cdn/main.jpg'
date: '2020-07-02'
tags: ['article', 'CDN', 'Cloudflare', 'repository', 'deb', 'rpm', 'tgz']
author: 'Ivan Blinkov'
---

On initial open-source launch, ClickHouse packages were published at an independent repository implemented on Yandex infrastructure. We'd love to use the default repositories of Linux distributions, but, unfortunately, they have their own strict rules on third-party library usage and software compilation options. These rules happen to contradict with how ClickHouse is produced. In 2018 ClickHouse was added to [official Debian repository](https://packages.debian.org/sid/clickhouse-server) as an experiment, but it didn't get much traction. Adaptation to those rules ended up producing more like a demo version of ClickHouse with crippled performance and limited features.

!!! info "TL;DR"
    If you have configured your system to use <http://repo.yandex.ru/clickhouse/> for fetching ClickHouse packages, replace it with <https://repo.clickhouse.com/>.

Distributing packages via our own repository was working totally fine until ClickHouse has started getting traction in countries far from Moscow, most notably the USA and China. Downloading large files of packages from remote location was especially painful for Chinese ClickHouse users, likely due to how China is connected to the rest of the world via its famous firewall. But at least it worked (with high latencies and low throughput), while in some smaller countries there was completely no access to this repository and people living there had to host their own mirrors on neutral ground as a workaround.

Earlier this year we made the ClickHouse official website to be served via global CDN by [Cloudflare](https://www.cloudflare.com) on a `clickhouse.com` domain. To solve the download issues discussed above, we have also configured a new location for ClickHouse packages that are also served by Cloudflare at [repo.clickhouse.com](https://repo.clickhouse.com). It used to have some quirks, but now it seems to be working fine while improving throughput and latencies in remote geographical locations by over an order of magnitude.

## Switching To Repository Behind CDN

This transition has some more benefits besides improving the package fetching, but let's get back to them in a minute. One of the key reasons for this post is that we can't actually influence the repository configuration of ClickHouse users. We have updated all instructions, but for people who have followed these instructions earlier, **action is required** to use the new location behind CDN. Basically, you need to replace `http://repo.yandex.ru/clickhouse/` with `https://repo.clickhouse.com/` in your package manager configuration.

One-liner for Ubuntu or Debian:
```bash
sudo apt-get install apt-transport-https ca-certificates && sudo perl -pi -e 's|http://repo.yandex.ru/clickhouse/|https://repo.clickhouse.com/|g' /etc/apt/sources.list.d/clickhouse.list && sudo apt-get update
```

One-liner for RedHat or CentOS:
```bash
sudo perl -pi -e 's|http://repo.yandex.ru/clickhouse/|https://repo.clickhouse.com/|g' /etc/yum.repos.d/clickhouse*
```

As you might have noticed, the domain name is not the only thing that has changed: the new URL uses `https://` protocol. Usually, it's considered less important for package repositories compared to normal websites because most package managers check [GPG signatures](https://en.wikipedia.org/wiki/GNU_Privacy_Guard) for what they download anyway. However it still has some benefits: for example, it's not so uncommon for people to download packages via browser, `curl` or `wget`, and install them manually (while for [tgz](https://repo.clickhouse.com/tgz/) builds it's the only option). Fewer opportunities for sniffing traffic can't hurt either. The downside is that `apt` in some Debian flavors has no HTTPS support by default and needs a couple more packages to be installed (`apt-transport-https` and `ca-certificates`).

## Investigating Repository Usage

The next important thing we obtained by using Cloudflare for our package repository is observability. Of course the same could have been implemented from scratch, but it'd require extra resources to develop and maintain, while Cloudflare provides quite rich tools for analyzing what's going on in your domains.

!!! info "Did you know?"
    It's kind of off-topic, but those Cloudflare features are internally based on ClickHouse, see their [HTTP analytics](https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/) and [DNS analytics](https://blog.cloudflare.com/how-cloudflare-analyzes-1m-dns-queries-per-second/) blog posts.

Just a few weeks ago they have also added [cache analytics](https://blog.cloudflare.com/introducing-cache-analytics/) feature, which allowed to drill into how effectively the content is cached on CDN edges and improve the CDN configuration accordingly. For example, it allowed debugging some inconsistencies in cached repository metadata.

## Digging Deeper

All those built-in observability tools provided by Cloudflare share one weak point: they are purely technical and generic, without any domain-specific awareness. They excel at debugging low-level issues, but it's hard to get a higher-level picture based on them. With our package repository scenario, we're not so interested in frequent metadata update requests, but we'd like to see reports on package downloads by version, kind, and so on. We definitely didn't want to operate a separate infrastructure to get those reports, but given there was no out-of-the-box solution, we had to be creative and managed to find a cool middle ground.

Ever heard the [“serverless computing”](https://en.wikipedia.org/wiki/Serverless_computing) hype recently? That was the basic idea: let's assemble a bunch of serverless or managed services to get what we want, without any dedicated servers. The plan was pretty straightforward:

1. Dump details about package downloads to a ClickHouse database.
2. Connect some [BI](https://en.wikipedia.org/wiki/Business_intelligence) tool to that ClickHouse database and configure required charts/dashboards.

Implementing it required a little bit of research, but the overall solution appeared to be quite elegant:

1. For a ClickHouse database, it was a no-brainer to use [Yandex Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse). With a few clicks in the admin interface, we got a running ClickHouse cluster with properly configured high-availability and automated backups. Ad-hoc SQL queries could be run from that same admin interface.
2. Cloudflare allows customers to run custom code on CDN edge servers in a serverless fashion (so-called [workers](https://workers.cloudflare.com)). Those workers are executed in a tight sandbox which doesn't allow for anything complicated, but this feature fits perfectly to gather some data about download events and send it somewhere else. This is normally a paid feature, but special thanks to Connor Peshek from Cloudflare who arranged a lot of extra features for free on `clickhouse.com` when we have applied to their [open-source support program](https://developers.cloudflare.com/sponsorships/).
3. To avoid publicly exposing yet another ClickHouse instance (like we did with **[playground](/docs/en/getting-started/playground/)** regardless of being a 100% anti-pattern), the download event data is sent to [Yandex Cloud Functions](https://cloud.yandex.com/services/functions). It's a generic serverless computing framework at Yandex Cloud, which also allows running custom code without maintaining any servers, but with less strict sandbox limitations and direct access to other cloud services like Managed ClickHouse that was needed for this task.
4. It didn't require much effort to choose a visualization tool either, as [DataLens BI](https://cloud.yandex.com/docs/datalens/) is tightly integrated with ClickHouse, capable to build what's required right from the UI, and satisfies the “no servers” requirement because it's a SaaS solution. Public access option for charts and dashboards have also appeared to be handy.

There's not so much data collected yet, but here's a live example of how the resulting data visualization looks like. For example, here we can see that LTS releases of ClickHouse are not so popular yet *(yes, we have [LTS releases](https://clickhouse.com/docs/en/faq/operations/production/)!)*:
![iframe](https://datalens.yandex/qk01mwxkgiysm?_embedded=1)

While here we confirmed that `rpm` is at least as popular as `deb`:
![iframe](https://datalens.yandex/lfvldsf92i2uh?_embedded=1)

Or you can take a look at all key charts for `repo.clickhouse.com` together on a handy **[dashboard](https://datalens.yandex/pjzq4rot3t2ql)** with a filtering possibility.

## Lessons Learned

* CDN is a must-have if you want people from all over the world to download some artifacts that you produce. Beware the huge pay-for-traffic bills from most CDN providers though.
* Generic technical system metrics and drill-downs are a good starting point, but not always enough.
* Serverless is not a myth. Nowadays it is indeed possible to build useful products by just integrating various infrastructure services together, without any dedicated servers to take care of.

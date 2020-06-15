## Introduction

First of all, **relevant guest posts are welcome**! Especially with success stories or demonstration of ClickHouse ecosystem projects.

The ClickHouse blog is published alongside documentation and the rest of official website. So the posts reside in this same repository in [Markdown](https://github.com/ClickHouse/ClickHouse/tree/master/docs#markdown-cheatsheet) format.

## How To Add a New Post?

Basically you need to create a new Markdown file at the following location inside repository `/blog/<lang>/<year>/<post-slug>.md` and then [open a pull-request](https://github.com/ClickHouse/ClickHouse/compare) with it.

Each post needs to have a `yaml` meta-header with the following fields:

-   Required:
    -   `title`, main name of the article. In Title Case for English.
    -   `date`, publication date in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format, like `YYYY-MM-DD` (can be in future to postpone publication).
-   Optional:
    -   `image`, URL to main post image.
    -   `tags`, list of post tags.
 
Then after header goes post content in a normal markdown (with some optional extensions).
 
### Example
 ```markdown
---
title: 'ClickHouse Meetup in Beijing on June 8, 2019'
image: 'https://avatars.mds.yandex.net/get-yablogs/41243/file_1560510043188/orig'
date: '2019-06-13'
tags: ['meetup','Beijing','China','events']
---

24th ClickHouse Meetup globally and 3rd one in China took place in Beijing on Dragon Boat Festival weekend, which appeared to...
```

## How To Add a New Blog Language?

It's not so straightforward and won't be documented for now, but you can just create the language directory with the first post as described above and we'll configure the website infrastructure to include it during/after merging the pull-request.

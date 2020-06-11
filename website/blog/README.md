## Overview

The ClickHouse blog is published alongside documentation and the rest of official website.

## How To Add New Post?

Basically you need to create a new Markdown file at the following location inside repository: `/blog/<lang>/<year>/<post-slug>.md`

Post needs to have a `yaml` meta-header with the following fields:

-   Required:
    -   `title`, main name of the article. In Title Case for English.
    -   `date`, in ISO8601 format, like `YYYY-MM-DD`.
-   Optional:
    -   `image`, URL to main post image.
    -   `tags`, list of post tags.
 
 
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

## Introduction

First of all, **relevant guest posts are welcome**! Especially with success stories or demonstration of ClickHouse ecosystem projects.

The ClickHouse blog is published alongside documentation and the rest of official website. So the posts reside in this same repository in [Markdown](https://github.com/ClickHouse/ClickHouse/tree/master/docs#markdown-cheatsheet) format.

## How To Add a New Post?

Basically you need to create a new Markdown file at the following location inside repository `/website/blog/<lang>/<year>/<post-slug>.md` and then [open a pull-request](https://github.com/ClickHouse/ClickHouse/compare) with it. You can do it even right from the GitHub web interface using the "Create new file" button.

Each post needs to have a `yaml` meta-header with the following fields:

-   Required:
    -   `title`, main name of the article. In Title Case for English.
    -   `date`, publication date in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format, like `YYYY-MM-DD` (can be in future to postpone publication).
-   Optional:
    -   `image`, URL to main post image.
    -   `tags`, list of post tags.
 
Then after header goes post content in a normal markdown (with some optional extensions).
 
The recommended place to store images is this GitHub repo: <https://github.com/ClickHouse/clickhouse-blog-images>. It's folder structure matches this folder with blog posts:

-   `<lang>/<year>/<post-slug>/main.jpg` for main post image (linked in `image` header field).
-   `<lang>/<year>/<post-slug>/whatever.jpg` for other images (`png` or `gif` are acceptable as well, if necessary).

### Example
 ```markdown
---
title: 'ClickHouse Meetup in Beijing on June 8, 2019'
image: 'https://blog-images.clickhouse.tech/en/2019/clickhouse-meetup-in-beijing-on-june-8-2019/main.jpg'
date: '2019-06-13'
tags: ['meetup','Beijing','China','events']
---

24th ClickHouse Meetup globally and 3rd one in China took place in Beijing on Dragon Boat Festival weekend, which appeared to...

![ClickHouse branded Beijing duck](https://blog-images.clickhouse.tech/en/2019/clickhouse-meetup-in-beijing-on-june-8-2019/9.jpg)
```

## How To Preview My Post?

Use [deploy-to-test.sh](https://github.com/ClickHouse/ClickHouse/blob/master/docs/tools/deploy-to-test.sh) script. Note that on the first use you'll need to follow the steps in its first comment, and [install prerequisites for build.py](https://github.com/ClickHouse/ClickHouse/blob/master/docs/tools/README.md#use-buildpy-use-build-py). Alternatively, you can use `--livereload=N` argument of [build.py](https://github.com/ClickHouse/ClickHouse/blob/master/docs/tools/build.py).

## How To Add a New Blog Language?

If you want to write a guest post, you are welcome to use your native language or make multiple posts in multiple languages
 
Unlike documentation, blog languages are independent, i.e. they have partially overlapping sets of posts and it's ok. Most posts are written only in one language because they are not relevant to audiences of other languages.

At the moment it's not so straightforward to set up a new language for blog and it won't be documented for now, but you can just create a language directory with the first post as described above and we'll configure the website infrastructure to include it during/after merging the pull-request.

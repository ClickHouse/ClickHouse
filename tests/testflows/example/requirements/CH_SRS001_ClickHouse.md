# CH-SRS001 ClickHouse Software Requirements Specification Template

**Author:** [name of the author]

**Date:** [date]

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
  * 2.1 [Table of Contents](#table-of-contents)
  * 2.2 [Generating HTML version](#generating-html-version)
  * 2.3 [Generating Python Requirements](#generating-python-requirements)
* 3 [Terminology](#terminology)
  * 3.1 [SRS](#srs)
  * 3.2 [Some term that you will use](#some-term-that-you-will-use)
* 4 [Requirements](#requirements)
  * 4.1 [RQ.CH-SRS001.Example](#rqch-srs001example)
  * 4.2 [RQ.CH-SRS001.Example.Subgroup](#rqch-srs001examplesubgroup)
  * 4.3 [RQ.CH-SRS001.Example.Select.1](#rqch-srs001exampleselect1)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software.

## Introduction

This section provides an introduction to the project or the feature.
All [SRS] documents must be uniquely identified by a number. In this
case this document is identified by the number

    CH-SRS001

The document number must always be used as a prefix to the document title. For example,

    CH-SRSxxx Name of the document

All the requirements must be specified in the [Requirements](#requirements) section.

### Table of Contents

Note that currently the table of contents is generated manually using 

```bash
cat CH_SRS001_ClickHouse.md | tfs document toc
```

command and needs to be updated any time requirement name is changed
or a new requirement is added.

### Generating HTML version

You can easily generate a pretty HTML version of this document using the command.

```bash
cat CH_SRS001_ClickHouse.md | tfs document convert > CH_SRS001_ClickHouse.html
```

### Generating Python Requirements

You can convert this [SRS] into the `requirements.py` by using the command.

```bash
cat CH_SRS001_ClickHouse.md | tfs requirements generate > requirements.py
```

## Terminology

You can define terminolgy using the examples below and you can make them
linkable as [SRS] by defining the links in the [References](#References) section.

### SRS

Software Requirements Specification

### Some term that you will use

Some description of the term that you would like to use.

## Requirements

This section includes all the requirements. This section can be structured in any way one sees fit. 

Each requirement is defined by the section that starts with
the following prefix:

    RQ.[document id].[requirement name]

then immediately followed by a one-line block that contains the 
the `version` of the requirement.

### RQ.CH-SRS001.Example
version: 1.0

This is a long description of the requirement that can include any
relevant information. 

The one-line block that follows the requirement defines the `version` 
of the requirement. The version is controlled manually and is used
to indicate material changes to the requirement that would 
require tests that cover this requirement to be updated.

It is a good practice to use requirement names that are broken
up into groups. It is not recommended to use only numbers
because if the requirement must be moved the numbering will not match.
Therefore, the requirement name should start with the group
name which is then followed by a number if any. For example,

    RQ.SRS001.Group.Subgroup.1

To keep names short, try to use abbreviations for the requirement's group name.

### RQ.CH-SRS001.Example.Subgroup
version: 1.0

This an example of a sub-requirement of the [RQ.CH-SRS001.Example](#rqch-srs001example).

### RQ.CH-SRS001.Example.Select.1
version: 1.0

[ClickHouse] SHALL return `1` when user executes query

```sql
SELECT 1
```

## References

* **ClickHouse:** https://clickhouse.tech

[SRS]: #SRS
[Some term that you will use]: #Sometermthatyouwilluse
[ClickHouse]: https://clickhouse.tech
[Git]: https://git-scm.com/

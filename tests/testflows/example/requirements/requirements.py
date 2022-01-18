# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.201216.1172002.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_001_Example = Requirement(
    name='RQ.SRS-001.Example',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'This is a long description of the requirement that can include any\n'
        'relevant information. \n'
        '\n'
        'The one-line block that follows the requirement defines the `version` \n'
        'of the requirement. The version is controlled manually and is used\n'
        'to indicate material changes to the requirement that would \n'
        'require tests that cover this requirement to be updated.\n'
        '\n'
        'It is a good practice to use requirement names that are broken\n'
        'up into groups. It is not recommended to use only numbers\n'
        'because if the requirement must be moved the numbering will not match.\n'
        'Therefore, the requirement name should start with the group\n'
        'name which is then followed by a number if any. For example,\n'
        '\n'
        '    RQ.SRS-001.Group.Subgroup.1\n'
        '\n'
        "To keep names short, try to use abbreviations for the requirement's group name.\n"
        '\n'
        ),
    link=None,
    level=2,
    num='4.1')

RQ_SRS_001_Example_Subgroup = Requirement(
    name='RQ.SRS-001.Example.Subgroup',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        'This an example of a sub-requirement of the [RQ.SRS-001.Example](#rqsrs-001example).\n'
        '\n'
        ),
    link=None,
    level=2,
    num='4.2')

RQ_SRS_001_Example_Select_1 = Requirement(
    name='RQ.SRS-001.Example.Select.1',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return `1` when user executes query\n'
        '\n'
        '```sql\n'
        'SELECT 1\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=2,
    num='4.3')

SRS_001_ClickHouse_Software_Requirements_Specification_Template = Specification(
    name='SRS-001 ClickHouse Software Requirements Specification Template', 
    description=None,
    author='[name of the author]',
    date='[date]', 
    status=None, 
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Table of Contents', level=2, num='2.1'),
        Heading(name='Generating HTML version', level=2, num='2.2'),
        Heading(name='Generating Python Requirements', level=2, num='2.3'),
        Heading(name='Terminology', level=1, num='3'),
        Heading(name='SRS', level=2, num='3.1'),
        Heading(name='Some term that you will use', level=2, num='3.2'),
        Heading(name='Requirements', level=1, num='4'),
        Heading(name='RQ.SRS-001.Example', level=2, num='4.1'),
        Heading(name='RQ.SRS-001.Example.Subgroup', level=2, num='4.2'),
        Heading(name='RQ.SRS-001.Example.Select.1', level=2, num='4.3'),
        Heading(name='References', level=1, num='5'),
        ),
    requirements=(
        RQ_SRS_001_Example,
        RQ_SRS_001_Example_Subgroup,
        RQ_SRS_001_Example_Select_1,
        ),
    content='''
# SRS-001 ClickHouse Software Requirements Specification Template

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
  * 4.1 [RQ.SRS-001.Example](#rqsrs-001example)
  * 4.2 [RQ.SRS-001.Example.Subgroup](#rqsrs-001examplesubgroup)
  * 4.3 [RQ.SRS-001.Example.Select.1](#rqsrs-001exampleselect1)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software.

## Introduction

This section provides an introduction to the project or the feature.
All [SRS] documents must be uniquely identified by a number. In this
case this document is identified by the number

    SRS-001

The document number must always be used as a prefix to the document title. For example,

    SRS-xxx Name of the document

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

### RQ.SRS-001.Example
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

    RQ.SRS-001.Group.Subgroup.1

To keep names short, try to use abbreviations for the requirement's group name.

### RQ.SRS-001.Example.Subgroup
version: 1.0

This an example of a sub-requirement of the [RQ.SRS-001.Example](#rqsrs-001example).

### RQ.SRS-001.Example.Select.1
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
''')

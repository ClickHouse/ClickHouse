#!/usr/bin/env python3
import os
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from rbac.requirements import SRS_006_ClickHouse_Role_Based_Access_Control
from helpers.common import check_clickhouse_version

issue_14091 = "https://github.com/ClickHouse/ClickHouse/issues/14091"
issue_14149 = "https://github.com/ClickHouse/ClickHouse/issues/14149"
issue_14224 = "https://github.com/ClickHouse/ClickHouse/issues/14224"
issue_14418 = "https://github.com/ClickHouse/ClickHouse/issues/14418"
issue_14451 = "https://github.com/ClickHouse/ClickHouse/issues/14451"
issue_14566 = "https://github.com/ClickHouse/ClickHouse/issues/14566"
issue_14674 = "https://github.com/ClickHouse/ClickHouse/issues/14674"
issue_14810 = "https://github.com/ClickHouse/ClickHouse/issues/14810"
issue_15165 = "https://github.com/ClickHouse/ClickHouse/issues/15165"
issue_15980 = "https://github.com/ClickHouse/ClickHouse/issues/15980"
issue_16403 = "https://github.com/ClickHouse/ClickHouse/issues/16403"
issue_17146 = "https://github.com/ClickHouse/ClickHouse/issues/17146"
issue_17147 = "https://github.com/ClickHouse/ClickHouse/issues/17147"
issue_17653 = "https://github.com/ClickHouse/ClickHouse/issues/17653"
issue_17655 = "https://github.com/ClickHouse/ClickHouse/issues/17655"
issue_17766 = "https://github.com/ClickHouse/ClickHouse/issues/17766"
issue_18110 = "https://github.com/ClickHouse/ClickHouse/issues/18110"
issue_21083 = "https://github.com/ClickHouse/ClickHouse/issues/21083"
issue_21084 = "https://github.com/ClickHouse/ClickHouse/issues/21084"
issue_25413 = "https://github.com/ClickHouse/ClickHouse/issues/25413"
issue_26746 = "https://github.com/ClickHouse/ClickHouse/issues/26746"

xfails = {
    "syntax/show create quota/I show create quota current": [
        (Fail, "https://github.com/ClickHouse/ClickHouse/issues/12495")
    ],
    "views/:/create with subquery privilege granted directly or via role/:": [
        (Fail, issue_14091)
    ],
    "views/:/create with join query privilege granted directly or via role/:": [
        (Fail, issue_14091)
    ],
    "views/:/create with union query privilege granted directly or via role/:": [
        (Fail, issue_14091)
    ],
    "views/:/create with join union subquery privilege granted directly or via role/:": [
        (Fail, issue_14091)
    ],
    "views/:/create with nested views privilege granted directly or via role/:": [
        (Fail, issue_14091)
    ],
    "views/view/select with join query privilege granted directly or via role/:": [
        (Fail, issue_14149)
    ],
    "views/view/select with join union subquery privilege granted directly or via role/:": [
        (Fail, issue_14149)
    ],
    "views/view/select with nested views privilege granted directly or via role/:": [
        (Fail, issue_14149)
    ],
    "views/live view/refresh with privilege granted directly or via role/:": [
        (Fail, issue_14224)
    ],
    "views/live view/refresh with privilege revoked directly or from role/:": [
        (Fail, issue_14224)
    ],
    "views/live view/select:": [(Fail, issue_14418)],
    "views/live view/select:/:": [(Fail, issue_14418)],
    "views/materialized view/select with:": [(Fail, issue_14451)],
    "views/materialized view/select with:/:": [(Fail, issue_14451)],
    "views/materialized view/modify query:": [(Fail, issue_14674)],
    "views/materialized view/modify query:/:": [(Fail, issue_14674)],
    "views/materialized view/insert on source table privilege granted directly or via role/:": [
        (Fail, issue_14810)
    ],
    "privileges/alter ttl/table_type=:/user with some privileges": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/role with some privileges": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/user with privileges on cluster": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/user with privileges from user with grant option": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/user with privileges from role with grant option": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/role with privileges from user with grant option": [
        (Fail, issue_14566)
    ],
    "privileges/alter ttl/table_type=:/role with privileges from role with grant option": [
        (Fail, issue_14566)
    ],
    "privileges/distributed table/:/special cases/insert with table on source table of materialized view:": [
        (Fail, issue_14810)
    ],
    "privileges/distributed table/cluster tests/cluster='sharded*": [
        (Fail, issue_15165)
    ],
    "privileges/distributed table/cluster tests/cluster=:/special cases/insert with table on source table of materialized view privilege granted directly or via role/:": [
        (Fail, issue_14810)
    ],
    "views/materialized view/select from implicit target table privilege granted directly or via role/select from implicit target table, privilege granted directly": [
        (Fail, ".inner table is not created as expected")
    ],
    "views/materialized view/insert on target table privilege granted directly or via role/insert on target table, privilege granted through a role": [
        (Fail, ".inner table is not created as expected")
    ],
    "views/materialized view/select from implicit target table privilege granted directly or via role/select from implicit target table, privilege granted through a role": [
        (Fail, ".inner table is not created as expected")
    ],
    "views/materialized view/insert on target table privilege granted directly or via role/insert on target table, privilege granted directly": [
        (Fail, ".inner table is not created as expected")
    ],
    "views/materialized view/select from source table privilege granted directly or via role/select from implicit target table, privilege granted directly": [
        (Fail, ".inner table is not created as expected")
    ],
    "views/materialized view/select from source table privilege granted directly or via role/select from implicit target table, privilege granted through a role": [
        (Fail, ".inner table is not created as expected")
    ],
    "privileges/alter move/:/:/:/:/move partition to implicit target table of a materialized view": [
        (Fail, ".inner table is not created as expected")
    ],
    "privileges/alter move/:/:/:/:/user without ALTER MOVE PARTITION privilege/": [
        (Fail, issue_16403)
    ],
    "privileges/alter move/:/:/:/:/user with revoked ALTER MOVE PARTITION privilege/": [
        (Fail, issue_16403)
    ],
    "privileges/create table/create with join query privilege granted directly or via role/:": [
        (Fail, issue_17653)
    ],
    "privileges/create table/create with join union subquery privilege granted directly or via role/:": [
        (Fail, issue_17653)
    ],
    "privileges/create table/create with nested tables privilege granted directly or via role/:": [
        (Fail, issue_17653)
    ],
    "privileges/kill mutation/no privilege/kill mutation on cluster": [
        (Fail, issue_17146)
    ],
    "privileges/kill query/privilege granted directly or via role/:/": [
        (Fail, issue_17147)
    ],
    "privileges/show dictionaries/:/check privilege/:/exists/EXISTS with privilege": [
        (Fail, issue_17655)
    ],
    "privileges/public tables/sensitive tables": [(Fail, issue_18110)],
    "privileges/: row policy/nested live:": [(Fail, issue_21083)],
    "privileges/: row policy/nested mat:": [(Fail, issue_21084)],
    "privileges/show dictionaries/:/check privilege/check privilege=SHOW DICTIONARIES/show dict/SHOW DICTIONARIES with privilege": [
        (Fail, "new bug")
    ],
    "privileges/show dictionaries/:/check privilege/check privilege=CREATE DICTIONARY/show dict/SHOW DICTIONARIES with privilege": [
        (Fail, "new bug")
    ],
    "privileges/show dictionaries/:/check privilege/check privilege=DROP DICTIONARY/show dict/SHOW DICTIONARIES with privilege": [
        (Fail, "new bug")
    ],
    "privileges/kill mutation/:/:/KILL ALTER : without privilege": [
        (Fail, issue_25413)
    ],
    "privileges/kill mutation/:/:/KILL ALTER : with revoked privilege": [
        (Fail, issue_25413)
    ],
    "privileges/kill mutation/:/:/KILL ALTER : with revoked ALL privilege": [
        (Fail, issue_25413)
    ],
    "privileges/create table/create with subquery privilege granted directly or via role/create with subquery, privilege granted directly": [
        (Fail, issue_26746)
    ],
    "privileges/create table/create with subquery privilege granted directly or via role/create with subquery, privilege granted through a role": [
        (Fail, issue_26746)
    ],
    "views/live view/create with join subquery privilege granted directly or via role/create with join subquery, privilege granted directly": [
        (Fail, issue_26746)
    ],
    "views/live view/create with join subquery privilege granted directly or via role/create with join subquery, privilege granted through a role": [
        (Fail, issue_26746)
    ],
}

xflags = {
    "privileges/alter index/table_type='ReplicatedVersionedCollapsingMergeTree-sharded_cluster'/role with privileges from role with grant option/granted=:/I try to ALTER INDEX with given privileges/I check order by when privilege is granted": (
        SKIP,
        0,
    )
}

ffails = {
    "/clickhouse/rbac/privileges/:/table_type='ReplicatedReplacingMergeTree-sharded_cluster": (
        Skip,
        "Causes clickhouse timeout on 21.10",
        (
            lambda test: check_clickhouse_version(">=21.10")(test)
            and check_clickhouse_version("<21.11")(test)
        ),
    ),
    "/clickhouse/rbac/views": (
        Skip,
        "Does not work on clickhouse 21.09",
        (
            lambda test: check_clickhouse_version(">=21.9")(test)
            and check_clickhouse_version("<21.10")(test)
        ),
    ),
}


@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@FFails(ffails)
@Name("rbac")
@Specifications(SRS_006_ClickHouse_Role_Based_Access_Control)
def regression(
    self, local, clickhouse_binary_path, clickhouse_version=None, stress=None
):
    """RBAC regression."""
    nodes = {"clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3")}

    self.context.clickhouse_version = clickhouse_version

    if stress is not None:
        self.context.stress = stress

    from platform import processor as current_cpu

    folder_name = os.path.basename(current_dir())
    if current_cpu() == "aarch64":
        env = f"{folder_name}_env_arm64"
    else:
        env = f"{folder_name}_env"

    with Cluster(
        local,
        clickhouse_binary_path,
        nodes=nodes,
        docker_compose_project_dir=os.path.join(current_dir(), env),
    ) as cluster:
        self.context.cluster = cluster

        Feature(run=load("rbac.tests.syntax.feature", "feature"))
        Feature(run=load("rbac.tests.privileges.feature", "feature"))
        Feature(run=load("rbac.tests.views.feature", "feature"))


if main():
    regression()

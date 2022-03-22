#!/usr/bin/env python3
import os
import sys
from testflows.core import *

append_path(sys.path, "..", "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.role_mapping.requirements import *
from helpers.common import check_clickhouse_version

# Cross-outs of known fails
xfails = {
    "mapping/roles removed and added in parallel": [(Fail, "known bug")],
    "user dn detection/mapping/roles removed and added in parallel": [
        (Fail, "known bug")
    ],
    "cluster secret/external user directory/:/:/cluster with secret/ldap user/:mapped True/select using mapped role/with privilege on source and distributed": [
        (Fail, "https://github.com/ClickHouse/ClickHouse/issues/34130")
    ],
}

# Force results without running the test
ffails = {
    "cluster secret": (
        Skip,
        "feature available on 20.10+",
        check_clickhouse_version("<20.10"),
    )
}


@TestFeature
@Name("role mapping")
@ArgumentParser(argparser)
@Specifications(SRS_014_ClickHouse_LDAP_Role_Mapping)
@Requirements(RQ_SRS_014_LDAP_RoleMapping("1.0"))
@XFails(xfails)
@FFails(ffails)
def regression(
    self, local, clickhouse_binary_path, clickhouse_version=None, stress=None
):
    """ClickHouse LDAP role mapping regression module."""
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

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

        Scenario(
            run=load("ldap.authentication.tests.sanity", "scenario"), name="ldap sanity"
        )
        Feature(run=load("ldap.role_mapping.tests.server_config", "feature"))
        Feature(run=load("ldap.role_mapping.tests.mapping", "feature"))
        Feature(run=load("ldap.role_mapping.tests.user_dn_detection", "feature"))
        Feature(run=load("ldap.role_mapping.tests.cluster_secret", "feature"))


if main():
    regression()

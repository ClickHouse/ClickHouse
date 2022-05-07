#!/usr/bin/env python3
import os
import sys
from testflows.core import *

append_path(sys.path, "..", "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.external_user_directory.requirements import *
from helpers.common import check_clickhouse_version

# Cross-outs of known fails
xfails = {
    "connection protocols/tls/tls_require_cert='try'": [
        (Fail, "can't be tested with self-signed certificates")
    ],
    "connection protocols/tls/tls_require_cert='demand'": [
        (Fail, "can't be tested with self-signed certificates")
    ],
    "connection protocols/starttls/tls_require_cert='try'": [
        (Fail, "can't be tested with self-signed certificates")
    ],
    "connection protocols/starttls/tls_require_cert='demand'": [
        (Fail, "can't be tested with self-signed certificates")
    ],
    "connection protocols/tls require cert default demand": [
        (Fail, "can't be tested with self-signed certificates")
    ],
    "connection protocols/starttls with custom port": [
        (
            Fail,
            "it seems that starttls is not enabled by default on custom plain-text ports in LDAP server",
        )
    ],
    "connection protocols/tls cipher suite": [(Fail, "can't get it to work")],
}

ffails = {
    "user authentications/verification cooldown performance/:": (
        Skip,
        "causes timeout on 21.8",
        (
            lambda test: check_clickhouse_version(">=21.8")(test)
            and check_clickhouse_version("<21.9")(test)
        ),
    )
}


@TestFeature
@Name("external user directory")
@ArgumentParser(argparser)
@Specifications(SRS_009_ClickHouse_LDAP_External_User_Directory)
@Requirements(RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication("1.0"))
@XFails(xfails)
@FFails(ffails)
def regression(
    self, local, clickhouse_binary_path, clickhouse_version=None, stress=None
):
    """ClickHouse LDAP external user directory regression module."""
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

        Scenario(run=load("ldap.authentication.tests.sanity", "scenario"))
        Scenario(run=load("ldap.external_user_directory.tests.simple", "scenario"))
        Feature(run=load("ldap.external_user_directory.tests.restart", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.server_config", "feature"))
        Feature(
            run=load(
                "ldap.external_user_directory.tests.external_user_directory_config",
                "feature",
            )
        )
        Feature(run=load("ldap.external_user_directory.tests.connections", "feature"))
        Feature(
            run=load("ldap.external_user_directory.tests.authentications", "feature")
        )
        Feature(run=load("ldap.external_user_directory.tests.roles", "feature"))


if main():
    regression()

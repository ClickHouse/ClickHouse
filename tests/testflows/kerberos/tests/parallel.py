from testflows.core import *
from kerberos.tests.common import *
from kerberos.requirements.requirements import *
from multiprocessing.dummy import Pool

import time
import datetime
import itertools


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Parallel_ValidRequests_SameCredentials("1.0")
)
def valid_requests_same_credentials(self):
    """ClickHouse should be able to process parallel requests sent under the same credentials.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client1_node = ch_nodes[1]
    client2_node = ch_nodes[2]


    with Given("kinit for clients"):
        kinit_no_keytab(node=client1_node)
        kinit_no_keytab(node=client2_node)

    with And('create server principal'):
        create_server_principal(node=server_node)

    def helper(cmd):
        """Just a tiny helper: async needs a function to be passed
        """
        return cmd(test_select_query(node=server_node))

    for i in range(15):
        pool = Pool(2)
        tasks = []
        with When("I try simultaneous authentication"):
            tasks.append(pool.apply_async(helper, (client1_node.cmd, )))
            tasks.append(pool.apply_async(helper, (client2_node.cmd, )))
            tasks[0].wait(timeout=200)
            tasks[1].wait(timeout=200)

        with Then(f"I expect requests to success"):
            assert tasks[0].get(timeout=300).output == "kerberos_user", error()
            assert tasks[1].get(timeout=300).output == "kerberos_user", error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Parallel_ValidRequests_DifferentCredentials("1.0")
)
def valid_requests_different_credentials(self):
    """ClickHouse should be able to process parallel requests by different users.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client1_node = ch_nodes[1]
    client2_node = ch_nodes[2]

    with Given("kinit for clients"):
        kinit_no_keytab(node=client1_node, principal="krb1")
        kinit_no_keytab(node=client2_node, principal="krb2")

    with And("create server principal"):
        create_server_principal(node=server_node)

    def helper(cmd):
        """Just a tiny helper: async needs a function to be passed
        """
        return cmd(test_select_query(node=server_node))

    for i in range(15):
        pool = Pool(2)
        tasks = []

        with And("add 2 kerberos users via RBAC"):
            server_node.query("CREATE USER krb1 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")
            server_node.query("CREATE USER krb2 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")

        with When("I try simultaneous authentication for valid and invalid"):
            tasks.append(pool.apply_async(helper, (client1_node.cmd, )))
            tasks.append(pool.apply_async(helper, (client2_node.cmd, )))
            tasks[0].wait(timeout=200)
            tasks[1].wait(timeout=200)

        with Then(f"I expect have auth failure"):
            assert tasks[1].get(timeout=300).output == "krb2", error()
            assert tasks[0].get(timeout=300).output == "krb1", error()

        with Finally("I make sure both users are removed"):
            server_node.query("DROP USER IF EXISTS krb1", no_checks=True)
            server_node.query("DROP USER IF EXISTS krb2", no_checks=True)


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Parallel_ValidInvalid("1.0")
)
def valid_invalid(self):
    """Valid users' Kerberos authentication should not be affected by invalid users' attempts.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client1_node = ch_nodes[1]
    client2_node = ch_nodes[2]

    with Given("kinit for clients"):
        kinit_no_keytab(node=client2_node)
        kinit_no_keytab(node=client1_node, principal="invalid_user")

    with And('create server principal'):
        create_server_principal(node=server_node)

    def helper(cmd):
        """Just a tiny helper: async needs a function to be passed
        """
        return cmd(test_select_query(node=server_node), no_checks=True)

    for i in range(15):
        pool = Pool(2)
        tasks = []
        with When("I try simultaneous authentication for valid and invalid"):
            tasks.append(pool.apply_async(helper, (client1_node.cmd, )))     # invalid
            tasks.append(pool.apply_async(helper, (client2_node.cmd, )))     # valid

        with Then(f"I expect have auth failure"):
            assert tasks[1].get(timeout=300).output == "kerberos_user", error()
            assert tasks[0].get(timeout=300).output != "kerberos_user", error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Parallel_Deletion("1.0")
)
def deletion(self):
    """ClickHouse SHALL NOT crash when 2 Kerberos users are simultaneously deleting one another.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client1_node = ch_nodes[1]
    client2_node = ch_nodes[2]

    with Given("kinit for clients"):
        kinit_no_keytab(node=client1_node, principal="krb1")
        kinit_no_keytab(node=client2_node, principal="krb2")

    with And("create server principal"):
        create_server_principal(node=server_node)

    def delete_helper(cmd, todel):
        """Just a tiny helper: async needs a function to be passed
        """
        return cmd(test_select_query(node=server_node, req=f"DROP USER IF EXISTS {todel}"), no_checks=True)

    for i in range(15):
        pool = Pool(2)
        tasks = []

        with And("add 2 kerberos users via RBAC"):
            server_node.query("CREATE USER krb1 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")
            server_node.query("CREATE USER krb2 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")
            server_node.query("GRANT ACCESS MANAGEMENT ON *.* TO krb1")
            server_node.query("GRANT ACCESS MANAGEMENT ON *.* TO krb2")


        with When("I try simultaneous authentication for valid and invalid"):
            tasks.append(pool.apply_async(delete_helper, (client1_node.cmd, "krb2")))
            tasks.append(pool.apply_async(delete_helper, (client2_node.cmd, "krb1")))
            tasks[0].wait(timeout=200)
            tasks[1].wait(timeout=200)

        with Then(f"I check CH is alive"):
            assert ch_nodes[0].query("SELECT 1").output == "1", error()

        with Finally("I make sure both users are removed"):
            server_node.query("DROP USER krb1", no_checks=True)
            server_node.query("DROP USER krb2", no_checks=True)


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Parallel_ValidRequests_KerberosAndNonKerberos("1.0")
)
def kerberos_and_nonkerberos(self):
    """ClickHouse SHALL support processing of simultaneous kerberized and non-kerberized requests.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client1_node = ch_nodes[1]
    client2_node = ch_nodes[2]

    with Given("kinit for clients"):
        kinit_no_keytab(node=client2_node)

    with And('create server principal'):
        create_server_principal(node=server_node)

    def helper(cmd, krb_auth):
        """Just a tiny helper: async needs a function to be passed
        """
        return cmd(test_select_query(node=server_node, krb_auth=krb_auth), no_checks=True)

    for i in range(15):
        pool = Pool(2)
        tasks = []
        with When("I try simultaneous authentication for valid and invalid"):
            tasks.append(pool.apply_async(helper, (client1_node.cmd, False)))  # non-kerberos
            tasks.append(pool.apply_async(helper, (client2_node.cmd, True)))  # kerberos

        with Then(f"I expect have auth failure"):
            assert tasks[1].get(timeout=300).output == "kerberos_user", error()
            assert tasks[0].get(timeout=300).output == "default", error()


@TestFeature
@Requirements(
    RQ_SRS_016_Kerberos_Parallel("1.0")
)
def parallel(self):
    """Perform ClickHouse Kerberos authentication testing for incorrect configuration files
    """

    self.context.ch_nodes = [self.context.cluster.node(f"clickhouse{i}") for i in range(1, 4)]
    self.context.krb_server = self.context.cluster.node("kerberos")

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)

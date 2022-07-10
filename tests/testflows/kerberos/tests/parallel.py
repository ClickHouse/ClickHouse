from testflows.core import *
from kerberos.tests.common import *
from kerberos.requirements.requirements import *


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Parallel_ValidRequests_SameCredentials("1.0"))
def valid_requests_same_credentials(self):
    """ClickHouse should be able to process parallel requests sent under the same credentials."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for clients"):
        kinit_no_keytab(node=ch_nodes[1])
        kinit_no_keytab(node=ch_nodes[2])

    with And("create server principal"):
        create_server_principal(node=ch_nodes[0])

    def helper(cmd):
        return cmd(test_select_query(node=ch_nodes[0]))

    for i in range(15):
        tasks = []
        with Pool(2) as pool:
            with When("I try simultaneous authentication"):
                tasks.append(pool.submit(helper, (ch_nodes[1].cmd,)))
                tasks.append(pool.submit(helper, (ch_nodes[2].cmd,)))
                tasks[0].result(timeout=200)
                tasks[1].result(timeout=200)

            with Then(f"I expect requests to success"):
                assert tasks[0].result(timeout=300).output == "kerberos_user", error()
                assert tasks[1].result(timeout=300).output == "kerberos_user", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Parallel_ValidRequests_DifferentCredentials("1.0"))
def valid_requests_different_credentials(self):
    """ClickHouse should be able to process parallel requests by different users."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for clients"):
        kinit_no_keytab(node=ch_nodes[1], principal="krb1")
        kinit_no_keytab(node=ch_nodes[2], principal="krb2")

    with And("create server principal"):
        create_server_principal(node=ch_nodes[0])

    def helper(cmd):
        return cmd(test_select_query(node=ch_nodes[0]))

    for i in range(15):

        tasks = []
        with Pool(2) as pool:
            with And("add 2 kerberos users via RBAC"):
                ch_nodes[0].query(
                    "CREATE USER krb1 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
                )
                ch_nodes[0].query(
                    "CREATE USER krb2 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
                )

            with When("I try simultaneous authentication for valid and invalid"):
                tasks.append(pool.submit(helper, (ch_nodes[1].cmd,)))
                tasks.append(pool.submit(helper, (ch_nodes[2].cmd,)))
                tasks[0].result(timeout=200)
                tasks[1].result(timeout=200)

            with Then(f"I expect have auth failure"):
                assert tasks[1].result(timeout=300).output == "krb2", error()
                assert tasks[0].result(timeout=300).output == "krb1", error()

            with Finally("I make sure both users are removed"):
                ch_nodes[0].query("DROP USER krb1", no_checks=True)
                ch_nodes[0].query("DROP USER krb2", no_checks=True)


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Parallel_ValidInvalid("1.0"))
def valid_invalid(self):
    """Valid users' Kerberos authentication should not be affected by invalid users' attempts."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for clients"):
        kinit_no_keytab(node=ch_nodes[2])
        kinit_no_keytab(node=ch_nodes[1], principal="invalid_user")

    with And("create server principal"):
        create_server_principal(node=ch_nodes[0])

    def helper(cmd):
        return cmd(test_select_query(node=ch_nodes[0]), no_checks=True)

    for i in range(15):
        tasks = []
        with Pool(2) as pool:
            with When("I try simultaneous authentication for valid and invalid"):
                tasks.append(pool.submit(helper, (ch_nodes[1].cmd,)))  # invalid
                tasks.append(pool.submit(helper, (ch_nodes[2].cmd,)))  # valid

            with Then(f"I expect have auth failure"):
                assert tasks[1].result(timeout=300).output == "kerberos_user", error()
                assert tasks[0].result(timeout=300).output != "kerberos_user", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Parallel_Deletion("1.0"))
def deletion(self):
    """ClickHouse SHALL NOT crash when 2 Kerberos users are simultaneously deleting one another."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for clients"):
        kinit_no_keytab(node=ch_nodes[1], principal="krb1")
        kinit_no_keytab(node=ch_nodes[2], principal="krb2")

    with And("create server principal"):
        create_server_principal(node=ch_nodes[0])

    def helper(cmd, todel):
        return cmd(
            test_select_query(node=ch_nodes[0], req=f"DROP USER {todel}"),
            no_checks=True,
        )

    for i in range(15):
        tasks = []
        with Pool(2) as pool:
            with And("add 2 kerberos users via RBAC"):
                ch_nodes[0].query(
                    "CREATE USER krb1 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
                )
                ch_nodes[0].query(
                    "CREATE USER krb2 IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
                )
                ch_nodes[0].query("GRANT ACCESS MANAGEMENT ON *.* TO krb1")
                ch_nodes[0].query("GRANT ACCESS MANAGEMENT ON *.* TO krb2")

            with When("I try simultaneous authentication for valid and invalid"):
                tasks.append(pool.submit(helper, (ch_nodes[1].cmd, "krb2")))
                tasks.append(pool.submit(helper, (ch_nodes[2].cmd, "krb1")))
                tasks[0].result(timeout=200)
                tasks[1].result(timeout=200)

            with Then(f"I check CH is alive"):
                assert ch_nodes[0].query("SELECT 1").output == "1", error()

            with Finally("I make sure both users are removed"):
                ch_nodes[0].query("DROP USER krb1", no_checks=True)
                ch_nodes[0].query("DROP USER krb2", no_checks=True)


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Parallel_ValidRequests_KerberosAndNonKerberos("1.0"))
def kerberos_and_nonkerberos(self):
    """ClickHouse SHALL support processing of simultaneous kerberized and non-kerberized requests."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for clients"):
        kinit_no_keytab(node=ch_nodes[2])

    with And("create server principal"):
        create_server_principal(node=ch_nodes[0])

    def helper(cmd, krb_auth):
        return cmd(
            test_select_query(node=ch_nodes[0], krb_auth=krb_auth), no_checks=True
        )

    for i in range(15):
        tasks = []
        with Pool(2) as pool:
            with When("I try simultaneous authentication for valid and invalid"):
                tasks.append(
                    pool.submit(helper, (ch_nodes[1].cmd, False))
                )  # non-kerberos
                tasks.append(pool.submit(helper, (ch_nodes[2].cmd, True)))  # kerberos

            with Then(f"I expect have auth failure"):
                assert tasks[1].result(timeout=300).output == "kerberos_user", error()
                assert tasks[0].result(timeout=300).output == "default", error()


@TestFeature
@Requirements(RQ_SRS_016_Kerberos_Parallel("1.0"))
def parallel(self):
    """Perform ClickHouse Kerberos authentication testing for incorrect configuration files"""

    self.context.ch_nodes = [
        self.context.cluster.node(f"clickhouse{i}") for i in range(1, 4)
    ]
    self.context.krb_server = self.context.cluster.node("kerberos")
    self.context.clients = [
        self.context.cluster.node(f"krb-client{i}") for i in range(1, 6)
    ]

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)

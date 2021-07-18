from testflows.core import *
from kerberos.tests.common import *
from kerberos.requirements.requirements import *

import time


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Ping("1.0")
)
def ping(self):
    """Containers should be reachable
    """
    ch_nodes = self.context.ch_nodes

    for i in range(3):
        with When(f"curl ch_{i} kerberos"):
            r = ch_nodes[i].command(f"curl kerberos -c 1")
        with Then(f"return code should be 0"):
            assert r.exitcode == 7, error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_ValidUser_XMLConfiguredUser("1.0")
)
def xml_configured_user(self):
    """ClickHouse SHALL accept Kerberos authentication for valid XML-configured user
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    with Given("kinit for client"):
        kinit_no_keytab(node=client_node)

    with And("kinit for server"):
        create_server_principal(node=server_node)

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=server_node))

    with Then(f"I expect 'kerberos_user'"):
        assert r.output == "kerberos_user", error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_ValidUser_RBACConfiguredUser("1.0")
)
def rbac_configured_user(self):
    """ClickHouse SHALL accept Kerberos authentication for valid RBAC-configured user
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    try:
        with Given("kinit for client"):
            kinit_no_keytab(node=client_node, principal="krb_rbac")

        with And("kinit for server"):
            create_server_principal(node=server_node)

        with When("I create a RBAC user"):
            server_node.query("CREATE USER krb_rbac IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")

        with And("I attempt to authenticate"):
            r = client_node.cmd(test_select_query(node=server_node))

        with Then("I expect user to be 'krb_rbac'"):
            assert r.output == "krb_rbac", error()

    finally:
        with Finally("I restore server original state"):
            server_node.query("DROP USER IF EXISTS krb_rbac")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_KerberosNotAvailable_InvalidServerTicket("1.0")
)
def invalid_server_ticket(self):
    """ClickHouse SHALL reject Kerberos authentication no Kerberos server is reachable
    and CH-server has no valid ticket (or the existing ticket is outdated).
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    try:
        with Given("kinit for client"):
            kinit_no_keytab(node=client_node)

        with And("I set up server principal"):
            create_server_principal(node=server_node)

        with And("I kill kerberos-server"):
            self.context.krb_server.stop()

        with When("I attempt to authenticate as kerberos_user"):
            r = client_node.cmd(test_select_query(node=server_node))

        with Then("I expect the user to be default"):
            assert r.output == "default", error()

    finally:
        with Finally("I start kerberos server again"):
            self.context.krb_server.start()
            client_node.cmd("kdestroy")
            while True:
                kinit_no_keytab(node=client_node)
                create_server_principal(node=server_node)
                if client_node.cmd(test_select_query(node=server_node)).output == "kerberos_user":
                    break
                debug(test_select_query(node=server_node))
            client_node.cmd("kdestroy")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_KerberosNotAvailable_InvalidClientTicket("1.0")
)
def invalid_client_ticket(self):
    """ClickHouse SHALL reject Kerberos authentication in case client has
     no valid ticket (or the existing ticket is outdated).
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    with Given("kinit for client"):
        kinit_no_keytab(node=client_node, lifetime_option="-l 00:00:05")

    with And("setting up server principal"):
        create_server_principal(node=server_node)

    with And("I wait until client ticket is expired"):
        time.sleep(10)

    with When("I attempt to authenticate as kerberos_user"):
        r = client_node.cmd(test_select_query(node=server_node))

    with Then("I expect the user to be default"):
        assert r.output == "default", error()


@TestCase
@Requirements(
    RQ_SRS_016_Kerberos_KerberosNotAvailable_ValidTickets("1.0")
)
def kerberos_unreachable_valid_tickets(self):
    """ClickHouse SHALL accept Kerberos authentication if no Kerberos server is reachable
    but both CH-server and client have valid tickets.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    try:
        with Given("kinit for client"):
            kinit_no_keytab(node=client_node)

        with And("setting up server principal"):
            create_server_principal(node=server_node)

        with And("make sure server obtained ticket"):
            client_node.cmd(test_select_query(node=server_node))

        with When("I kill kerberos-server"):
            self.context.krb_server.stop()

        with And("I attempt to authenticate as kerberos_user"):
            r = client_node.cmd(test_select_query(node=server_node))

        with Then("I expect the user to be default"):
            assert r.output == "kerberos_user", error()

    finally:
        with Finally("I start kerberos server again"):
            self.context.krb_server.start()
            client_node.cmd("kdestroy")
            while True:
                kinit_no_keytab(node=client_node)
                if client_node.cmd(test_select_query(node=server_node)).output == "kerberos_user":
                    break
            client_node.cmd("kdestroy")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_ValidUser_KerberosNotConfigured("1.0")
)
def kerberos_not_configured(self):
    """ClickHouse SHALL reject Kerberos authentication if user is not a kerberos-auth user.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    try:
        with Given("kinit for client"):
            kinit_no_keytab(node=client_node, principal="unkerberized")

        with And('kinit for server'):
            create_server_principal(node=server_node)

        with By("I add non-Kerberos user to ClickHouse"):
            server_node.query("CREATE USER unkerberized IDENTIFIED WITH plaintext_password BY 'qwerty'")

        with When("I attempt to authenticate"):
            r = client_node.cmd(test_select_query(node=server_node), no_checks=True)

        with Then("I expect authentication failure"):
            assert "Authentication failed" in r.output, error()

    finally:
        with Finally("I drop the user"):
            server_node.query("DROP USER IF EXISTS unkerberized")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_KerberosServerRestarted("1.0")
)
def kerberos_server_restarted(self):
    """ClickHouse SHALL accept Kerberos authentication if Kerberos server was restarted.
    """
    ch_nodes = self.context.ch_nodes
    krb_server = self.context.krb_server
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    with Given("I obtain keytab for user"):
        kinit_no_keytab(node=client_node)

    with And("I create server principal"):
        create_server_principal(node=server_node)

    with And("I obtain server ticket"):
        client_node.cmd(test_select_query(node=server_node), no_checks=True)

    with When("I dump, restart and restore kerberos server"):
        krb_server.cmd("kdb5_util dump dump.dmp", shell_command="/bin/sh")
        krb_server.restart()
        krb_server.cmd("kdb5_util load dump.dmp", shell_command="/bin/sh")

    with Then("I attempt to authenticate"):
        r = client_node.cmd(test_select_query(node=server_node))

    with And("I wait for kerberos to be healthy"):
        client_node.cmd("kdestroy")
        while True:
            kinit_no_keytab(node=ch_nodes[2])
            if client_node.cmd(test_select_query(node=server_node)).output == "kerberos_user":
                break

    with Then(f"I expect kerberos_user"):
        assert r.output == "kerberos_user", error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_InvalidUser("1.0")
)
def invalid_user(self):
    """ClickHouse SHALL reject Kerberos authentication for invalid principal
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    with Given("I obtain keytab for invalid user"):
        kinit_no_keytab(node=client_node, principal="invalid")

    with And("I create server principal"):
        create_server_principal(node=server_node)

    with When("I attempt to authenticate"):
        r = client_node.cmd(test_select_query(node=server_node), no_checks=True)

    with Then(f"I expect default"):
        assert "Authentication failed: password is incorrect or there is no user with such name" in r.output, error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_InvalidUser_UserDeleted("1.0")
)
def user_deleted(self):
    """ClickHouse SHALL reject Kerberos authentication if Kerberos user was deleted prior to query.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    with Given("I obtain keytab for a user"):
        kinit_no_keytab(node=client_node, principal="krb_rbac")

    with And("I create server principal"):
        create_server_principal(node=server_node)

    with And("I create and then delete kerberized user"):
        server_node.query("CREATE USER krb_rbac IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'")
        server_node.query("DROP USER IF EXISTS krb_rbac")

    with When("I attempt to authenticate"):
        r = client_node.cmd(test_select_query(node=server_node), no_checks=True)

    with Then(f"I expect error"):
        assert "Authentication failed: password is incorrect or there is no user with such name" in r.output, error()


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Performance("1.0")
)
def authentication_performance(self):
    """ClickHouse's performance for Kerberos authentication SHALL shall be comparable to regular authentication.
    """
    ch_nodes = self.context.ch_nodes
    server_node = ch_nodes[0]
    client_node = ch_nodes[2]

    try:
        with Given("I obtain keytab for a user"):
            kinit_no_keytab(node=client_node)

        with And("I create server principal"):
            create_server_principal(node=server_node)

        with And("I create a password-identified user"):
            server_node.query("CREATE USER pwd_user IDENTIFIED WITH plaintext_password BY 'pwd'")

        with When("I measure kerberos auth time"):
            start_time_krb = time.time()
            for i in range(100):
                client_node.cmd(test_select_query(node=server_node))
            krb_time = (time.time() - start_time_krb) / 100

        with And("I measure password auth time"):
            start_time_usual = time.time()
            for i in range(100):
                client_node.cmd(f"echo 'SELECT 1' | curl 'http://pwd_user:pwd@clickhouse1:8123/' -d @-")
            usual_time = (time.time() - start_time_usual) / 100

        with Then("measuring the performance compared to password auth"):
            metric("percentage_improvement", units="%", value=100*(krb_time - usual_time)/usual_time)

    finally:
        with Finally("I drop pwd_user"):
            server_node.query("DROP USER IF EXISTS pwd_user")


@TestFeature
def generic(self):
    """Perform ClickHouse Kerberos authentication testing
    """

    self.context.ch_nodes = [self.context.cluster.node(f"clickhouse{i}") for i in range(1, 4)]
    self.context.krb_server = self.context.cluster.node("kerberos")

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)

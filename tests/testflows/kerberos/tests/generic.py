from testflows.core import *
from kerberos.tests.common import *
from kerberos.requirements.requirements import *

import time


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Ping("1.0"))
def ping(self):
    """Containers should be reachable"""
    ch_nodes = self.context.ch_nodes

    for i in range(3):
        with When(f"curl ch_{i} kerberos"):
            r = ch_nodes[i].command(f"curl kerberos -c 1")
        with Then(f"return code should be 0"):
            assert r.exitcode == 7, error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_ValidUser_XMLConfiguredUser("1.0"))
def xml_configured_user(self):
    """ClickHouse SHALL accept Kerberos authentication for valid XML-configured user"""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2])

    with And("kinit for server"):
        create_server_principal(node=ch_nodes[0])

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with Then(f"I expect 'kerberos_user'"):
        assert r.output == "kerberos_user", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_ValidUser_RBACConfiguredUser("1.0"))
def rbac_configured_user(self):
    """ClickHouse SHALL accept Kerberos authentication for valid RBAC-configured user"""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2], principal="krb_rbac")

    with And("kinit for server"):
        create_server_principal(node=ch_nodes[0])

    with When("I create a RBAC user"):
        ch_nodes[0].query(
            "CREATE USER krb_rbac IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
        )

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with Then("I restore server original state"):
        ch_nodes[0].query("DROP USER krb_rbac")

    with Finally("I expect 'krb_rbac'"):
        assert r.output == "krb_rbac", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_KerberosNotAvailable_InvalidServerTicket("1.0"))
def invalid_server_ticket(self):
    """ClickHouse SHALL reject Kerberos authentication no Kerberos server is reachable
    and CH-server has no valid ticket (or the existing ticket is outdated).
    """
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2])

    with And("setting up server principal"):
        create_server_principal(node=ch_nodes[0])

    with And("I kill kerberos-server"):
        self.context.krb_server.stop()

    with When("I attempt to authenticate as kerberos_user"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with Then("I start kerberos server again"):
        self.context.krb_server.start()
        ch_nodes[2].cmd("kdestroy")
        while True:
            kinit_no_keytab(node=ch_nodes[2])
            create_server_principal(node=ch_nodes[0])
            if (
                ch_nodes[2].cmd(test_select_query(node=ch_nodes[0])).output
                == "kerberos_user"
            ):
                break
            debug(test_select_query(node=ch_nodes[0]))
        ch_nodes[2].cmd("kdestroy")

    with And("I expect the user to be default"):
        assert r.output == "default", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_KerberosNotAvailable_InvalidClientTicket("1.0"))
def invalid_client_ticket(self):
    """ClickHouse SHALL reject Kerberos authentication in case client has
    no valid ticket (or the existing ticket is outdated).
    """
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2], lifetime_option="-l 00:00:05")

    with And("setting up server principal"):
        create_server_principal(node=ch_nodes[0])

    # with And("I kill kerberos-server"):
    #     self.context.krb_server.stop()

    with And("I wait until client ticket is expired"):
        time.sleep(10)

    with When("I attempt to authenticate as kerberos_user"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with Then("I expect the user to be default"):
        assert r.output == "default", error()

    with Finally(""):
        # self.context.krb_server.start()
        time.sleep(1)
        ch_nodes[2].cmd(f"echo pwd | kinit -l 10:00 kerberos_user")
        while True:
            time.sleep(1)
            if (
                ch_nodes[2].cmd(test_select_query(node=ch_nodes[0])).output
                == "kerberos_user"
            ):
                break
        ch_nodes[2].cmd("kdestroy")


@TestCase
@Requirements(RQ_SRS_016_Kerberos_KerberosNotAvailable_ValidTickets("1.0"))
def kerberos_unreachable_valid_tickets(self):
    """ClickHouse SHALL accept Kerberos authentication if no Kerberos server is reachable
    but both CH-server and client have valid tickets.
    """
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2])

    with And("setting up server principal"):
        create_server_principal(node=ch_nodes[0])

    with And("make sure server obtained ticket"):
        ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with And("I kill kerberos-server"):
        self.context.krb_server.stop()

    with When("I attempt to authenticate as kerberos_user"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with Then("I expect the user to be default"):
        assert r.output == "kerberos_user", error()

    with Finally("I start kerberos server again"):
        self.context.krb_server.start()
        ch_nodes[2].cmd("kdestroy")
        while True:
            kinit_no_keytab(node=ch_nodes[2])
            if (
                ch_nodes[2].cmd(test_select_query(node=ch_nodes[0])).output
                == "kerberos_user"
            ):
                break
        ch_nodes[2].cmd("kdestroy")


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_ValidUser_KerberosNotConfigured("1.0"))
def kerberos_not_configured(self):
    """ClickHouse SHALL reject Kerberos authentication if user is not a kerberos-auth user."""
    ch_nodes = self.context.ch_nodes

    with Given("kinit for client"):
        kinit_no_keytab(node=ch_nodes[2], principal="unkerberized")

    with And("Kinit for server"):
        create_server_principal(node=ch_nodes[0])

    with By("I add non-Kerberos user to ClickHouse"):
        ch_nodes[0].query(
            "CREATE USER unkerberized IDENTIFIED WITH plaintext_password BY 'qwerty'"
        )

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]), no_checks=True)

    with Then("I expect authentication failure"):
        assert "Authentication failed" in r.output, error()

    with Finally("I drop the user"):
        ch_nodes[0].query("DROP USER unkerberized")


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_KerberosServerRestarted("1.0"))
def kerberos_server_restarted(self):
    """ClickHouse SHALL accept Kerberos authentication if Kerberos server was restarted."""
    ch_nodes = self.context.ch_nodes
    krb_server = self.context.krb_server

    with Given("I obtain keytab for user"):
        kinit_no_keytab(node=ch_nodes[2])
    with And("I create server principal"):
        create_server_principal(node=ch_nodes[0])
    with And("I obtain server ticket"):
        ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]), no_checks=True)
    with By("I dump, restart and restore kerberos server"):
        krb_server.cmd("kdb5_util dump dump.dmp", shell_command="/bin/sh")
        krb_server.restart()
        krb_server.cmd("kdb5_util load dump.dmp", shell_command="/bin/sh")

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))

    with And("I wait for kerberos to be healthy"):
        ch_nodes[2].cmd("kdestroy")
        while True:
            kinit_no_keytab(node=ch_nodes[2])
            if (
                ch_nodes[2].cmd(test_select_query(node=ch_nodes[0])).output
                == "kerberos_user"
            ):
                break

    with Then(f"I expect kerberos_user"):
        assert r.output == "kerberos_user", error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_InvalidUser("1.0"))
def invalid_user(self):
    """ClickHouse SHALL reject Kerberos authentication for invalid principal"""
    ch_nodes = self.context.ch_nodes

    with Given("I obtain keytab for invalid user"):
        kinit_no_keytab(node=ch_nodes[2], principal="invalid")

    with And("I create server principal"):
        create_server_principal(node=ch_nodes[0])

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]), no_checks=True)

    with Then(f"I expect default"):
        assert (
            "Authentication failed: password is incorrect or there is no user with such name"
            in r.output
        ), error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_InvalidUser_UserDeleted("1.0"))
def user_deleted(self):
    """ClickHouse SHALL reject Kerberos authentication if Kerberos user was deleted prior to query."""
    ch_nodes = self.context.ch_nodes

    with Given("I obtain keytab for a user"):
        kinit_no_keytab(node=ch_nodes[2], principal="krb_rbac")

    with And("I create server principal"):
        create_server_principal(node=ch_nodes[0])

    with And("I create and then delete kerberized user"):
        ch_nodes[0].query(
            "CREATE USER krb_rbac IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'"
        )
        ch_nodes[0].query("DROP USER krb_rbac")

    with When("I attempt to authenticate"):
        r = ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]), no_checks=True)

    with Then(f"I expect error"):
        assert (
            "Authentication failed: password is incorrect or there is no user with such name"
            in r.output
        ), error()


@TestScenario
@Requirements(RQ_SRS_016_Kerberos_Performance("1.0"))
def authentication_performance(self):
    """ClickHouse's performance for Kerberos authentication SHALL shall be comparable to regular authentication."""
    ch_nodes = self.context.ch_nodes

    with Given("I obtain keytab for a user"):
        kinit_no_keytab(node=ch_nodes[2])

    with And("I create server principal"):
        create_server_principal(node=ch_nodes[0])

    with And("I create a password-identified user"):
        ch_nodes[0].query(
            "CREATE USER pwd_user IDENTIFIED WITH plaintext_password BY 'pwd'"
        )

    with When("I measure kerberos auth time"):
        start_time_krb = time.time()
        for i in range(100):
            ch_nodes[2].cmd(test_select_query(node=ch_nodes[0]))
        krb_time = (time.time() - start_time_krb) / 100

    with And("I measure password auth time"):
        start_time_usual = time.time()
        for i in range(100):
            ch_nodes[2].cmd(
                f"echo 'SELECT 1' | curl 'http://pwd_user:pwd@clickhouse1:8123/' -d @-"
            )
        usual_time = (time.time() - start_time_usual) / 100

    with Then("measuring the performance compared to password auth"):
        metric(
            "percentage_improvement",
            units="%",
            value=100 * (krb_time - usual_time) / usual_time,
        )

    with Finally("I drop pwd_user"):
        ch_nodes[0].query("DROP USER pwd_user")


@TestFeature
def generic(self):
    """Perform ClickHouse Kerberos authentication testing"""

    self.context.ch_nodes = [
        self.context.cluster.node(f"clickhouse{i}") for i in range(1, 4)
    ]
    self.context.krb_server = self.context.cluster.node("kerberos")
    self.context.clients = [
        self.context.cluster.node(f"krb-client{i}") for i in range(1, 6)
    ]

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)  # , setup=instrument_clickhouse_server_log)

import os

from testflows.core import *
from testflows.asserts import error
from ssl_server.requirements import *
from ssl_server.tests.common import *


@TestOutline
def enable_ssl(
    self,
    my_own_ca_key_passphrase,
    server_key_passphrase,
    restart=True,
    restart_before=True,
    node=None,
    timeout=100,
):
    """Check enabling basic SSL server configuration."""
    if node is None:
        node = self.context.node

    my_own_ca_key = "my_own_ca.key"
    my_own_ca_crt = "my_own_ca.crt"
    server_key = "server.key"
    server_csr = "server.csr"
    server_crt = "server.crt"
    dh_params = "dh_params.pem"

    node_server_crt = "/etc/clickhouse-server/" + os.path.basename(server_crt)
    node_server_key = "/etc/clickhouse-server/" + os.path.basename(server_key)
    node_dh_params = "/etc/clickhouse-server/" + os.path.basename(dh_params)

    if restart_before:
        with Given("I restart clickhouse before enabling SSL"):
            node.restart_clickhouse()

    with Given("I create my own CA key"):
        my_own_ca_key = create_rsa_private_key(
            outfile=my_own_ca_key, passphrase=my_own_ca_key_passphrase
        )
        debug(f"{my_own_ca_key}")

    with And("I create my own CA certificate"):
        my_own_ca_crt = create_ca_certificate(
            outfile=my_own_ca_crt,
            key=my_own_ca_key,
            passphrase=my_own_ca_key_passphrase,
            common_name="root",
        )

    with And("I generate DH parameters"):
        dh_params = create_dh_params(outfile=dh_params)

    with And("I generate server key"):
        server_key = create_rsa_private_key(
            outfile=server_key, passphrase=server_key_passphrase
        )

    with And("I generate server certificate signing request"):
        server_csr = create_certificate_signing_request(
            outfile=server_csr,
            common_name="clickhouse1",
            key=server_key,
            passphrase=server_key_passphrase,
        )

    with And("I sign server certificate with my own CA"):
        server_crt = sign_certificate(
            outfile=server_crt,
            csr=server_csr,
            ca_certificate=my_own_ca_crt,
            ca_key=my_own_ca_key,
            ca_passphrase=my_own_ca_key_passphrase,
        )

    with And("I validate server certificate"):
        validate_certificate(certificate=server_crt, ca_certificate=my_own_ca_crt)

    with And("I add certificate to node"):
        add_trusted_ca_certificate(node=node, certificate=my_own_ca_crt)

    with And("I copy server certificate, key and dh params", description=f"{node}"):
        copy(dest_node=node, src_path=server_crt, dest_path=node_server_crt)
        copy(dest_node=node, src_path=server_key, dest_path=node_server_key)
        copy(dest_node=node, src_path=dh_params, dest_path=node_dh_params)

    with And("I set correct permission on server key file"):
        node.command(f'chmod 600 "{node_server_key}"')
        node.command(f"ls -l /etc/clickhouse-server/config.d")

    with And("I add SSL server configuration file"):
        entries = {
            "certificateFile": f"{node_server_crt}",
            "privateKeyFile": f"{node_server_key}",
            "dhParamsFile": f"{node_dh_params}",
            "verificationMode": "none",
            "loadDefaultCAFile": "true",
            "cacheSessions": "true",
            "disableProtocols": "sslv2,sslv3",
            "preferServerCiphers": "true",
        }
        if server_key_passphrase:
            entries["privateKeyPassphraseHandler"] = {
                "name": "KeyFileHandler",
                "options": [{"password": server_key_passphrase}],
            }
        add_ssl_server_configuration_file(entries=entries)

    with And("I add SSL ports configuration file"):
        add_secure_ports_configuration_file(restart=restart, timeout=timeout)

    with When("I execute query using secure connection"):
        r = node.query("SELECT 1", secure=True)

    with Then("it should work"):
        assert r.output == "1", error()


@TestScenario
def enable_ssl_no_server_key_passphrase(self, node=None):
    """Check enabling basic SSL server configuration
    without passphrase on server key.
    """
    enable_ssl(
        my_own_ca_key_passphrase="hello",
        server_key_passphrase="",
        restart=True,
        node=node,
    )


@TestScenario
def enable_ssl_no_server_key_passphrase_dynamically(self, node=None):
    """Check enabling basic SSL server configuration
    without passphrase on server key dynamically without restarting
    the server.
    """
    enable_ssl(
        my_own_ca_key_passphrase="hello",
        server_key_passphrase="",
        restart=False,
        node=node,
    )


@TestScenario
def enable_ssl_with_server_key_passphrase(self, node=None):
    """Check enabling basic SSL server configuration
    with passphrase on server key.
    """
    enable_ssl(
        my_own_ca_key_passphrase="hello",
        server_key_passphrase="hello",
        restart=True,
        node=node,
    )


@TestScenario
def enable_ssl_with_server_key_passphrase_dynamically(self, node=None):
    """Check enabling basic SSL server configuration
    with passphrase on server key dynamically without restarting
    the server.
    """
    enable_ssl(
        my_own_ca_key_passphrase="hello",
        server_key_passphrase="hello",
        restart=True,
        node=node,
    )


@TestFeature
@Name("ssl context")
@Requirements()
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()

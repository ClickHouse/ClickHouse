import os
import time

from testflows.core import *
from testflows.asserts import error
from ssl_server.requirements import *
from ssl_server.tests.common import *

@TestScenario
def enable_ssl(self, node=None):
    """Check enabling basic SSL server configuration.
    """
    uid = getuid()
    cluster = self.context.cluster
    if node is None:
        node = self.context.node

    my_own_ca_key = cluster.temp_file(f"my_own_ca_{uid}.key")
    my_own_ca_crt = cluster.temp_file(f"my_own_ca_{uid}.crt")
    server_key = cluster.temp_file(f"server_{uid}.key")
    server_csr = cluster.temp_file(f"server_{uid}.csr")
    server_crt = cluster.temp_file(f"server_{uid}.crt")
    dh_params = cluster.temp_file(f"dh_params_{uid}.pem")
    node_server_crt = "/etc/clickhouse-server/" + os.path.basename(server_crt)
    node_server_key = "/etc/clickhouse-server/" + os.path.basename(server_key)
    node_dh_params = "/etc/clickhouse-server/" + os.path.basename(dh_params)

    my_own_ca_key_passphrase = "hello"
    server_key_passpharase = "hello"

    with Given("I create my own CA key"):
        create_rsa_private_key(outfile=my_own_ca_key, passphrase=my_own_ca_key_passphrase)

    with And("I create my own CA certificate"):
        create_ca_certificate(outfile=my_own_ca_crt, key=my_own_ca_key,
            passphrase=my_own_ca_key_passphrase, common_name="root")

    with And("I generate DH parameters"):
        create_dh_params(outfile=dh_params)

    with And("I generate server key"):
        create_rsa_private_key(outfile=server_key, passphrase=server_key_passpharase)

    with And("I generate server certificate signing request"):
        create_certificate_signing_request(outfile=server_csr, common_name="clickhouse1",
            key=server_key, passphrase=server_key_passpharase)

    with And("I sign server certificate with my own CA"):
        sign_certificate(outfile=server_crt, csr=server_csr,
            ca_certificate=my_own_ca_crt, ca_key=my_own_ca_key,
            ca_passphrase=my_own_ca_key_passphrase)

    with And("I validate server certificate"):
        validate_certificate(certificate=server_crt, ca_certificate=my_own_ca_crt)

    with And("I add certificate to node"):
        add_trusted_ca_certificate(node=node, certificate=my_own_ca_crt)

    with And("I copy server certificate, key and dh params", description=f"{node}"):
        copy(dest_node=node, src_path=server_crt, dest_path=node_server_crt)
        copy(dest_node=node, src_path=server_key, dest_path=node_server_key)
        copy(dest_node=node, src_path=dh_params, dest_path=node_dh_params)

    with And("I set correct permission on server key file"):
        node.command(f"chmod 600 \"{node_server_key}\"")
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
            "privateKeyPassphraseHandler": {
	            "name": "KeyFileHandler",
                "options": [{"password": server_key_passpharase}]
            }
        }
        add_ssl_server_configuration_file(entries=entries)

    with And("I add SSL ports configuration file"):
        add_secure_ports_configuration_file(restart=True)

    with When("I execute query using secure connection"):
        r = node.query("SELECT 1", secure=True)

    with Then("it should work"):
        assert r.output == "1", error()


@TestFeature
@Name("dynamic ssl context")
@Requirements(
)
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()

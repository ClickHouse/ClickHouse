import os
from testflows.core import *
from testflows.asserts import error
from helpers.common import *


@TestStep(Given)
def add_ssl_server_configuration_file(self, entries,
        config=None, config_d_dir="/etc/clickhouse-server/config.d",
        config_file="ssl_server.xml", timeout=300, restart=False, node=None):
    """Add SSL server configuration to config.xml.

    Example parameters that are available for the server
    (https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h#L71)

    <privateKeyFile>mycert.key</privateKeyFile>
	<certificateFile>mycert.crt</certificateFile>
	<caConfig>rootcert.pem</caConfig>
	<verificationMode>none|relaxed|strict|once</verificationMode>
	<verificationDepth>1..9</verificationDepth>
	<loadDefaultCAFile>true|false</loadDefaultCAFile>
	<cipherList>ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH</cipherList>
	<preferServerCiphers>true|false</preferServerCiphers>
	<privateKeyPassphraseHandler>
	    <name>KeyFileHandler</name>
	    <options>
	        <password>test</password>
	    </options>
	</privateKeyPassphraseHandler>
	<invalidCertificateHandler>
	     <name>ConsoleCertificateHandler</name>
	</invalidCertificateHandler>
	<cacheSessions>true|false</cacheSessions>
	<sessionIdContext>someString</sessionIdContext> <!-- server only -->
	<sessionCacheSize>0..n</sessionCacheSize>       <!-- server only -->
	<sessionTimeout>0..n</sessionTimeout>           <!-- server only -->
	<extendedVerification>true|false</extendedVerification>
	<requireTLSv1>true|false</requireTLSv1>
	<requireTLSv1_1>true|false</requireTLSv1_1>
	<requireTLSv1_2>true|false</requireTLSv1_2>
	<disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1,tlsv1_2</disableProtocols>
	<dhParamsFile>dh.pem</dhParamsFile>
	<ecdhCurve>prime256v1</ecdhCurve>
    """
    _entries = {
        "openSSL": {
            "server": entries
        }
    }

    if config is None:
        config = create_xml_config_content(_entries, config_file=config_file, config_d_dir=config_d_dir)

    return add_config(config, timeout=timeout, restart=restart, node=node)


@TestStep(Given)
def add_ssl_client_configuration_file(self, entries,
        config=None, config_d_dir="/etc/clickhouse-server/config.d",
        config_file="ssl_client.xml", timeout=300, restart=False, node=None):
    """Add SSL client configuration to config.xml.

    Example parameters that are available for the server
    (https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h#L71)

    <privateKeyFile>mycert.key</privateKeyFile>
	<certificateFile>mycert.crt</certificateFile>
	<caConfig>rootcert.pem</caConfig>
	<verificationMode>none|relaxed|strict|once</verificationMode>
	<verificationDepth>1..9</verificationDepth>
	<loadDefaultCAFile>true|false</loadDefaultCAFile>
	<cipherList>ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH</cipherList>
	<preferServerCiphers>true|false</preferServerCiphers>
	<privateKeyPassphraseHandler>
	    <name>KeyFileHandler</name>
	    <options>
	        <password>test</password>
	    </options>
	</privateKeyPassphraseHandler>
	<invalidCertificateHandler>
	     <name>ConsoleCertificateHandler</name>
	</invalidCertificateHandler>
	<cacheSessions>true|false</cacheSessions>
	<extendedVerification>true|false</extendedVerification>
	<requireTLSv1>true|false</requireTLSv1>
	<requireTLSv1_1>true|false</requireTLSv1_1>
	<requireTLSv1_2>true|false</requireTLSv1_2>
	<disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1,tlsv1_2</disableProtocols>
	<dhParamsFile>dh.pem</dhParamsFile>
	<ecdhCurve>prime256v1</ecdhCurve>
    """
    _entries = {
        "openSSL": {
            "client": entries
        }
    }
    if config is None:
        config = create_xml_config_content(_entries, config_file=config_file, config_d_dir=config_d_dir)

    return add_config(config, timeout=timeout, restart=restart, node=node)


@TestStep(Given)
def add_ssl_fips_configuration_file(self, value,
        config=None, config_d_dir="/etc/clickhouse-server/config.d",
        config_file="ssl_fips.xml", timeout=300, restart=False, node=None):
    """Add SSL fips configuration to config.xml.

    <yandex>
        <openSSL>
            <fips>false</fips>
        </openSSL>
    </yandex>

    :param value: either "true", or "false"
    """
    assert value in ("true", "false")

    entries = {
        "openSSL": {
            "fips": f"{value}"
        }
    }
    if config is None:
        config = create_xml_config_content(entries, config_file=config_file, config_d_dir=config_d_dir)

    return add_config(config, timeout=timeout, restart=restart, node=node)


@TestStep(Given)
def add_secure_ports_configuration_file(self, https="8443", tcp="9440",
        config=None, config_d_dir="/etc/clickhouse-server/config.d",
        config_file="ssl_ports.xml", timeout=300, restart=False, node=None):
    """Add SSL secure ports to config.xml.
    """
    entries = {
        "https_port": f"{https}",
        "tcp_port_secure": f"{tcp}"
    }
    if config is None:
        config = create_xml_config_content(entries, config_file=config_file, config_d_dir=config_d_dir)

    return add_config(config, timeout=timeout, restart=restart, node=node)


@TestStep(Given)
def create_rsa_private_key(self, outfile, passphrase, algorithm="aes256", length=2048, node=None):
    """Generate RSA private key.
    """
    bash = self.context.cluster.bash(node=node)

    if algorithm:
        algorithm = f"-{algorithm} "
    else:
        algorithm = ""

    try:
        with bash(f"openssl genrsa {algorithm}-out {outfile} {length}", name="openssl", asynchronous=True) as cmd:
            choice = cmd.app.expect(f"(Enter pass phrase for.*?:)|({bash.prompt})")
            if choice.group(2) is None:
                cmd.app.send(passphrase)
                cmd.app.expect("Verifying - Enter pass phrase for .*?:")
                cmd.app.send(passphrase)

        yield outfile

    finally:
        with Finally("I remove private key file"):
            bash(f"rm -rf \"{outfile}\"")


@TestStep(Given)
def create_ca_certificate(self, key, passphrase, common_name,
        outfile, type="x509", days="3650",
        hash="sha256", extensions="v3_ca",
        country_name="", state_or_province="", locality_name="",
        organization_name="", organization_unit_name="",
        email_address="", node=None):
    """Generate CA certificate.
    """
    bash = self.context.cluster.bash(node=node)

    try:
        with bash(f"openssl req -new -{type} -days {days} -key {key} "
                f"-{hash} -extensions {extensions} -out {outfile}", name="openssl", asynchronous=True) as cmd:
            cmd.app.expect("Enter pass phrase for.*?:")
            cmd.app.send(passphrase)
            cmd.app.expect("Country Name.*?:")
            cmd.app.send(country_name)
            cmd.app.expect("State or Province Name.*?:")
            cmd.app.send(state_or_province)
            cmd.app.expect("Locality Name.*?:")
            cmd.app.send(locality_name)
            cmd.app.expect("Organization Name.*?:")
            cmd.app.send(organization_name)
            cmd.app.expect("Organizational Unit Name.*?:")
            cmd.app.send(organization_unit_name)
            cmd.app.expect("Common Name.*?:")
            cmd.app.send(common_name)
            cmd.app.expect("Email Address.*?:")
            cmd.app.send(email_address)

        yield outfile

    finally:
        with Finally("I remove CA certificate file"):
            bash(f"rm -rf \"{outfile}\"")


@TestStep(Given)
def create_certificate_signing_request(self, outfile, key, passphrase, common_name, hash="sha256",
        country_name="", state_or_province="", locality_name="",
        organization_name="", organization_unit_name="",
        email_address="", challenge_password="", company_name="", node=None):
    """Generate certificate signing request.
    """
    bash = self.context.cluster.bash(node=node)

    try:
        with bash(f"openssl req -{hash} -new -key {key} -out {outfile}", name="openssl",
                asynchronous=True) as cmd:
            choice = cmd.app.expect("(Enter pass phrase for.*?:)|(Country Name.*?:)")
            if choice.group(1):
                cmd.app.send(passphrase)
                cmd.app.expect("Country Name.*?:")
            cmd.app.send(country_name)
            cmd.app.expect("State or Province Name.*?:")
            cmd.app.send(state_or_province)
            cmd.app.expect("Locality Name.*?:")
            cmd.app.send(locality_name)
            cmd.app.expect("Organization Name.*?:")
            cmd.app.send(organization_name)
            cmd.app.expect("Organizational Unit Name.*?:")
            cmd.app.send(organization_unit_name)
            cmd.app.expect("Common Name.*?:")
            cmd.app.send(common_name)
            cmd.app.expect("Email Address.*?:")
            cmd.app.send(email_address)
            cmd.app.expect("A challenge password.*?:")
            cmd.app.send(challenge_password)
            cmd.app.expect("An optional company name.*?:")
            cmd.app.send(company_name)


        yield outfile

    finally:
        with Finally("I remove certificate signing request file"):
            bash(f"rm -rf \"{outfile}\"")


@TestStep(Given)
def sign_certificate(self, outfile, csr, ca_certificate, ca_key, ca_passphrase,
        type="x509", hash="sha256", days="365", node=None):
    """Sign certificate using CA certificate.
    """
    bash = self.context.cluster.bash(node=node)

    try:
        with bash(f"openssl {type} -{hash} -req -in {csr} -CA {ca_certificate} "
                f"-CAkey {ca_key} -CAcreateserial -out {outfile} -days {days}",
                name="openssl", asynchronous=True) as cmd:
            cmd.app.expect("Enter pass phrase for.*?:")
            cmd.app.send(ca_passphrase)

        yield outfile

    finally:
        with Finally("I remove certificate file"):
            bash(f"rm -rf \"{outfile}\"")


@TestStep(Given)
def create_dh_params(self, outfile, length=256, node=None):
    """Generate Diffie-Hellman parameters file for the server.
    """
    bash = self.context.cluster.bash(node=node)

    try:
        cmd = bash(f"openssl dhparam -out {outfile} {length}")

        with Then("checking exitcode 0"):
            assert cmd.exitcode == 0, error()

        yield outfile

    finally:
        with Finally("I remove dhparams file"):
            bash(f"rm -rf \"{outfile}\"")


@TestStep(Then)
def validate_certificate(self, certificate, ca_certificate, node=None):
    """Validate certificate using CA certificate.
    """
    bash = self.context.cluster.bash(node=node)

    cmd = bash(f"openssl verify -CAfile {ca_certificate} {certificate}")

    with By("checking certificate was validated"):
        assert "OK" in cmd.output, error()

    with And("exitcode is 0"):
        assert cmd.exitcode == 0, error()


@TestStep(Given)
def add_trusted_ca_certificate(self, node, certificate, path=None,
        directory="/usr/local/share/ca-certificates/",
        eof="EOF", certificate_node=None):
    """Add CA certificate as trusted by the system.
    """
    bash = self.context.cluster.bash(node=certificate_node)

    if path is None:
        path = os.path.join(directory, os.path.basename(certificate))

    with By("copying certificate to node", description=f"{node}:{path}"):
        copy(dest_node=node, src_path=certificate, dest_path=path, bash=bash, eof=eof)

    with And("updating system certificates"):
        cmd = node.command("update-ca-certificates")

    with Then("checking certificate was added"):
        assert "Adding " in cmd.output, error()

    with And("exitcode is 0"):
        assert cmd.exitcode == 0, error()

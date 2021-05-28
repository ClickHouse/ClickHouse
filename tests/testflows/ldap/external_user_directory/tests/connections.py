from testflows.core import *
from testflows.asserts import error

from ldap.external_user_directory.tests.common import login
from ldap.external_user_directory.requirements import *

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_PlainText("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_No("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port_Default("1.0")
)
def plain_text(self):
    """Check that we can perform LDAP user authentication using `plain text` connection protocol.
    """
    servers = {
        "openldap1": {
            "host": "openldap1",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap1", "username": "user1", "password": "user1", "login": True}
    ]
    login(servers, "openldap1", *users)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_PlainText("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port("1.0")
)
def plain_text_with_custom_port(self):
    """Check that we can perform LDAP user authentication using `plain text` connection protocol
    with the server that uses custom port.
    """
    servers = {
        "openldap3": {
            "host": "openldap3",
            "port": "3089",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap3", "username": "user3", "password": "user3", "login": True}
    ]
    login(servers, "openldap3", *users)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port("1.0")
)
def tls_with_custom_port(self):
    """Check that we can perform LDAP user authentication using `TLS` connection protocol
    with the server that uses custom port.
    """
    servers = {
        "openldap4": {
            "host": "openldap4",
            "port": "6036",
            "tls_require_cert": "never",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap4", "username": "user4", "password": "user4", "login": True}
    ]
    login(servers, "openldap4", *users)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_StartTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port("1.0")
)
def starttls_with_custom_port(self):
    """Check that we can perform LDAP user authentication using `StartTLS` connection protocol
    with the server that uses custom port.
    """
    servers = {
        "openldap4": {
            "host": "openldap4",
            "port": "3089",
            "enable_tls": "starttls",
            "tls_require_cert": "never",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap4", "username": "user4", "password": "user4", "login": True}
    ]
    login(servers, "openldap4", *users)

def tls_connection(enable_tls, tls_require_cert):
    """Try to login using LDAP user authentication over a TLS connection."""
    servers = {
        "openldap2": {
            "host": "openldap2",
            "enable_tls": enable_tls,
            "tls_require_cert": tls_require_cert,
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap2", "username": "user2", "password": "user2", "login": True}
    ]

    requirements = []

    if tls_require_cert == "never":
        requirements = [RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Never("1.0")]
    elif tls_require_cert == "allow":
        requirements = [RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Allow("1.0")]
    elif tls_require_cert == "try":
        requirements = [RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Try("1.0")]
    elif tls_require_cert == "demand":
        requirements = [RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Demand("1.0")]

    with Example(name=f"tls_require_cert='{tls_require_cert}'", requirements=requirements):
        login(servers, "openldap2", *users)

@TestScenario
@Examples("enable_tls tls_require_cert", [
    ("yes", "never"),
    ("yes", "allow"),
    ("yes", "try"),
    ("yes", "demand")
])
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Yes("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port_Default("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Default("1.0")
)
def tls(self):
    """Check that we can perform LDAP user authentication using `TLS` connection protocol.
    """
    for example in self.examples:
        tls_connection(*example)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Default("1.0")
)
def tls_enable_tls_default_yes(self):
    """Check that the default value for the `enable_tls` is set to `yes`."""
    servers = {
        "openldap2": {
            "host": "openldap2",
            "tls_require_cert": "never",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap2", "username": "user2", "password": "user2", "login": True}
    ]
    login(servers, "openldap2", *users)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Default("1.0")
)
def tls_require_cert_default_demand(self):
    """Check that the default value for the `tls_require_cert` is set to `demand`."""
    servers = {
        "openldap2": {
            "host": "openldap2",
            "enable_tls": "yes",
            "port": "636",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap2", "username": "user2", "password": "user2", "login": True}
    ]
    login(servers, "openldap2", *users)

@TestScenario
@Examples("enable_tls tls_require_cert", [
    ("starttls", "never"),
    ("starttls", "allow"),
    ("starttls", "try"),
    ("starttls", "demand")
])
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_StartTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_StartTLS("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port_Default("1.0")
)
def starttls(self):
    """Check that we can perform LDAP user authentication using legacy `StartTLS` connection protocol.
    """
    for example in self.examples:
        tls_connection(*example)

@TestScenario
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCipherSuite("1.0")
)
def tls_cipher_suite(self):
    """Check that `tls_cipher_suite` parameter can be used specify allowed cipher suites."""
    servers = {
        "openldap4": {
            "host": "openldap4",
            "port": "6036",
            "tls_require_cert": "never",
            "tls_cipher_suite": "SECURE256:+SECURE128:-VERS-TLS-ALL:+VERS-TLS1.2:-RSA:-DHE-DSS:-CAMELLIA-128-CBC:-CAMELLIA-256-CBC",
            "tls_minimum_protocol_version": "tls1.2",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }
    users = [
        {"server": "openldap4", "username": "user4", "password": "user4", "login": True}
    ]
    login(servers, "openldap4", *users)

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion("1.0"),
    RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Values("1.0")
)
@Examples("version exitcode message", [
    ("ssl2", None, None),
    ("ssl3", None, None),
    ("tls1.0", None, None),
    ("tls1.1", None, None),
    ("tls1.2", None, None)
])
def tls_minimum_protocol_version(self, version, exitcode, message):
    """Check that `tls_minimum_protocol_version` parameter can be used specify
    to specify the minimum protocol version of SSL/TLS."""

    servers = {
        "openldap4": {
            "host": "openldap4",
            "port": "6036",
            "tls_require_cert": "never",
            "tls_minimum_protocol_version": version,
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }

    users = [{
        "server": "openldap4", "username": "user4", "password": "user4",
        "login": True, "exitcode": int(exitcode) if exitcode is not None else None, "message": message
    }]

    login(servers,"openldap4", *users)

@TestFeature
@Name("connection protocols")
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()

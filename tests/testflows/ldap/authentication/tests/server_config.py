from testflows.core import *

from ldap.authentication.tests.common import *
from ldap.authentication.requirements import *

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_Server_Name("1.0")
)
def empty_server_name(self, timeout=20):
    """Check that empty string as a server name is not allowed.
    """
    servers = {"": {"host": "foo", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    invalid_server_config(servers, timeout=timeout)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_UnreachableServer("1.0")
)
def invalid_host(self):
    """Check that server returns an error when LDAP server
    host name is invalid.
    """
    servers = {"foo": {"host": "foo", "port": "389", "enable_tls": "no"}}
    users = [{
        "server": "foo", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_Server_Host("1.0")
)
def empty_host(self):
    """Check that server returns an error when LDAP server
    host value is empty.
    """
    servers = {"foo": {"host": "", "port": "389", "enable_tls": "no"}}
    users = [{
        "server": "foo", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_Server_Host("1.0")
)
def missing_host(self):
    """Check that server returns an error when LDAP server
    host is missing.
    """
    servers = {"foo": {"port": "389", "enable_tls": "no"}}
    users = [{
        "server": "foo", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def invalid_port(self):
    """Check that server returns an error when LDAP server
    port is not valid.
    """
    servers = {"openldap1": {"host": "openldap1", "port": "3890", "enable_tls": "no"}}
    users = [{
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)


@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def invalid_auth_dn_prefix(self):
    """Check that server returns an error when LDAP server
    port is not valid.
    """
    servers = {"openldap1": {"host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "foo=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def invalid_auth_dn_suffix(self):
    """Check that server returns an error when LDAP server
    port is not valid.
    """
    servers = {"openldap1": {"host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",foo=users,dc=company,dc=com"
    }}
    users = [{
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def invalid_enable_tls_value(self):
    """Check that server returns an error when enable_tls
    option has invalid value.
    """
    servers = {"openldap1": {"host": "openldap1", "port": "389", "enable_tls": "foo",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def invalid_tls_require_cert_value(self):
    """Check that server returns an error when tls_require_cert
    option has invalid value.
    """
    servers = {"openldap2": {
        "host": "openldap2", "port": "636", "enable_tls": "yes",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        "tls_require_cert": "foo",
        "ca_cert_dir": "/container/service/slapd/assets/certs/",
        "ca_cert_file": "/container/service/slapd/assets/certs/ca.crt"
    }}
    users = [{
        "server": "openldap2", "username": "user2", "password": "user2", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def empty_ca_cert_dir(self):
    """Check that server returns an error when ca_cert_dir is empty.
    """
    servers = {"openldap2": {"host": "openldap2", "port": "636", "enable_tls": "yes",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        "tls_require_cert": "demand",
        "ca_cert_dir": "",
        "ca_cert_file": "/container/service/slapd/assets/certs/ca.crt"
    }}
    users = [{
        "server": "openldap2", "username": "user2", "password": "user2", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Server_Configuration_Invalid("1.0")
)
def empty_ca_cert_file(self):
    """Check that server returns an error when ca_cert_file is empty.
    """
    servers = {"openldap2": {"host": "openldap2", "port": "636", "enable_tls": "yes",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        "tls_require_cert": "demand",
        "ca_cert_dir": "/container/service/slapd/assets/certs/",
        "ca_cert_file": ""
    }}
    users = [{
        "server": "openldap2", "username": "user2", "password": "user2", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user2: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Value("1.0"),
    RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Prefix("1.0"),
    RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Suffix("1.0")
)
def auth_dn_value(self):
    """Check that server configuration can properly define the `dn` value of the user."""
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }}
    user = {"server": "openldap1", "username": "user1", "password": "user1", "login": True}

    login(servers, user)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Configuration_Server_Syntax("1.0")
)
def syntax(self):
    """Check that server configuration with valid syntax can be loaded.
    ```xml
    <yandex>
        <ldap_server>
            <host>localhost</host>
            <port>636</port>
            <auth_dn_prefix>cn=</auth_dn_prefix>
            <auth_dn_suffix>, ou=users, dc=example, dc=com</auth_dn_suffix>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </ldap_server>
    </yandex>
    ```
    """
    servers = {
        "openldap2": {
            "host": "openldap2",
            "port": "389",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "enable_tls": "yes",
            "tls_minimum_protocol_version": "tls1.2" ,
            "tls_require_cert": "demand",
            "tls_cert_file": "/container/service/slapd/assets/certs/ldap.crt",
            "tls_key_file": "/container/service/slapd/assets/certs/ldap.key",
            "tls_ca_cert_file": "/container/service/slapd/assets/certs/ca.crt",
            "tls_ca_cert_dir": "/container/service/slapd/assets/certs/",
            "tls_cipher_suite": "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384"
        }
    }
    with ldap_servers(servers):
        pass

@TestFeature
@Name("server config")
def feature(self, node="clickhouse1"):
    """Check that LDAP server configuration.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()

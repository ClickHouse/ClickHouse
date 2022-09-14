# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.201216.1172002.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_007_LDAP_Authentication = Requirement(
    name="RQ.SRS-007.LDAP.Authentication",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support user authentication via an [LDAP] server.\n" "\n"
    ),
    link=None,
    level=3,
    num="4.1.1",
)

RQ_SRS_007_LDAP_Authentication_MultipleServers = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.MultipleServers",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying multiple [LDAP] servers that can be used to authenticate\n"
        "users.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.2",
)

RQ_SRS_007_LDAP_Authentication_Protocol_PlainText = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Protocol.PlainText",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support user authentication using plain text `ldap://` non secure protocol.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.3",
)

RQ_SRS_007_LDAP_Authentication_Protocol_TLS = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Protocol.TLS",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support user authentication using `SSL/TLS` `ldaps://` secure protocol.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.4",
)

RQ_SRS_007_LDAP_Authentication_Protocol_StartTLS = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Protocol.StartTLS",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support user authentication using legacy `StartTLS` protocol which is a\n"
        "plain text `ldap://` protocol that is upgraded to [TLS].\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.5",
)

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_Validation = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.Validation",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support certificate validation used for [TLS] connections.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.6",
)

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SelfSigned = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SelfSigned",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support self-signed certificates for [TLS] connections.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.7",
)

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SpecificCertificationAuthority = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SpecificCertificationAuthority",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support certificates signed by specific Certification Authority for [TLS] connections.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.8",
)

RQ_SRS_007_LDAP_Server_Configuration_Invalid = Requirement(
    name="RQ.SRS-007.LDAP.Server.Configuration.Invalid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server configuration is not valid.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.9",
)

RQ_SRS_007_LDAP_User_Configuration_Invalid = Requirement(
    name="RQ.SRS-007.LDAP.User.Configuration.Invalid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit user login if user configuration is not valid.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.10",
)

RQ_SRS_007_LDAP_Authentication_Mechanism_Anonymous = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Mechanism.Anonymous",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication using [Anonymous Authentication Mechanism of Simple Bind]\n"
        "authentication mechanism.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.11",
)

RQ_SRS_007_LDAP_Authentication_Mechanism_Unauthenticated = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Mechanism.Unauthenticated",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication using [Unauthenticated Authentication Mechanism of Simple Bind]\n"
        "authentication mechanism.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.12",
)

RQ_SRS_007_LDAP_Authentication_Mechanism_NamePassword = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Mechanism.NamePassword",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL allow authentication using only [Name/Password Authentication Mechanism of Simple Bind]\n"
        "authentication mechanism.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.13",
)

RQ_SRS_007_LDAP_Authentication_Valid = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Valid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL only allow user authentication using [LDAP] server if and only if\n"
        "user name and password match [LDAP] server records for the user.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.14",
)

RQ_SRS_007_LDAP_Authentication_Invalid = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Invalid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication if either user name or password\n"
        "do not match [LDAP] server records for the user.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.15",
)

RQ_SRS_007_LDAP_Authentication_Invalid_DeletedUser = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Invalid.DeletedUser",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication if the user\n"
        "has been deleted from the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.16",
)

RQ_SRS_007_LDAP_Authentication_UsernameChanged = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.UsernameChanged",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication if the username is changed\n"
        "on the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.17",
)

RQ_SRS_007_LDAP_Authentication_PasswordChanged = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.PasswordChanged",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit authentication if the password\n"
        "for the user is changed on the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.18",
)

RQ_SRS_007_LDAP_Authentication_LDAPServerRestart = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.LDAPServerRestart",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support authenticating users after [LDAP] server is restarted.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.19",
)

RQ_SRS_007_LDAP_Authentication_ClickHouseServerRestart = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.ClickHouseServerRestart",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support authenticating users after server is restarted.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.20",
)

RQ_SRS_007_LDAP_Authentication_Parallel = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Parallel",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support parallel authentication of users using [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.21",
)

RQ_SRS_007_LDAP_Authentication_Parallel_ValidAndInvalid = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Parallel.ValidAndInvalid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support authentication of valid users and\n"
        "prohibit authentication of invalid users using [LDAP] server\n"
        "in parallel without having invalid attempts affecting valid authentications.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.1.22",
)

RQ_SRS_007_LDAP_UnreachableServer = Requirement(
    name="RQ.SRS-007.LDAP.UnreachableServer",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server is unreachable.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.1",
)

RQ_SRS_007_LDAP_Configuration_Server_Name = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.Name",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL not support empty string as a server name.\n" "\n"
    ),
    link=None,
    level=3,
    num="4.2.2",
)

RQ_SRS_007_LDAP_Configuration_Server_Host = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.Host",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<host>` parameter to specify [LDAP]\n"
        "server hostname or IP, this parameter SHALL be mandatory and SHALL not be empty.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.3",
)

RQ_SRS_007_LDAP_Configuration_Server_Port = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.Port",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<port>` parameter to specify [LDAP] server port.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.4",
)

RQ_SRS_007_LDAP_Configuration_Server_Port_Default = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.Port.Default",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL use default port number `636` if `enable_tls` is set to `yes` or `389` otherwise.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.5",
)

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Prefix = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Prefix",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<auth_dn_prefix>` parameter to specify the prefix\n"
        "of value used to construct the DN to bound to during authentication via [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.6",
)

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Suffix = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Suffix",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<auth_dn_suffix>` parameter to specify the suffix\n"
        "of value used to construct the DN to bound to during authentication via [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.7",
)

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Value = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Value",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL construct DN as  `auth_dn_prefix + escape(user_name) + auth_dn_suffix` string.\n"
        "\n"
        "> This implies that auth_dn_suffix should usually have comma ',' as its first non-space character.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.8",
)

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<enable_tls>` parameter to trigger the use of secure connection to the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.9",
)

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Default = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Default",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL use `yes` value as the default for `<enable_tls>` parameter\n"
        "to enable SSL/TLS `ldaps://` protocol.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.10",
)

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_No = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.No",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `no` as the value of `<enable_tls>` parameter to enable\n"
        "plain text `ldap://` protocol.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.11",
)

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Yes = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Yes",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `yes` as the value of `<enable_tls>` parameter to enable\n"
        "SSL/TLS `ldaps://` protocol.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.12",
)

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_StartTLS = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.StartTLS",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `starttls` as the value of `<enable_tls>` parameter to enable\n"
        "legacy `StartTLS` protocol that used plain text `ldap://` protocol, upgraded to [TLS].\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.13",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_minimum_protocol_version>` parameter to specify\n"
        "the minimum protocol version of SSL/TLS.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.14",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Values = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Values",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, and `tls1.2`\n"
        "as a value of the `<tls_minimum_protocol_version>` parameter.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.15",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Default = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Default",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL set `tls1.2` as the default value of the `<tls_minimum_protocol_version>` parameter.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.16",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_require_cert>` parameter to specify [TLS] peer\n"
        "certificate verification behavior.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.17",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Default = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Default",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL use `demand` value as the default for the `<tls_require_cert>` parameter.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.18",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Demand = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Demand",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `demand` as the value of `<tls_require_cert>` parameter to\n"
        "enable requesting of client certificate.  If no certificate  is  provided,  or  a  bad   certificate   is\n"
        "provided, the session SHALL be immediately terminated.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.19",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Allow = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Allow",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `allow` as the value of `<tls_require_cert>` parameter to\n"
        "enable requesting of client certificate. If no\n"
        "certificate is provided, the session SHALL proceed normally.\n"
        "If a bad certificate is provided, it SHALL be ignored and the session SHALL proceed normally.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.20",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Try = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Try",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `try` as the value of `<tls_require_cert>` parameter to\n"
        "enable requesting of client certificate. If no certificate is provided, the session\n"
        "SHALL proceed  normally.  If a bad certificate is provided, the session SHALL be\n"
        "immediately terminated.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.21",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Never = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Never",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying `never` as the value of `<tls_require_cert>` parameter to\n"
        "disable requesting of client certificate.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.22",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSCertFile = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSCertFile",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_cert_file>` to specify the path to certificate file used by\n"
        "[ClickHouse] to establish connection with the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.23",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSKeyFile = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSKeyFile",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_key_file>` to specify the path to key file for the certificate\n"
        "specified by the `<tls_cert_file>` parameter.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.24",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSCACertDir = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSCACertDir",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_ca_cert_dir>` parameter to specify to a path to\n"
        "the directory containing [CA] certificates used to verify certificates provided by the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.25",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSCACertFile = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSCACertFile",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `<tls_ca_cert_file>` parameter to specify a path to a specific\n"
        "[CA] certificate file used to verify certificates provided by the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.26",
)

RQ_SRS_007_LDAP_Configuration_Server_TLSCipherSuite = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.TLSCipherSuite",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `tls_cipher_suite` parameter to specify allowed cipher suites.\n"
        "The value SHALL use the same format as the `ciphersuites` in the [OpenSSL Ciphers].\n"
        "\n"
        "For example,\n"
        "\n"
        "```xml\n"
        "<tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>\n"
        "```\n"
        "\n"
        "The available suites SHALL depend on the [OpenSSL] library version and variant used to build\n"
        "[ClickHouse] and therefore might change.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.27",
)

RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support `verification_cooldown` parameter in the [LDAP] server configuration section\n"
        "that SHALL define a period of time, in seconds, after a successful bind attempt, during which a user SHALL be assumed\n"
        "to be successfully authenticated for all consecutive requests without contacting the [LDAP] server.\n"
        "After period of time since the last successful attempt expires then on the authentication attempt\n"
        "SHALL result in contacting the [LDAP] server to verify the username and password. \n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.28",
)

RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown_Default = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Default",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] `verification_cooldown` parameter in the [LDAP] server configuration section\n"
        "SHALL have a default value of `0` that disables caching and forces contacting\n"
        "the [LDAP] server for each authentication request.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.29",
)

RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown_Invalid = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Invalid",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[Clickhouse] SHALL return an error if the value provided for the `verification_cooldown` parameter is not a valid positive integer.\n"
        "\n"
        "For example:\n"
        "\n"
        "* negative integer\n"
        "* string\n"
        "* empty value\n"
        "* extremely large positive value (overflow)\n"
        "* extremely large negative value (overflow)\n"
        "\n"
        "The error SHALL appear in the log and SHALL be similar to the following:\n"
        "\n"
        "```bash\n"
        "<Error> Access(user directories): Could not parse LDAP server `openldap1`: Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Syntax error: Not a valid unsigned integer: *input value*\n"
        "```\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.30",
)

RQ_SRS_007_LDAP_Configuration_Server_Syntax = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.Server.Syntax",
    version="2.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support the following example syntax to create an entry for an [LDAP] server inside the `config.xml`\n"
        "configuration file or of any configuration file inside the `config.d` directory.\n"
        "\n"
        "```xml\n"
        "<clickhouse>\n"
        "    <my_ldap_server>\n"
        "        <host>localhost</host>\n"
        "        <port>636</port>\n"
        "        <auth_dn_prefix>cn=</auth_dn_prefix>\n"
        "        <auth_dn_suffix>, ou=users, dc=example, dc=com</auth_dn_suffix>\n"
        "        <verification_cooldown>0</verification_cooldown>\n"
        "        <enable_tls>yes</enable_tls>\n"
        "        <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>\n"
        "        <tls_require_cert>demand</tls_require_cert>\n"
        "        <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>\n"
        "        <tls_key_file>/path/to/tls_key_file</tls_key_file>\n"
        "        <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>\n"
        "        <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>\n"
        "        <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>\n"
        "    </my_ldap_server>\n"
        "</clickhouse>\n"
        "```\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.31",
)

RQ_SRS_007_LDAP_Configuration_User_RBAC = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.RBAC",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support creating users identified using an [LDAP] server using\n"
        "the following RBAC command\n"
        "\n"
        "```sql\n"
        "CREATE USER name IDENTIFIED WITH ldap SERVER 'server_name'\n"
        "```\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.32",
)

RQ_SRS_007_LDAP_Configuration_User_Syntax = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.Syntax",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support the following example syntax to create a user that is authenticated using\n"
        "an [LDAP] server inside the `users.xml` file or any configuration file inside the `users.d` directory.\n"
        "\n"
        "```xml\n"
        "<clickhouse>\n"
        "    <users>\n"
        "        <user_name>\n"
        "            <ldap>\n"
        "                <server>my_ldap_server</server>\n"
        "            </ldap>\n"
        "        </user_name>\n"
        "    </users>\n"
        "</clickhouse>\n"
        "```\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.33",
)

RQ_SRS_007_LDAP_Configuration_User_Name_Empty = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.Name.Empty",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=("[ClickHouse] SHALL not support empty string as a user name.\n" "\n"),
    link=None,
    level=3,
    num="4.2.34",
)

RQ_SRS_007_LDAP_Configuration_User_BothPasswordAndLDAP = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.BothPasswordAndLDAP",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL throw an error if `<ldap>` is specified for the user and at the same\n"
        "time user configuration contains any of the `<password*>` entries.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.35",
)

RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_NotDefined = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.NotDefined",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL throw an error during any authentication attempt\n"
        "if the name of the [LDAP] server used inside the `<ldap>` entry\n"
        "is not defined in the `<ldap_servers>` section.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.36",
)

RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_Empty = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.Empty",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL throw an error during any authentication attempt\n"
        "if the name of the [LDAP] server used inside the `<ldap>` entry\n"
        "is empty.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.37",
)

RQ_SRS_007_LDAP_Configuration_User_OnlyOneServer = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.OnlyOneServer",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying only one [LDAP] server for a given user.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.38",
)

RQ_SRS_007_LDAP_Configuration_User_Name_Long = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.Name.Long",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support long user names of at least 256 bytes\n"
        "to specify users that can be authenticated using an [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.39",
)

RQ_SRS_007_LDAP_Configuration_User_Name_UTF8 = Requirement(
    name="RQ.SRS-007.LDAP.Configuration.User.Name.UTF8",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support user names that contain [UTF-8] characters.\n" "\n"
    ),
    link=None,
    level=3,
    num="4.2.40",
)

RQ_SRS_007_LDAP_Authentication_Username_Empty = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Username.Empty",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL not support authenticating users with empty username.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.41",
)

RQ_SRS_007_LDAP_Authentication_Username_Long = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Username.Long",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support authenticating users with a long username of at least 256 bytes.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.42",
)

RQ_SRS_007_LDAP_Authentication_Username_UTF8 = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Username.UTF8",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support authentication users with a username that contains [UTF-8] characters.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.43",
)

RQ_SRS_007_LDAP_Authentication_Password_Empty = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Password.Empty",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL not support authenticating users with empty passwords\n"
        "even if an empty password is valid for the user and\n"
        "is allowed by the [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.44",
)

RQ_SRS_007_LDAP_Authentication_Password_Long = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Password.Long",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support long password of at least 256 bytes\n"
        "that can be used to authenticate users using an [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.45",
)

RQ_SRS_007_LDAP_Authentication_Password_UTF8 = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.Password.UTF8",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support [UTF-8] characters in passwords\n"
        "used to authenticate users using an [LDAP] server.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.46",
)

RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Performance = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Performance",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL provide better login performance of [LDAP] authenticated users\n"
        "when `verification_cooldown` parameter is set to a positive value when comparing\n"
        "to the the case when `verification_cooldown` is turned off either for a single user or multiple users\n"
        "making a large number of repeated requests.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.47",
)

RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL reset any currently cached [LDAP] authentication bind requests enabled by the\n"
        "`verification_cooldown` parameter in the [LDAP] server configuration section\n"
        "if either `host`, `port`, `auth_dn_prefix`, or `auth_dn_suffix` parameter values\n"
        "change in the configuration file. The reset SHALL cause any subsequent authentication attempts for any user\n"
        "to result in contacting the [LDAP] server to verify user's username and password.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.48",
)

RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_InvalidPassword = Requirement(
    name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.InvalidPassword",
    version="1.0",
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL reset current cached [LDAP] authentication bind request enabled by the\n"
        "`verification_cooldown` parameter in the [LDAP] server configuration section\n"
        "for the user if the password provided in the current authentication attempt does not match\n"
        "the valid password provided during the first successful authentication request that was cached\n"
        "for this exact user. The reset SHALL cause the next authentication attempt for this user\n"
        "to result in contacting the [LDAP] server to verify user's username and password.\n"
        "\n"
    ),
    link=None,
    level=3,
    num="4.2.49",
)

SRS_007_ClickHouse_Authentication_of_Users_via_LDAP = Specification(
    name="SRS-007 ClickHouse Authentication of Users via LDAP",
    description=None,
    author=None,
    date=None,
    status=None,
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name="Revision History", level=1, num="1"),
        Heading(name="Introduction", level=1, num="2"),
        Heading(name="Terminology", level=1, num="3"),
        Heading(name="Requirements", level=1, num="4"),
        Heading(name="Generic", level=2, num="4.1"),
        Heading(name="RQ.SRS-007.LDAP.Authentication", level=3, num="4.1.1"),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.MultipleServers", level=3, num="4.1.2"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Protocol.PlainText",
            level=3,
            num="4.1.3",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Protocol.TLS", level=3, num="4.1.4"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Protocol.StartTLS",
            level=3,
            num="4.1.5",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.Validation",
            level=3,
            num="4.1.6",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SelfSigned",
            level=3,
            num="4.1.7",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SpecificCertificationAuthority",
            level=3,
            num="4.1.8",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Server.Configuration.Invalid", level=3, num="4.1.9"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.User.Configuration.Invalid", level=3, num="4.1.10"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Mechanism.Anonymous",
            level=3,
            num="4.1.11",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Mechanism.Unauthenticated",
            level=3,
            num="4.1.12",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Mechanism.NamePassword",
            level=3,
            num="4.1.13",
        ),
        Heading(name="RQ.SRS-007.LDAP.Authentication.Valid", level=3, num="4.1.14"),
        Heading(name="RQ.SRS-007.LDAP.Authentication.Invalid", level=3, num="4.1.15"),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Invalid.DeletedUser",
            level=3,
            num="4.1.16",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.UsernameChanged", level=3, num="4.1.17"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.PasswordChanged", level=3, num="4.1.18"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.LDAPServerRestart",
            level=3,
            num="4.1.19",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.ClickHouseServerRestart",
            level=3,
            num="4.1.20",
        ),
        Heading(name="RQ.SRS-007.LDAP.Authentication.Parallel", level=3, num="4.1.21"),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Parallel.ValidAndInvalid",
            level=3,
            num="4.1.22",
        ),
        Heading(name="Specific", level=2, num="4.2"),
        Heading(name="RQ.SRS-007.LDAP.UnreachableServer", level=3, num="4.2.1"),
        Heading(name="RQ.SRS-007.LDAP.Configuration.Server.Name", level=3, num="4.2.2"),
        Heading(name="RQ.SRS-007.LDAP.Configuration.Server.Host", level=3, num="4.2.3"),
        Heading(name="RQ.SRS-007.LDAP.Configuration.Server.Port", level=3, num="4.2.4"),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.Port.Default",
            level=3,
            num="4.2.5",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Prefix",
            level=3,
            num="4.2.6",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Suffix",
            level=3,
            num="4.2.7",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Value",
            level=3,
            num="4.2.8",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS", level=3, num="4.2.9"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Default",
            level=3,
            num="4.2.10",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.No",
            level=3,
            num="4.2.11",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Yes",
            level=3,
            num="4.2.12",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.StartTLS",
            level=3,
            num="4.2.13",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion",
            level=3,
            num="4.2.14",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Values",
            level=3,
            num="4.2.15",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Default",
            level=3,
            num="4.2.16",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert",
            level=3,
            num="4.2.17",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Default",
            level=3,
            num="4.2.18",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Demand",
            level=3,
            num="4.2.19",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Allow",
            level=3,
            num="4.2.20",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Try",
            level=3,
            num="4.2.21",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Never",
            level=3,
            num="4.2.22",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSCertFile",
            level=3,
            num="4.2.23",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSKeyFile",
            level=3,
            num="4.2.24",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSCACertDir",
            level=3,
            num="4.2.25",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSCACertFile",
            level=3,
            num="4.2.26",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.TLSCipherSuite",
            level=3,
            num="4.2.27",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown",
            level=3,
            num="4.2.28",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Default",
            level=3,
            num="4.2.29",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Invalid",
            level=3,
            num="4.2.30",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.Server.Syntax", level=3, num="4.2.31"
        ),
        Heading(name="RQ.SRS-007.LDAP.Configuration.User.RBAC", level=3, num="4.2.32"),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.Syntax", level=3, num="4.2.33"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.Name.Empty", level=3, num="4.2.34"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.BothPasswordAndLDAP",
            level=3,
            num="4.2.35",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.NotDefined",
            level=3,
            num="4.2.36",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.Empty",
            level=3,
            num="4.2.37",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.OnlyOneServer",
            level=3,
            num="4.2.38",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.Name.Long", level=3, num="4.2.39"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Configuration.User.Name.UTF8", level=3, num="4.2.40"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Username.Empty", level=3, num="4.2.41"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Username.Long", level=3, num="4.2.42"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Username.UTF8", level=3, num="4.2.43"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Password.Empty", level=3, num="4.2.44"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Password.Long", level=3, num="4.2.45"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.Password.UTF8", level=3, num="4.2.46"
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Performance",
            level=3,
            num="4.2.47",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters",
            level=3,
            num="4.2.48",
        ),
        Heading(
            name="RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.InvalidPassword",
            level=3,
            num="4.2.49",
        ),
        Heading(name="References", level=1, num="5"),
    ),
    requirements=(
        RQ_SRS_007_LDAP_Authentication,
        RQ_SRS_007_LDAP_Authentication_MultipleServers,
        RQ_SRS_007_LDAP_Authentication_Protocol_PlainText,
        RQ_SRS_007_LDAP_Authentication_Protocol_TLS,
        RQ_SRS_007_LDAP_Authentication_Protocol_StartTLS,
        RQ_SRS_007_LDAP_Authentication_TLS_Certificate_Validation,
        RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SelfSigned,
        RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SpecificCertificationAuthority,
        RQ_SRS_007_LDAP_Server_Configuration_Invalid,
        RQ_SRS_007_LDAP_User_Configuration_Invalid,
        RQ_SRS_007_LDAP_Authentication_Mechanism_Anonymous,
        RQ_SRS_007_LDAP_Authentication_Mechanism_Unauthenticated,
        RQ_SRS_007_LDAP_Authentication_Mechanism_NamePassword,
        RQ_SRS_007_LDAP_Authentication_Valid,
        RQ_SRS_007_LDAP_Authentication_Invalid,
        RQ_SRS_007_LDAP_Authentication_Invalid_DeletedUser,
        RQ_SRS_007_LDAP_Authentication_UsernameChanged,
        RQ_SRS_007_LDAP_Authentication_PasswordChanged,
        RQ_SRS_007_LDAP_Authentication_LDAPServerRestart,
        RQ_SRS_007_LDAP_Authentication_ClickHouseServerRestart,
        RQ_SRS_007_LDAP_Authentication_Parallel,
        RQ_SRS_007_LDAP_Authentication_Parallel_ValidAndInvalid,
        RQ_SRS_007_LDAP_UnreachableServer,
        RQ_SRS_007_LDAP_Configuration_Server_Name,
        RQ_SRS_007_LDAP_Configuration_Server_Host,
        RQ_SRS_007_LDAP_Configuration_Server_Port,
        RQ_SRS_007_LDAP_Configuration_Server_Port_Default,
        RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Prefix,
        RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Suffix,
        RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Value,
        RQ_SRS_007_LDAP_Configuration_Server_EnableTLS,
        RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Default,
        RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_No,
        RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Yes,
        RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_StartTLS,
        RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion,
        RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Values,
        RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Default,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Default,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Demand,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Allow,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Try,
        RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Never,
        RQ_SRS_007_LDAP_Configuration_Server_TLSCertFile,
        RQ_SRS_007_LDAP_Configuration_Server_TLSKeyFile,
        RQ_SRS_007_LDAP_Configuration_Server_TLSCACertDir,
        RQ_SRS_007_LDAP_Configuration_Server_TLSCACertFile,
        RQ_SRS_007_LDAP_Configuration_Server_TLSCipherSuite,
        RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown,
        RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown_Default,
        RQ_SRS_007_LDAP_Configuration_Server_VerificationCooldown_Invalid,
        RQ_SRS_007_LDAP_Configuration_Server_Syntax,
        RQ_SRS_007_LDAP_Configuration_User_RBAC,
        RQ_SRS_007_LDAP_Configuration_User_Syntax,
        RQ_SRS_007_LDAP_Configuration_User_Name_Empty,
        RQ_SRS_007_LDAP_Configuration_User_BothPasswordAndLDAP,
        RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_NotDefined,
        RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_Empty,
        RQ_SRS_007_LDAP_Configuration_User_OnlyOneServer,
        RQ_SRS_007_LDAP_Configuration_User_Name_Long,
        RQ_SRS_007_LDAP_Configuration_User_Name_UTF8,
        RQ_SRS_007_LDAP_Authentication_Username_Empty,
        RQ_SRS_007_LDAP_Authentication_Username_Long,
        RQ_SRS_007_LDAP_Authentication_Username_UTF8,
        RQ_SRS_007_LDAP_Authentication_Password_Empty,
        RQ_SRS_007_LDAP_Authentication_Password_Long,
        RQ_SRS_007_LDAP_Authentication_Password_UTF8,
        RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Performance,
        RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters,
        RQ_SRS_007_LDAP_Authentication_VerificationCooldown_Reset_InvalidPassword,
    ),
    content="""
# SRS-007 ClickHouse Authentication of Users via LDAP
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
    * 4.1.1 [RQ.SRS-007.LDAP.Authentication](#rqsrs-007ldapauthentication)
    * 4.1.2 [RQ.SRS-007.LDAP.Authentication.MultipleServers](#rqsrs-007ldapauthenticationmultipleservers)
    * 4.1.3 [RQ.SRS-007.LDAP.Authentication.Protocol.PlainText](#rqsrs-007ldapauthenticationprotocolplaintext)
    * 4.1.4 [RQ.SRS-007.LDAP.Authentication.Protocol.TLS](#rqsrs-007ldapauthenticationprotocoltls)
    * 4.1.5 [RQ.SRS-007.LDAP.Authentication.Protocol.StartTLS](#rqsrs-007ldapauthenticationprotocolstarttls)
    * 4.1.6 [RQ.SRS-007.LDAP.Authentication.TLS.Certificate.Validation](#rqsrs-007ldapauthenticationtlscertificatevalidation)
    * 4.1.7 [RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SelfSigned](#rqsrs-007ldapauthenticationtlscertificateselfsigned)
    * 4.1.8 [RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SpecificCertificationAuthority](#rqsrs-007ldapauthenticationtlscertificatespecificcertificationauthority)
    * 4.1.9 [RQ.SRS-007.LDAP.Server.Configuration.Invalid](#rqsrs-007ldapserverconfigurationinvalid)
    * 4.1.10 [RQ.SRS-007.LDAP.User.Configuration.Invalid](#rqsrs-007ldapuserconfigurationinvalid)
    * 4.1.11 [RQ.SRS-007.LDAP.Authentication.Mechanism.Anonymous](#rqsrs-007ldapauthenticationmechanismanonymous)
    * 4.1.12 [RQ.SRS-007.LDAP.Authentication.Mechanism.Unauthenticated](#rqsrs-007ldapauthenticationmechanismunauthenticated)
    * 4.1.13 [RQ.SRS-007.LDAP.Authentication.Mechanism.NamePassword](#rqsrs-007ldapauthenticationmechanismnamepassword)
    * 4.1.14 [RQ.SRS-007.LDAP.Authentication.Valid](#rqsrs-007ldapauthenticationvalid)
    * 4.1.15 [RQ.SRS-007.LDAP.Authentication.Invalid](#rqsrs-007ldapauthenticationinvalid)
    * 4.1.16 [RQ.SRS-007.LDAP.Authentication.Invalid.DeletedUser](#rqsrs-007ldapauthenticationinvaliddeleteduser)
    * 4.1.17 [RQ.SRS-007.LDAP.Authentication.UsernameChanged](#rqsrs-007ldapauthenticationusernamechanged)
    * 4.1.18 [RQ.SRS-007.LDAP.Authentication.PasswordChanged](#rqsrs-007ldapauthenticationpasswordchanged)
    * 4.1.19 [RQ.SRS-007.LDAP.Authentication.LDAPServerRestart](#rqsrs-007ldapauthenticationldapserverrestart)
    * 4.1.20 [RQ.SRS-007.LDAP.Authentication.ClickHouseServerRestart](#rqsrs-007ldapauthenticationclickhouseserverrestart)
    * 4.1.21 [RQ.SRS-007.LDAP.Authentication.Parallel](#rqsrs-007ldapauthenticationparallel)
    * 4.1.22 [RQ.SRS-007.LDAP.Authentication.Parallel.ValidAndInvalid](#rqsrs-007ldapauthenticationparallelvalidandinvalid)
  * 4.2 [Specific](#specific)
    * 4.2.1 [RQ.SRS-007.LDAP.UnreachableServer](#rqsrs-007ldapunreachableserver)
    * 4.2.2 [RQ.SRS-007.LDAP.Configuration.Server.Name](#rqsrs-007ldapconfigurationservername)
    * 4.2.3 [RQ.SRS-007.LDAP.Configuration.Server.Host](#rqsrs-007ldapconfigurationserverhost)
    * 4.2.4 [RQ.SRS-007.LDAP.Configuration.Server.Port](#rqsrs-007ldapconfigurationserverport)
    * 4.2.5 [RQ.SRS-007.LDAP.Configuration.Server.Port.Default](#rqsrs-007ldapconfigurationserverportdefault)
    * 4.2.6 [RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Prefix](#rqsrs-007ldapconfigurationserverauthdnprefix)
    * 4.2.7 [RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Suffix](#rqsrs-007ldapconfigurationserverauthdnsuffix)
    * 4.2.8 [RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Value](#rqsrs-007ldapconfigurationserverauthdnvalue)
    * 4.2.9 [RQ.SRS-007.LDAP.Configuration.Server.EnableTLS](#rqsrs-007ldapconfigurationserverenabletls)
    * 4.2.10 [RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Default](#rqsrs-007ldapconfigurationserverenabletlsoptionsdefault)
    * 4.2.11 [RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.No](#rqsrs-007ldapconfigurationserverenabletlsoptionsno)
    * 4.2.12 [RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Yes](#rqsrs-007ldapconfigurationserverenabletlsoptionsyes)
    * 4.2.13 [RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.StartTLS](#rqsrs-007ldapconfigurationserverenabletlsoptionsstarttls)
    * 4.2.14 [RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion](#rqsrs-007ldapconfigurationservertlsminimumprotocolversion)
    * 4.2.15 [RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Values](#rqsrs-007ldapconfigurationservertlsminimumprotocolversionvalues)
    * 4.2.16 [RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Default](#rqsrs-007ldapconfigurationservertlsminimumprotocolversiondefault)
    * 4.2.17 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert](#rqsrs-007ldapconfigurationservertlsrequirecert)
    * 4.2.18 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Default](#rqsrs-007ldapconfigurationservertlsrequirecertoptionsdefault)
    * 4.2.19 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Demand](#rqsrs-007ldapconfigurationservertlsrequirecertoptionsdemand)
    * 4.2.20 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Allow](#rqsrs-007ldapconfigurationservertlsrequirecertoptionsallow)
    * 4.2.21 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Try](#rqsrs-007ldapconfigurationservertlsrequirecertoptionstry)
    * 4.2.22 [RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Never](#rqsrs-007ldapconfigurationservertlsrequirecertoptionsnever)
    * 4.2.23 [RQ.SRS-007.LDAP.Configuration.Server.TLSCertFile](#rqsrs-007ldapconfigurationservertlscertfile)
    * 4.2.24 [RQ.SRS-007.LDAP.Configuration.Server.TLSKeyFile](#rqsrs-007ldapconfigurationservertlskeyfile)
    * 4.2.25 [RQ.SRS-007.LDAP.Configuration.Server.TLSCACertDir](#rqsrs-007ldapconfigurationservertlscacertdir)
    * 4.2.26 [RQ.SRS-007.LDAP.Configuration.Server.TLSCACertFile](#rqsrs-007ldapconfigurationservertlscacertfile)
    * 4.2.27 [RQ.SRS-007.LDAP.Configuration.Server.TLSCipherSuite](#rqsrs-007ldapconfigurationservertlsciphersuite)
    * 4.2.28 [RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown](#rqsrs-007ldapconfigurationserververificationcooldown)
    * 4.2.29 [RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Default](#rqsrs-007ldapconfigurationserververificationcooldowndefault)
    * 4.2.30 [RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Invalid](#rqsrs-007ldapconfigurationserververificationcooldowninvalid)
    * 4.2.31 [RQ.SRS-007.LDAP.Configuration.Server.Syntax](#rqsrs-007ldapconfigurationserversyntax)
    * 4.2.32 [RQ.SRS-007.LDAP.Configuration.User.RBAC](#rqsrs-007ldapconfigurationuserrbac)
    * 4.2.33 [RQ.SRS-007.LDAP.Configuration.User.Syntax](#rqsrs-007ldapconfigurationusersyntax)
    * 4.2.34 [RQ.SRS-007.LDAP.Configuration.User.Name.Empty](#rqsrs-007ldapconfigurationusernameempty)
    * 4.2.35 [RQ.SRS-007.LDAP.Configuration.User.BothPasswordAndLDAP](#rqsrs-007ldapconfigurationuserbothpasswordandldap)
    * 4.2.36 [RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.NotDefined](#rqsrs-007ldapconfigurationuserldapinvalidservernamenotdefined)
    * 4.2.37 [RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.Empty](#rqsrs-007ldapconfigurationuserldapinvalidservernameempty)
    * 4.2.38 [RQ.SRS-007.LDAP.Configuration.User.OnlyOneServer](#rqsrs-007ldapconfigurationuseronlyoneserver)
    * 4.2.39 [RQ.SRS-007.LDAP.Configuration.User.Name.Long](#rqsrs-007ldapconfigurationusernamelong)
    * 4.2.40 [RQ.SRS-007.LDAP.Configuration.User.Name.UTF8](#rqsrs-007ldapconfigurationusernameutf8)
    * 4.2.41 [RQ.SRS-007.LDAP.Authentication.Username.Empty](#rqsrs-007ldapauthenticationusernameempty)
    * 4.2.42 [RQ.SRS-007.LDAP.Authentication.Username.Long](#rqsrs-007ldapauthenticationusernamelong)
    * 4.2.43 [RQ.SRS-007.LDAP.Authentication.Username.UTF8](#rqsrs-007ldapauthenticationusernameutf8)
    * 4.2.44 [RQ.SRS-007.LDAP.Authentication.Password.Empty](#rqsrs-007ldapauthenticationpasswordempty)
    * 4.2.45 [RQ.SRS-007.LDAP.Authentication.Password.Long](#rqsrs-007ldapauthenticationpasswordlong)
    * 4.2.46 [RQ.SRS-007.LDAP.Authentication.Password.UTF8](#rqsrs-007ldapauthenticationpasswordutf8)
    * 4.2.47 [RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Performance](#rqsrs-007ldapauthenticationverificationcooldownperformance)
    * 4.2.48 [RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters](#rqsrs-007ldapauthenticationverificationcooldownresetchangeincoreserverparameters)
    * 4.2.49 [RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.InvalidPassword](#rqsrs-007ldapauthenticationverificationcooldownresetinvalidpassword)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Git]'s [Revision History].

## Introduction

[ClickHouse] currently does not have any integration with [LDAP].
As the initial step in integrating with [LDAP] this software requirements specification covers
only the requirements to enable authentication of users using an [LDAP] server.

## Terminology

* **CA** -
  Certificate Authority ([CA])

* **LDAP** -
  Lightweight Directory Access Protocol ([LDAP])

## Requirements

### Generic

#### RQ.SRS-007.LDAP.Authentication
version: 1.0

[ClickHouse] SHALL support user authentication via an [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.MultipleServers
version: 1.0

[ClickHouse] SHALL support specifying multiple [LDAP] servers that can be used to authenticate
users.

#### RQ.SRS-007.LDAP.Authentication.Protocol.PlainText
version: 1.0

[ClickHouse] SHALL support user authentication using plain text `ldap://` non secure protocol.

#### RQ.SRS-007.LDAP.Authentication.Protocol.TLS
version: 1.0

[ClickHouse] SHALL support user authentication using `SSL/TLS` `ldaps://` secure protocol.

#### RQ.SRS-007.LDAP.Authentication.Protocol.StartTLS
version: 1.0

[ClickHouse] SHALL support user authentication using legacy `StartTLS` protocol which is a
plain text `ldap://` protocol that is upgraded to [TLS].

#### RQ.SRS-007.LDAP.Authentication.TLS.Certificate.Validation
version: 1.0

[ClickHouse] SHALL support certificate validation used for [TLS] connections.

#### RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SelfSigned
version: 1.0

[ClickHouse] SHALL support self-signed certificates for [TLS] connections.

#### RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SpecificCertificationAuthority
version: 1.0

[ClickHouse] SHALL support certificates signed by specific Certification Authority for [TLS] connections.

#### RQ.SRS-007.LDAP.Server.Configuration.Invalid
version: 1.0

[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server configuration is not valid.

#### RQ.SRS-007.LDAP.User.Configuration.Invalid
version: 1.0

[ClickHouse] SHALL return an error and prohibit user login if user configuration is not valid.

#### RQ.SRS-007.LDAP.Authentication.Mechanism.Anonymous
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication using [Anonymous Authentication Mechanism of Simple Bind]
authentication mechanism.

#### RQ.SRS-007.LDAP.Authentication.Mechanism.Unauthenticated
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication using [Unauthenticated Authentication Mechanism of Simple Bind]
authentication mechanism.

#### RQ.SRS-007.LDAP.Authentication.Mechanism.NamePassword
version: 1.0

[ClickHouse] SHALL allow authentication using only [Name/Password Authentication Mechanism of Simple Bind]
authentication mechanism.

#### RQ.SRS-007.LDAP.Authentication.Valid
version: 1.0

[ClickHouse] SHALL only allow user authentication using [LDAP] server if and only if
user name and password match [LDAP] server records for the user.

#### RQ.SRS-007.LDAP.Authentication.Invalid
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if either user name or password
do not match [LDAP] server records for the user.

#### RQ.SRS-007.LDAP.Authentication.Invalid.DeletedUser
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if the user
has been deleted from the [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.UsernameChanged
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if the username is changed
on the [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.PasswordChanged
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if the password
for the user is changed on the [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.LDAPServerRestart
version: 1.0

[ClickHouse] SHALL support authenticating users after [LDAP] server is restarted.

#### RQ.SRS-007.LDAP.Authentication.ClickHouseServerRestart
version: 1.0

[ClickHouse] SHALL support authenticating users after server is restarted.

#### RQ.SRS-007.LDAP.Authentication.Parallel
version: 1.0

[ClickHouse] SHALL support parallel authentication of users using [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.Parallel.ValidAndInvalid
version: 1.0

[ClickHouse] SHALL support authentication of valid users and
prohibit authentication of invalid users using [LDAP] server
in parallel without having invalid attempts affecting valid authentications.

### Specific

#### RQ.SRS-007.LDAP.UnreachableServer
version: 1.0

[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server is unreachable.

#### RQ.SRS-007.LDAP.Configuration.Server.Name
version: 1.0

[ClickHouse] SHALL not support empty string as a server name.

#### RQ.SRS-007.LDAP.Configuration.Server.Host
version: 1.0

[ClickHouse] SHALL support `<host>` parameter to specify [LDAP]
server hostname or IP, this parameter SHALL be mandatory and SHALL not be empty.

#### RQ.SRS-007.LDAP.Configuration.Server.Port
version: 1.0

[ClickHouse] SHALL support `<port>` parameter to specify [LDAP] server port.

#### RQ.SRS-007.LDAP.Configuration.Server.Port.Default
version: 1.0

[ClickHouse] SHALL use default port number `636` if `enable_tls` is set to `yes` or `389` otherwise.

#### RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Prefix
version: 1.0

[ClickHouse] SHALL support `<auth_dn_prefix>` parameter to specify the prefix
of value used to construct the DN to bound to during authentication via [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Suffix
version: 1.0

[ClickHouse] SHALL support `<auth_dn_suffix>` parameter to specify the suffix
of value used to construct the DN to bound to during authentication via [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Value
version: 1.0

[ClickHouse] SHALL construct DN as  `auth_dn_prefix + escape(user_name) + auth_dn_suffix` string.

> This implies that auth_dn_suffix should usually have comma ',' as its first non-space character.

#### RQ.SRS-007.LDAP.Configuration.Server.EnableTLS
version: 1.0

[ClickHouse] SHALL support `<enable_tls>` parameter to trigger the use of secure connection to the [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Default
version: 1.0

[ClickHouse] SHALL use `yes` value as the default for `<enable_tls>` parameter
to enable SSL/TLS `ldaps://` protocol.

#### RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.No
version: 1.0

[ClickHouse] SHALL support specifying `no` as the value of `<enable_tls>` parameter to enable
plain text `ldap://` protocol.

#### RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Yes
version: 1.0

[ClickHouse] SHALL support specifying `yes` as the value of `<enable_tls>` parameter to enable
SSL/TLS `ldaps://` protocol.

#### RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.StartTLS
version: 1.0

[ClickHouse] SHALL support specifying `starttls` as the value of `<enable_tls>` parameter to enable
legacy `StartTLS` protocol that used plain text `ldap://` protocol, upgraded to [TLS].

#### RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion
version: 1.0

[ClickHouse] SHALL support `<tls_minimum_protocol_version>` parameter to specify
the minimum protocol version of SSL/TLS.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Values
version: 1.0

[ClickHouse] SHALL support specifying `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, and `tls1.2`
as a value of the `<tls_minimum_protocol_version>` parameter.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Default
version: 1.0

[ClickHouse] SHALL set `tls1.2` as the default value of the `<tls_minimum_protocol_version>` parameter.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert
version: 1.0

[ClickHouse] SHALL support `<tls_require_cert>` parameter to specify [TLS] peer
certificate verification behavior.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Default
version: 1.0

[ClickHouse] SHALL use `demand` value as the default for the `<tls_require_cert>` parameter.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Demand
version: 1.0

[ClickHouse] SHALL support specifying `demand` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate.  If no certificate  is  provided,  or  a  bad   certificate   is
provided, the session SHALL be immediately terminated.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Allow
version: 1.0

[ClickHouse] SHALL support specifying `allow` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate. If no
certificate is provided, the session SHALL proceed normally.
If a bad certificate is provided, it SHALL be ignored and the session SHALL proceed normally.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Try
version: 1.0

[ClickHouse] SHALL support specifying `try` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate. If no certificate is provided, the session
SHALL proceed  normally.  If a bad certificate is provided, the session SHALL be
immediately terminated.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Never
version: 1.0

[ClickHouse] SHALL support specifying `never` as the value of `<tls_require_cert>` parameter to
disable requesting of client certificate.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSCertFile
version: 1.0

[ClickHouse] SHALL support `<tls_cert_file>` to specify the path to certificate file used by
[ClickHouse] to establish connection with the [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSKeyFile
version: 1.0

[ClickHouse] SHALL support `<tls_key_file>` to specify the path to key file for the certificate
specified by the `<tls_cert_file>` parameter.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSCACertDir
version: 1.0

[ClickHouse] SHALL support `<tls_ca_cert_dir>` parameter to specify to a path to
the directory containing [CA] certificates used to verify certificates provided by the [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSCACertFile
version: 1.0

[ClickHouse] SHALL support `<tls_ca_cert_file>` parameter to specify a path to a specific
[CA] certificate file used to verify certificates provided by the [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.Server.TLSCipherSuite
version: 1.0

[ClickHouse] SHALL support `tls_cipher_suite` parameter to specify allowed cipher suites.
The value SHALL use the same format as the `ciphersuites` in the [OpenSSL Ciphers].

For example,

```xml
<tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
```

The available suites SHALL depend on the [OpenSSL] library version and variant used to build
[ClickHouse] and therefore might change.

#### RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown
version: 1.0

[ClickHouse] SHALL support `verification_cooldown` parameter in the [LDAP] server configuration section
that SHALL define a period of time, in seconds, after a successful bind attempt, during which a user SHALL be assumed
to be successfully authenticated for all consecutive requests without contacting the [LDAP] server.
After period of time since the last successful attempt expires then on the authentication attempt
SHALL result in contacting the [LDAP] server to verify the username and password. 

#### RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Default
version: 1.0

[ClickHouse] `verification_cooldown` parameter in the [LDAP] server configuration section
SHALL have a default value of `0` that disables caching and forces contacting
the [LDAP] server for each authentication request.

#### RQ.SRS-007.LDAP.Configuration.Server.VerificationCooldown.Invalid
version: 1.0

[Clickhouse] SHALL return an error if the value provided for the `verification_cooldown` parameter is not a valid positive integer.

For example:

* negative integer
* string
* empty value
* extremely large positive value (overflow)
* extremely large negative value (overflow)

The error SHALL appear in the log and SHALL be similar to the following:

```bash
<Error> Access(user directories): Could not parse LDAP server `openldap1`: Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Syntax error: Not a valid unsigned integer: *input value*
```

#### RQ.SRS-007.LDAP.Configuration.Server.Syntax
version: 2.0

[ClickHouse] SHALL support the following example syntax to create an entry for an [LDAP] server inside the `config.xml`
configuration file or of any configuration file inside the `config.d` directory.

```xml
<clickhouse>
    <my_ldap_server>
        <host>localhost</host>
        <port>636</port>
        <auth_dn_prefix>cn=</auth_dn_prefix>
        <auth_dn_suffix>, ou=users, dc=example, dc=com</auth_dn_suffix>
        <verification_cooldown>0</verification_cooldown>
        <enable_tls>yes</enable_tls>
        <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
        <tls_require_cert>demand</tls_require_cert>
        <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
        <tls_key_file>/path/to/tls_key_file</tls_key_file>
        <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
        <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
        <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
    </my_ldap_server>
</clickhouse>
```

#### RQ.SRS-007.LDAP.Configuration.User.RBAC
version: 1.0

[ClickHouse] SHALL support creating users identified using an [LDAP] server using
the following RBAC command

```sql
CREATE USER name IDENTIFIED WITH ldap SERVER 'server_name'
```

#### RQ.SRS-007.LDAP.Configuration.User.Syntax
version: 1.0

[ClickHouse] SHALL support the following example syntax to create a user that is authenticated using
an [LDAP] server inside the `users.xml` file or any configuration file inside the `users.d` directory.

```xml
<clickhouse>
    <users>
        <user_name>
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </user_name>
    </users>
</clickhouse>
```

#### RQ.SRS-007.LDAP.Configuration.User.Name.Empty
version: 1.0

[ClickHouse] SHALL not support empty string as a user name.

#### RQ.SRS-007.LDAP.Configuration.User.BothPasswordAndLDAP
version: 1.0

[ClickHouse] SHALL throw an error if `<ldap>` is specified for the user and at the same
time user configuration contains any of the `<password*>` entries.

#### RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.NotDefined
version: 1.0

[ClickHouse] SHALL throw an error during any authentication attempt
if the name of the [LDAP] server used inside the `<ldap>` entry
is not defined in the `<ldap_servers>` section.

#### RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.Empty
version: 1.0

[ClickHouse] SHALL throw an error during any authentication attempt
if the name of the [LDAP] server used inside the `<ldap>` entry
is empty.

#### RQ.SRS-007.LDAP.Configuration.User.OnlyOneServer
version: 1.0

[ClickHouse] SHALL support specifying only one [LDAP] server for a given user.

#### RQ.SRS-007.LDAP.Configuration.User.Name.Long
version: 1.0

[ClickHouse] SHALL support long user names of at least 256 bytes
to specify users that can be authenticated using an [LDAP] server.

#### RQ.SRS-007.LDAP.Configuration.User.Name.UTF8
version: 1.0

[ClickHouse] SHALL support user names that contain [UTF-8] characters.

#### RQ.SRS-007.LDAP.Authentication.Username.Empty
version: 1.0

[ClickHouse] SHALL not support authenticating users with empty username.

#### RQ.SRS-007.LDAP.Authentication.Username.Long
version: 1.0

[ClickHouse] SHALL support authenticating users with a long username of at least 256 bytes.

#### RQ.SRS-007.LDAP.Authentication.Username.UTF8
version: 1.0

[ClickHouse] SHALL support authentication users with a username that contains [UTF-8] characters.

#### RQ.SRS-007.LDAP.Authentication.Password.Empty
version: 1.0

[ClickHouse] SHALL not support authenticating users with empty passwords
even if an empty password is valid for the user and
is allowed by the [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.Password.Long
version: 1.0

[ClickHouse] SHALL support long password of at least 256 bytes
that can be used to authenticate users using an [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.Password.UTF8
version: 1.0

[ClickHouse] SHALL support [UTF-8] characters in passwords
used to authenticate users using an [LDAP] server.

#### RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Performance
version: 1.0

[ClickHouse] SHALL provide better login performance of [LDAP] authenticated users
when `verification_cooldown` parameter is set to a positive value when comparing
to the the case when `verification_cooldown` is turned off either for a single user or multiple users
making a large number of repeated requests.

#### RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters
version: 1.0

[ClickHouse] SHALL reset any currently cached [LDAP] authentication bind requests enabled by the
`verification_cooldown` parameter in the [LDAP] server configuration section
if either `host`, `port`, `auth_dn_prefix`, or `auth_dn_suffix` parameter values
change in the configuration file. The reset SHALL cause any subsequent authentication attempts for any user
to result in contacting the [LDAP] server to verify user's username and password.

#### RQ.SRS-007.LDAP.Authentication.VerificationCooldown.Reset.InvalidPassword
version: 1.0

[ClickHouse] SHALL reset current cached [LDAP] authentication bind request enabled by the
`verification_cooldown` parameter in the [LDAP] server configuration section
for the user if the password provided in the current authentication attempt does not match
the valid password provided during the first successful authentication request that was cached
for this exact user. The reset SHALL cause the next authentication attempt for this user
to result in contacting the [LDAP] server to verify user's username and password.

## References

* **ClickHouse:** https://clickhouse.com

[Anonymous Authentication Mechanism of Simple Bind]: https://ldapwiki.com/wiki/Simple%20Authentication#section-Simple+Authentication-AnonymousAuthenticationMechanismOfSimpleBind
[Unauthenticated Authentication Mechanism of Simple Bind]: https://ldapwiki.com/wiki/Simple%20Authentication#section-Simple+Authentication-UnauthenticatedAuthenticationMechanismOfSimpleBind
[Name/Password Authentication Mechanism of Simple Bind]: https://ldapwiki.com/wiki/Simple%20Authentication#section-Simple+Authentication-NamePasswordAuthenticationMechanismOfSimpleBind
[UTF-8]: https://en.wikipedia.org/wiki/UTF-8
[OpenSSL]: https://www.openssl.org/
[OpenSSL Ciphers]: https://www.openssl.org/docs/manmaster/man1/openssl-ciphers.html
[CA]: https://en.wikipedia.org/wiki/Certificate_authority
[TLS]: https://en.wikipedia.org/wiki/Transport_Layer_Security
[LDAP]: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
[ClickHouse]: https://clickhouse.com
[GitHub]: https://github.com
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/authentication/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/ldap/authentication/requirements/requirements.md
[Git]: https://git-scm.com/
""",
)

# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.200623.1103543.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Requirement

RQ_SRS_007_LDAP_Authentication = Requirement(
        name='RQ.SRS-007.LDAP.Authentication',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication via an [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_MultipleServers = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.MultipleServers',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying multiple [LDAP] servers that can be used to authenticate\n'
        'users.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Protocol_PlainText = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Protocol.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using plain text `ldap://` non secure protocol.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Protocol_TLS = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Protocol.TLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using `SSL/TLS` `ldaps://` secure protocol.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Protocol_StartTLS = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Protocol.StartTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using legacy `StartTLS` protocol which is a\n'
        'plain text `ldap://` protocol that is upgraded to [TLS].\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_Validation = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.TLS.Certificate.Validation',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support certificate validation used for [TLS] connections.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SelfSigned = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SelfSigned',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support self-signed certificates for [TLS] connections.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_TLS_Certificate_SpecificCertificationAuthority = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.TLS.Certificate.SpecificCertificationAuthority',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support certificates signed by specific Certification Authority for [TLS] connections.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Server_Configuration_Invalid = Requirement(
        name='RQ.SRS-007.LDAP.Server.Configuration.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server configuration is not valid.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_User_Configuration_Invalid = Requirement(
        name='RQ.SRS-007.LDAP.User.Configuration.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit user login if user configuration is not valid.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Mechanism_Anonymous = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Mechanism.Anonymous',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication using [Anonymous Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Mechanism_Unauthenticated = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Mechanism.Unauthenticated',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication using [Unauthenticated Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Mechanism_NamePassword = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Mechanism.NamePassword',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL allow authentication using only [Name/Password Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Valid = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Valid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL only allow user authentication using [LDAP] server if and only if\n'
        'user name and password match [LDAP] server records for the user.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Invalid = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if either user name or password\n'
        'do not match [LDAP] server records for the user.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Invalid_DeletedUser = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Invalid.DeletedUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if the user\n'
        'has been deleted from the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_UsernameChanged = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.UsernameChanged',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if the username is changed\n'
        'on the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_PasswordChanged = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.PasswordChanged',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if the password \n'
        'for the user is changed on the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_LDAPServerRestart = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.LDAPServerRestart',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users after [LDAP] server is restarted.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_ClickHouseServerRestart = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.ClickHouseServerRestart',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users after server is restarted.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Parallel = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Parallel',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of users using [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Parallel_ValidAndInvalid = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Parallel.ValidAndInvalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authentication of valid users and \n'
        'prohibit authentication of invalid users using [LDAP] server \n'
        'in parallel without having invalid attempts affecting valid authentications.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_UnreachableServer = Requirement(
        name='RQ.SRS-007.LDAP.UnreachableServer',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server is unreachable.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_Name = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.Name',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support empty string as a server name.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_Host = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.Host',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<host>` parameter to specify [LDAP]\n'
        'server hostname or IP, this parameter SHALL be mandatory and SHALL not be empty. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_Port = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.Port',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<port>` parameter to specify [LDAP] server port.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_Port_Default = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.Port.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use default port number `636` if `enable_tls` is set to `yes` or `389` otherwise.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Prefix = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Prefix',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<auth_dn_prefix>` parameter to specify the prefix\n'
        'of value used to construct the DN to bound to during authentication via [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Suffix = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Suffix',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<auth_dn_suffix>` parameter to specify the suffix \n'
        'of value used to construct the DN to bound to during authentication via [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_AuthDN_Value = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.AuthDN.Value',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL construct DN as  `auth_dn_prefix + escape(user_name) + auth_dn_suffix` string.\n'
        '\n'
        "> This implies that auth_dn_suffix should usually have comma ',' as its first non-space character.\n"
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.EnableTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<enable_tls>` parameter to trigger the use of secure connection to the [LDAP] server. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Default = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use `yes` value as the default for `<enable_tls>` parameter\n'
        'to enable SSL/TLS `ldaps://` protocol. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_No = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.No',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `no` as the value of `<enable_tls>` parameter to enable \n'
        'plain text `ldap://` protocol.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_Yes = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.Yes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `yes` as the value of `<enable_tls>` parameter to enable \n'
        'SSL/TLS `ldaps://` protocol.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_EnableTLS_Options_StartTLS = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.EnableTLS.Options.StartTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `starttls` as the value of `<enable_tls>` parameter to enable \n'
        'legacy `StartTLS` protocol that used plain text `ldap://` protocol, upgraded to [TLS].\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_minimum_protocol_version>` parameter to specify \n'
        'the minimum protocol version of SSL/TLS.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Values = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Values',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, and `tls1.2`\n'
        'as a value of the `<tls_minimum_protocol_version>` parameter.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSMinimumProtocolVersion_Default = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSMinimumProtocolVersion.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL set `tls1.2` as the default value of the `<tls_minimum_protocol_version>` parameter. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_require_cert>` parameter to specify [TLS] peer \n'
        'certificate verification behavior.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Default = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use `demand` value as the default for the `<tls_require_cert>` parameter.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Demand = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Demand',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `demand` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate.  If no certificate  is  provided,  or  a  bad   certificate   is\n'
        'provided, the session SHALL be immediately terminated.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Allow = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Allow',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `allow` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate. If no\n'
        'certificate is provided, the session SHALL proceed normally.  \n'
        'If a bad certificate is provided, it SHALL be ignored and the session SHALL proceed normally.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Try = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Try',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `try` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate. If no certificate is provided, the session\n'
        'SHALL proceed  normally.  If a bad certificate is provided, the session SHALL be \n'
        'immediately terminated.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSRequireCert_Options_Never = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSRequireCert.Options.Never',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `never` as the value of `<tls_require_cert>` parameter to\n'
        'disable requesting of client certificate.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSCertFile = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSCertFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_cert_file>` to specify the path to certificate file used by\n'
        '[ClickHouse] to establish connection with the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSKeyFile = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSKeyFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_key_file>` to specify the path to key file for the certificate\n'
        'specified by the `<tls_cert_file>` parameter.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSCACertDir = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSCACertDir',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_ca_cert_dir>` parameter to specify to a path to \n'
        'the directory containing [CA] certificates used to verify certificates provided by the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSCACertFile = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSCACertFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_ca_cert_file>` parameter to specify a path to a specific \n'
        '[CA] certificate file used to verify certificates provided by the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_TLSCipherSuite = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.TLSCipherSuite',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `tls_cipher_suite` parameter to specify allowed cipher suites.\n'
        'The value SHALL use the same format as the `ciphersuites` in the [OpenSSL Ciphers].\n'
        '\n'
        'For example, \n'
        '\n'
        '```xml\n'
        '<tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>\n'
        '```\n'
        '\n'
        'The available suites SHALL depend on the [OpenSSL] library version and variant used to build\n'
        '[ClickHouse] and therefore might change.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_Server_Syntax = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.Server.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following example syntax to create an entry for an [LDAP] server inside the `config.xml`\n'
        'configuration file or of any configuration file inside the `config.d` directory.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <my_ldap_server>\n'
        '        <host>localhost</host>\n'
        '        <port>636</port>\n'
        '        <auth_dn_prefix>cn=</auth_dn_prefix>\n'
        '        <auth_dn_suffix>, ou=users, dc=example, dc=com</auth_dn_suffix>\n'
        '        <enable_tls>yes</enable_tls>\n'
        '        <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>\n'
        '        <tls_require_cert>demand</tls_require_cert>\n'
        '        <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>\n'
        '        <tls_key_file>/path/to/tls_key_file</tls_key_file>\n'
        '        <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>\n'
        '        <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>\n'
        '        <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>\n'
        '    </my_ldap_server>\n'
        '</yandex>\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_Syntax = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following example syntax to create a user that is authenticated using\n'
        'an [LDAP] server inside the `users.xml` file or any configuration file inside the `users.d` directory.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <users>\n'
        '        <user_name>\n'
        '            <ldap>\n'
        '                <server>my_ldap_server</server>\n'
        '            </ldap>\n'
        '        </user_name>\n'
        '    </users>\n'
        '</yandex>\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_Name_Empty = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.Name.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support empty string as a user name.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_BothPasswordAndLDAP = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.BothPasswordAndLDAP',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL throw an error if `<ldap>` is specified for the user and at the same \n'
        'time user configuration contains any of the `<password*>` entries.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_NotDefined = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.NotDefined',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL throw an error during any authentification attempt\n'
        'if the name of the [LDAP] server used inside the `<ldap>` entry \n'
        'is not defined in the `<ldap_servers>` section. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_Empty = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.LDAP.InvalidServerName.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL throw an error during any authentification attempt\n'
        'if the name of the [LDAP] server used inside the `<ldap>` entry\n'
        'is empty. \n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_OnlyOneServer = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.OnlyOneServer',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying only one [LDAP] server for a given user.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_Name_Long = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.Name.Long',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support long user names of at least 256 bytes\n'
        'to specify users that can be authenticated using an [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Configuration_User_Name_UTF8 = Requirement(
        name='RQ.SRS-007.LDAP.Configuration.User.Name.UTF8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user names that contain [UTF-8] characters.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Username_Empty = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Username.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support authenticating users with empty username.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Username_Long = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Username.Long',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users with a long username of at least 256 bytes.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Username_UTF8 = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Username.UTF8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authentication users with a username that contains [UTF-8] characters.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Password_Empty = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Password.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support authenticating users with empty passwords\n'
        'even if an empty password is valid for the user and\n'
        'is allowed by the [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Password_Long = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Password.Long',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support long password of at least 256 bytes\n'
        'that can be used to authenticate users using an [LDAP] server.\n'
        ),
        link=None
    )

RQ_SRS_007_LDAP_Authentication_Password_UTF8 = Requirement(
        name='RQ.SRS-007.LDAP.Authentication.Password.UTF8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support [UTF-8] characters in passwords\n'
        'used to authenticate users using an [LDAP] server.\n'
        ),
        link=None
    )

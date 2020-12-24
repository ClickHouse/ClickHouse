# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.201009.1190249.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Requirement

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users that are defined only on the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_MultipleUserDirectories = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users using multiple [LDAP] external user directories.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_MultipleUserDirectories_Lookup = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories.Lookup',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL attempt to authenticate external [LDAP] user\n'
        'using [LDAP] external user directory in the same order\n'
        'in which user directories are specified in the `config.xml` file.\n'
        'If a user cannot be authenticated using the first [LDAP] external user directory\n'
        'then the next user directory in the list SHALL be used.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Authentication_NewUsers = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Authentication.NewUsers',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users that are defined only on the [LDAP] server\n'
        'as soon as they are added to the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_DeletedUsers = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.DeletedUsers',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow authentication of users that\n'
        'were previously defined only on the [LDAP] server but were removed\n'
        'from the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Valid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL only allow user authentication using [LDAP] server if and only if\n'
        'user name and password match [LDAP] server records for the user\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if either user name or password\n'
        'do not match [LDAP] server records for the user\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_UsernameChanged = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.UsernameChanged',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if the username is changed\n'
        'on the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_PasswordChanged = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.PasswordChanged',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication if the password\n'
        'for the user is changed on the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_LDAPServerRestart = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.LDAPServerRestart',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users after [LDAP] server is restarted\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_ClickHouseServerRestart = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.ClickHouseServerRestart',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users after server is restarted\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of users using [LDAP] server\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.ValidAndInvalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authentication of valid users and\n'
        'prohibit authentication of invalid users using [LDAP] server\n'
        'in parallel without having invalid attempts affecting valid authentications\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_MultipleServers = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.MultipleServers',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of external [LDAP] users\n'
        'authenticated using multiple [LDAP] external user directories.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalOnly = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalOnly',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of users defined only locally\n'
        'when one or more [LDAP] external user directories are specified in the configuration file.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalAndMultipleLDAP = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalAndMultipleLDAP',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of local and external [LDAP] users\n'
        'authenticated using multiple [LDAP] external user directories.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.SameUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of the same external [LDAP] user\n'
        'authenticated using the same [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_DynamicallyAddedAndRemovedUsers = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.DynamicallyAddedAndRemovedUsers',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support parallel authentication of users using\n'
        '[LDAP] external user directory when [LDAP] users are dynamically added and\n'
        'removed.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_PlainText = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using plain text `ldap://` non secure protocol\n'
        'while connecting to the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using `SSL/TLS` `ldaps://` secure protocol\n'
        'while connecting to the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_StartTLS = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.StartTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user authentication using legacy `StartTLS` protocol which is a\n'
        'plain text `ldap://` protocol that is upgraded to [TLS] when connecting to the [LDAP] server\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_Validation = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.Validation',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support certificate validation used for [TLS] connections\n'
        'to the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_SelfSigned = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SelfSigned',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support self-signed certificates for [TLS] connections\n'
        'to the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_SpecificCertificationAuthority = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SpecificCertificationAuthority',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support certificates signed by specific Certification Authority for [TLS] connections\n'
        'to the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_Anonymous = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Anonymous',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication using [Anonymous Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_Unauthenticated = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Unauthenticated',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit authentication using [Unauthenticated Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_NamePassword = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.NamePassword',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL allow authentication using only [Name/Password Authentication Mechanism of Simple Bind]\n'
        'authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_UnreachableServer = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.UnreachableServer',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server is unreachable\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Lookup_Priority = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Lookup.Priority',
        version='2.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL lookup user presence in the same order\n'
        'as user directories are defined in the `config.xml`.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support restarting server when one or more LDAP external directories\n'
        'are configured.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server_ParallelLogins = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server.ParallelLogins',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support restarting server when one or more LDAP external directories\n'
        'are configured during parallel [LDAP] user logins.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL reject authentication attempt if any of the roles that are specified in the configuration\n'
        'of the external user directory are not defined at the time of the authentication attempt\n'
        'with an exception that if a user was able to authenticate in past and its internal user object was created and cached\n'
        'then the user SHALL be able to authenticate again, even if one of the roles is missing.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed_Privileges = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed.Privileges',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the privileges provided by the role from all the LDAP\n'
        'users authenticated using external user directory if it is removed\n'
        'including currently cached users that are still able to authenticated where the removed\n'
        'role is specified in the configuration of the external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Readded_Privileges = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Readded.Privileges',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL reassign the role and add the privileges provided by the role\n'
        'when it is re-added after removal for all LDAP users authenticated using external user directory\n'
        'including any cached users where the re-added role was specified in the configuration of the external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_New = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.New',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any new roles to be assigned to any LDAP\n'
        'users authenticated using external user directory unless the role is specified\n'
        'in the configuration of the external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NewPrivilege = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NewPrivilege',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add new privilege to all the LDAP users authenticated using external user directory\n'
        'including cached users when new privilege is added to one of the roles specified\n'
        'in the configuration of the external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_RemovedPrivilege = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.RemovedPrivilege',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove privilege from all the LDAP users authenticated using external user directory\n'
        'including cached users when privilege is removed from all the roles specified\n'
        'in the configuration of the external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Invalid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server configuration is not valid.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Definition = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Definition',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using the [LDAP] servers defined in the\n'
        '`ldap_servers` section of the `config.xml` as the server to be used\n'
        'for a external user directory that uses an [LDAP] server as a source of user definitions.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Name = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Name',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support empty string as a server name.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Host = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Host',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<host>` parameter to specify [LDAP]\n'
        'server hostname or IP, this parameter SHALL be mandatory and SHALL not be empty.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<port>` parameter to specify [LDAP] server port.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port_Default = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use default port number `636` if `enable_tls` is set to `yes` or `389` otherwise.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Prefix = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Prefix',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<auth_dn_prefix>` parameter to specify the prefix\n'
        'of value used to construct the DN to bound to during authentication via [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Suffix = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Suffix',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<auth_dn_suffix>` parameter to specify the suffix\n'
        'of value used to construct the DN to bound to during authentication via [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Value = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Value',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL construct DN as  `auth_dn_prefix + escape(user_name) + auth_dn_suffix` string.\n'
        '\n'
        "> This implies that auth_dn_suffix should usually have comma ',' as its first non-space character.\n"
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<enable_tls>` parameter to trigger the use of secure connection to the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Default = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use `yes` value as the default for `<enable_tls>` parameter\n'
        'to enable SSL/TLS `ldaps://` protocol.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_No = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.No',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `no` as the value of `<enable_tls>` parameter to enable\n'
        'plain text `ldap://` protocol.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Yes = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Yes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `yes` as the value of `<enable_tls>` parameter to enable\n'
        'SSL/TLS `ldaps://` protocol.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_StartTLS = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.StartTLS',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `starttls` as the value of `<enable_tls>` parameter to enable\n'
        'legacy `StartTLS` protocol that used plain text `ldap://` protocol, upgraded to [TLS].\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_minimum_protocol_version>` parameter to specify\n'
        'the minimum protocol version of SSL/TLS.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Values = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Values',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, and `tls1.2`\n'
        'as a value of the `<tls_minimum_protocol_version>` parameter.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Default = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL set `tls1.2` as the default value of the `<tls_minimum_protocol_version>` parameter.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_require_cert>` parameter to specify [TLS] peer\n'
        'certificate verification behavior.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Default = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use `demand` value as the default for the `<tls_require_cert>` parameter.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Demand = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Demand',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `demand` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate.  If no certificate  is  provided,  or  a  bad   certificate   is\n'
        'provided, the session SHALL be immediately terminated.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Allow = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Allow',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `allow` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate. If no\n'
        'certificate is provided, the session SHALL proceed normally.\n'
        'If a bad certificate is provided, it SHALL be ignored and the session SHALL proceed normally.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Try = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Try',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `try` as the value of `<tls_require_cert>` parameter to\n'
        'enable requesting of client certificate. If no certificate is provided, the session\n'
        'SHALL proceed  normally.  If a bad certificate is provided, the session SHALL be\n'
        'immediately terminated.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Never = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Never',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `never` as the value of `<tls_require_cert>` parameter to\n'
        'disable requesting of client certificate.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCertFile = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCertFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_cert_file>` to specify the path to certificate file used by\n'
        '[ClickHouse] to establish connection with the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSKeyFile = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSKeyFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_key_file>` to specify the path to key file for the certificate\n'
        'specified by the `<tls_cert_file>` parameter.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCACertDir = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertDir',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_ca_cert_dir>` parameter to specify to a path to\n'
        'the directory containing [CA] certificates used to verify certificates provided by the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCACertFile = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertFile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<tls_ca_cert_file>` parameter to specify a path to a specific\n'
        '[CA] certificate file used to verify certificates provided by the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCipherSuite = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCipherSuite',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `tls_cipher_suite` parameter to specify allowed cipher suites.\n'
        'The value SHALL use the same format as the `ciphersuites` in the [OpenSSL Ciphers].\n'
        '\n'
        'For example,\n'
        '\n'
        '```xml\n'
        '<tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>\n'
        '```\n'
        '\n'
        'The available suites SHALL depend on the [OpenSSL] library version and variant used to build\n'
        '[ClickHouse] and therefore might change.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Syntax = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Syntax',
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
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<ldap>` sub-section in the `<user_directories>` section of the `config.xml`\n'
        'that SHALL define a external user directory that uses an [LDAP] server as a source of user definitions.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory_MoreThanOne = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory.MoreThanOne',
        version='2.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support more than one `<ldap>` sub-sections in the `<user_directories>` section of the `config.xml`\n'
        'that SHALL allow to define more than one external user directory that use an [LDAP] server as a source\n'
        'of user definitions.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Syntax = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `<ldap>` section with the following syntax\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <user_directories>\n'
        '        <ldap>\n'
        '            <server>my_ldap_server</server>\n'
        '            <roles>\n'
        '                <my_local_role1 />\n'
        '                <my_local_role2 />\n'
        '            </roles>\n'
        '        </ldap>\n'
        '    </user_directories>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `server` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'section of the `config.xml` that SHALL specify one of LDAP server names\n'
        'defined in `<ldap_servers>` section.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Empty = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'is empty.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Missing = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Missing',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'is missing.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_MoreThanOne = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.MoreThanOne',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL only use the first definitition of the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'if more than one `server` parameter is defined in the configuration.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Invalid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the server specified as the value of the `<server>`\n'
        'parameter is not defined.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'section of the `config.xml` that SHALL specify the names of a locally defined roles that SHALL\n'
        'be assigned to all users retrieved from the [LDAP] server.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_MoreThanOne = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.MoreThanOne',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL only use the first definitition of the `roles` parameter\n'
        'in the `<ldap>` sub-section in the `<user_directories>`\n'
        'if more than one `roles` parameter is defined in the configuration.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Invalid = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Invalid',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return an error if the role specified in the `<roles>`\n'
        'parameter does not exist locally.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Empty = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow users authenticated using LDAP external user directory\n'
        'to perform any action if the `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'section is empty.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Missing = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Missing',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow users authenticated using LDAP external user directory\n'
        'to perform any action if the `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`\n'
        'section is missing.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Empty = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support authenticating users with empty username\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Long = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Long',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authenticating users with a long username of at least 256 bytes\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_UTF8 = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.UTF8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support authentication users with a username that contains [UTF-8] characters\n'
        'when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Empty',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not support authenticating users with empty passwords\n'
        'even if an empty password is valid for the user and\n'
        'is allowed by the [LDAP] server when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Long = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Long',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support long password of at least 256 bytes\n'
        'that can be used to authenticate users when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_UTF8 = Requirement(
        name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.UTF8',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support [UTF-8] characters in passwords\n'
        'used to authenticate users when using [LDAP] external user directory.\n'
        '\n'
        ),
        link=None)

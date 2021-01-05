# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.201216.1172002.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

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
    link=None,
    level=4,
    num='4.1.1.1')

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
    link=None,
    level=4,
    num='4.1.1.2')

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
    link=None,
    level=4,
    num='4.1.1.3')

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
    link=None,
    level=4,
    num='4.1.1.4')

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
    link=None,
    level=4,
    num='4.1.1.5')

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
    link=None,
    level=4,
    num='4.1.1.6')

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
    link=None,
    level=4,
    num='4.1.1.7')

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
    link=None,
    level=4,
    num='4.1.1.8')

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
    link=None,
    level=4,
    num='4.1.1.9')

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
    link=None,
    level=4,
    num='4.1.1.10')

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
    link=None,
    level=4,
    num='4.1.1.11')

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
    link=None,
    level=4,
    num='4.1.1.12')

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
    link=None,
    level=4,
    num='4.1.1.13')

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
    link=None,
    level=4,
    num='4.1.1.14')

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
    link=None,
    level=4,
    num='4.1.1.15')

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
    link=None,
    level=4,
    num='4.1.1.16')

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
    link=None,
    level=4,
    num='4.1.1.17')

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
    link=None,
    level=4,
    num='4.1.1.18')

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
    link=None,
    level=4,
    num='4.1.2.1')

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
    link=None,
    level=4,
    num='4.1.2.2')

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
    link=None,
    level=4,
    num='4.1.2.3')

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
    link=None,
    level=4,
    num='4.1.2.4')

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
    link=None,
    level=4,
    num='4.1.2.5')

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
    link=None,
    level=4,
    num='4.1.2.6')

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
    link=None,
    level=4,
    num='4.1.2.7')

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
    link=None,
    level=4,
    num='4.1.2.8')

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
    link=None,
    level=4,
    num='4.1.2.9')

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
    link=None,
    level=4,
    num='4.1.2.10')

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
    link=None,
    level=4,
    num='4.2.1.1')

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
    link=None,
    level=4,
    num='4.2.1.2')

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
    link=None,
    level=4,
    num='4.2.1.3')

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed',
    version='2.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL allow authentication even if the roles that are specified in the configuration\n'
        'of the external user directory are not defined at the time of the authentication attempt.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.2.1')

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
    link=None,
    level=4,
    num='4.2.2.2')

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
    link=None,
    level=4,
    num='4.2.2.3')

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
    link=None,
    level=4,
    num='4.2.2.4')

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
    link=None,
    level=4,
    num='4.2.2.5')

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
    link=None,
    level=4,
    num='4.2.2.6')

RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NotPresent_Added = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NotPresent.Added',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL add a role to the users authenticated using LDAP external user directory\n'
        'that did not exist during the time of authentication but are defined in the \n'
        'configuration file as soon as the role with that name becomes\n'
        'available.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.2.7')

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
    link=None,
    level=4,
    num='4.2.3.1')

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
    link=None,
    level=4,
    num='4.2.3.2')

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
    link=None,
    level=4,
    num='4.2.3.3')

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
    link=None,
    level=4,
    num='4.2.3.4')

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
    link=None,
    level=4,
    num='4.2.3.5')

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
    link=None,
    level=4,
    num='4.2.3.6')

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
    link=None,
    level=4,
    num='4.2.3.7')

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
    link=None,
    level=4,
    num='4.2.3.8')

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
    link=None,
    level=4,
    num='4.2.3.9')

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
    link=None,
    level=4,
    num='4.2.3.10')

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
    link=None,
    level=4,
    num='4.2.3.11')

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
    link=None,
    level=4,
    num='4.2.3.12')

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
    link=None,
    level=4,
    num='4.2.3.13')

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
    link=None,
    level=4,
    num='4.2.3.14')

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
    link=None,
    level=4,
    num='4.2.3.15')

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
    link=None,
    level=4,
    num='4.2.3.16')

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
    link=None,
    level=4,
    num='4.2.3.17')

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
    link=None,
    level=4,
    num='4.2.3.18')

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
    link=None,
    level=4,
    num='4.2.3.19')

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
    link=None,
    level=4,
    num='4.2.3.20')

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
    link=None,
    level=4,
    num='4.2.3.21')

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
    link=None,
    level=4,
    num='4.2.3.22')

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
    link=None,
    level=4,
    num='4.2.3.23')

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
    link=None,
    level=4,
    num='4.2.3.24')

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
    link=None,
    level=4,
    num='4.2.3.25')

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
    link=None,
    level=4,
    num='4.2.3.26')

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
    link=None,
    level=4,
    num='4.2.3.27')

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
    link=None,
    level=4,
    num='4.2.3.28')

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `verification_cooldown` parameter in the [LDAP] server configuration section\n'
        'that SHALL define a period of time, in seconds, after a successful bind attempt, during which a user SHALL be assumed\n'
        'to be successfully authenticated for all consecutive requests without contacting the [LDAP] server.\n'
        'After period of time since the last successful attempt expires then on the authentication attempt\n'
        'SHALL result in contacting the [LDAP] server to verify the username and password.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.3.29')

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown_Default = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Default',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] `verification_cooldown` parameter in the [LDAP] server configuration section\n'
        'SHALL have a default value of `0` that disables caching and forces contacting\n'
        'the [LDAP] server for each authentication request.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.3.30')

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown_Invalid = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Invalid',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[Clickhouse] SHALL return an error if the value provided for the `verification_cooldown` parameter is not a valid positive integer.\n'
        '\n'
        'For example:\n'
        '\n'
        '* negative integer\n'
        '* string\n'
        '* empty value\n'
        '* extremely large positive value (overflow)\n'
        '* extremely large negative value (overflow)\n'
        '\n'
        'The error SHALL appear in the log and SHALL be similar to the following:\n'
        '\n'
        '```bash\n'
        '<Error> Access(user directories): Could not parse LDAP server `openldap1`: Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Syntax error: Not a valid unsigned integer: *input value*\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.3.31')

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Syntax = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Syntax',
    version='2.0',
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
        '        <verification_cooldown>0</verification_cooldown>\n'
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
    link=None,
    level=4,
    num='4.2.3.32')

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
    link=None,
    level=4,
    num='4.2.3.33')

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
    link=None,
    level=4,
    num='4.2.3.34')

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
    link=None,
    level=4,
    num='4.2.3.35')

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
    link=None,
    level=4,
    num='4.2.3.36')

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
    link=None,
    level=4,
    num='4.2.3.37')

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
    link=None,
    level=4,
    num='4.2.3.38')

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
    link=None,
    level=4,
    num='4.2.3.39')

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
    link=None,
    level=4,
    num='4.2.3.40')

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
    link=None,
    level=4,
    num='4.2.3.41')

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
    link=None,
    level=4,
    num='4.2.3.42')

RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Invalid = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Invalid',
    version='2.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not return an error if the role specified in the `<roles>`\n'
        'parameter does not exist locally. \n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.3.43')

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
    link=None,
    level=4,
    num='4.2.3.44')

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
    link=None,
    level=4,
    num='4.2.3.45')

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
    link=None,
    level=4,
    num='4.2.4.1')

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
    link=None,
    level=4,
    num='4.2.4.2')

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
    link=None,
    level=4,
    num='4.2.4.3')

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
    link=None,
    level=4,
    num='4.2.4.4')

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
    link=None,
    level=4,
    num='4.2.4.5')

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
    link=None,
    level=4,
    num='4.2.4.6')

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Performance = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Performance',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL provide better login performance of users authenticated using [LDAP] external user directory\n'
        'when `verification_cooldown` parameter is set to a positive value when comparing\n'
        'to the the case when `verification_cooldown` is turned off either for a single user or multiple users\n'
        'making a large number of repeated requests.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.4.7')

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL reset any currently cached [LDAP] authentication bind requests enabled by the\n'
        '`verification_cooldown` parameter in the [LDAP] server configuration section\n'
        'if either `host`, `port`, `auth_dn_prefix`, or `auth_dn_suffix` parameter values\n'
        'change in the configuration file. The reset SHALL cause any subsequent authentication attempts for any user\n'
        "to result in contacting the [LDAP] server to verify user's username and password.\n"
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.4.8')

RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_InvalidPassword = Requirement(
    name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.InvalidPassword',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL reset current cached [LDAP] authentication bind request enabled by the\n'
        '`verification_cooldown` parameter in the [LDAP] server configuration section\n'
        'for the user if the password provided in the current authentication attempt does not match\n'
        'the valid password provided during the first successful authentication request that was cached\n'
        'for this exact user. The reset SHALL cause the next authentication attempt for this user\n'
        "to result in contacting the [LDAP] server to verify user's username and password.\n"
        '\n'
        ),
    link=None,
    level=4,
    num='4.2.4.9')

SRS_009_ClickHouse_LDAP_External_User_Directory = Specification(
    name='SRS-009 ClickHouse LDAP External User Directory', 
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
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Terminology', level=1, num='3'),
        Heading(name='LDAP', level=2, num='3.1'),
        Heading(name='Requirements', level=1, num='4'),
        Heading(name='Generic', level=2, num='4.1'),
        Heading(name='User Authentication', level=3, num='4.1.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication', level=4, num='4.1.1.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories', level=4, num='4.1.1.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories.Lookup', level=4, num='4.1.1.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Authentication.NewUsers', level=4, num='4.1.1.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.DeletedUsers', level=4, num='4.1.1.5'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Valid', level=4, num='4.1.1.6'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Invalid', level=4, num='4.1.1.7'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.UsernameChanged', level=4, num='4.1.1.8'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.PasswordChanged', level=4, num='4.1.1.9'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.LDAPServerRestart', level=4, num='4.1.1.10'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.ClickHouseServerRestart', level=4, num='4.1.1.11'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel', level=4, num='4.1.1.12'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.ValidAndInvalid', level=4, num='4.1.1.13'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.MultipleServers', level=4, num='4.1.1.14'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalOnly', level=4, num='4.1.1.15'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalAndMultipleLDAP', level=4, num='4.1.1.16'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.SameUser', level=4, num='4.1.1.17'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.DynamicallyAddedAndRemovedUsers', level=4, num='4.1.1.18'),
        Heading(name='Connection', level=3, num='4.1.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.PlainText', level=4, num='4.1.2.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS', level=4, num='4.1.2.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.StartTLS', level=4, num='4.1.2.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.Validation', level=4, num='4.1.2.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SelfSigned', level=4, num='4.1.2.5'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SpecificCertificationAuthority', level=4, num='4.1.2.6'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Anonymous', level=4, num='4.1.2.7'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Unauthenticated', level=4, num='4.1.2.8'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.NamePassword', level=4, num='4.1.2.9'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.UnreachableServer', level=4, num='4.1.2.10'),
        Heading(name='Specific', level=2, num='4.2'),
        Heading(name='User Discovery', level=3, num='4.2.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Lookup.Priority', level=4, num='4.2.1.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server', level=4, num='4.2.1.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server.ParallelLogins', level=4, num='4.2.1.3'),
        Heading(name='Roles', level=3, num='4.2.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed', level=4, num='4.2.2.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed.Privileges', level=4, num='4.2.2.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Readded.Privileges', level=4, num='4.2.2.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.New', level=4, num='4.2.2.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NewPrivilege', level=4, num='4.2.2.5'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.RemovedPrivilege', level=4, num='4.2.2.6'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NotPresent.Added', level=4, num='4.2.2.7'),
        Heading(name='Configuration', level=3, num='4.2.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Invalid', level=4, num='4.2.3.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Definition', level=4, num='4.2.3.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Name', level=4, num='4.2.3.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Host', level=4, num='4.2.3.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port', level=4, num='4.2.3.5'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port.Default', level=4, num='4.2.3.6'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Prefix', level=4, num='4.2.3.7'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Suffix', level=4, num='4.2.3.8'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Value', level=4, num='4.2.3.9'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS', level=4, num='4.2.3.10'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Default', level=4, num='4.2.3.11'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.No', level=4, num='4.2.3.12'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Yes', level=4, num='4.2.3.13'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.StartTLS', level=4, num='4.2.3.14'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion', level=4, num='4.2.3.15'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Values', level=4, num='4.2.3.16'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Default', level=4, num='4.2.3.17'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert', level=4, num='4.2.3.18'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Default', level=4, num='4.2.3.19'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Demand', level=4, num='4.2.3.20'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Allow', level=4, num='4.2.3.21'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Try', level=4, num='4.2.3.22'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Never', level=4, num='4.2.3.23'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCertFile', level=4, num='4.2.3.24'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSKeyFile', level=4, num='4.2.3.25'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertDir', level=4, num='4.2.3.26'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertFile', level=4, num='4.2.3.27'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCipherSuite', level=4, num='4.2.3.28'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown', level=4, num='4.2.3.29'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Default', level=4, num='4.2.3.30'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Invalid', level=4, num='4.2.3.31'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Syntax', level=4, num='4.2.3.32'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory', level=4, num='4.2.3.33'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory.MoreThanOne', level=4, num='4.2.3.34'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Syntax', level=4, num='4.2.3.35'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server', level=4, num='4.2.3.36'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Empty', level=4, num='4.2.3.37'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Missing', level=4, num='4.2.3.38'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.MoreThanOne', level=4, num='4.2.3.39'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Invalid', level=4, num='4.2.3.40'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles', level=4, num='4.2.3.41'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.MoreThanOne', level=4, num='4.2.3.42'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Invalid', level=4, num='4.2.3.43'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Empty', level=4, num='4.2.3.44'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Missing', level=4, num='4.2.3.45'),
        Heading(name='Authentication', level=3, num='4.2.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Empty', level=4, num='4.2.4.1'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Long', level=4, num='4.2.4.2'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.UTF8', level=4, num='4.2.4.3'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Empty', level=4, num='4.2.4.4'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Long', level=4, num='4.2.4.5'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.UTF8', level=4, num='4.2.4.6'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Performance', level=4, num='4.2.4.7'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters', level=4, num='4.2.4.8'),
        Heading(name='RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.InvalidPassword', level=4, num='4.2.4.9'),
        Heading(name='References', level=1, num='5'),
        ),
    requirements=(
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication,
        RQ_SRS_009_LDAP_ExternalUserDirectory_MultipleUserDirectories,
        RQ_SRS_009_LDAP_ExternalUserDirectory_MultipleUserDirectories_Lookup,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Authentication_NewUsers,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_DeletedUsers,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Valid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Invalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_UsernameChanged,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_PasswordChanged,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_LDAPServerRestart,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_ClickHouseServerRestart,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_ValidAndInvalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_MultipleServers,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalOnly,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_LocalAndMultipleLDAP,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_SameUser,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Parallel_DynamicallyAddedAndRemovedUsers,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_PlainText,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_StartTLS,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_Validation,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_SelfSigned,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Protocol_TLS_Certificate_SpecificCertificationAuthority,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_Anonymous,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_Unauthenticated,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_Mechanism_NamePassword,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Connection_Authentication_UnreachableServer,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Users_Lookup_Priority,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Restart_Server_ParallelLogins,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Removed_Privileges,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_Readded_Privileges,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_New,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NewPrivilege,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_RemovedPrivilege,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Role_NotPresent_Added,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Invalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Definition,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Name,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Host,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Port_Default,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Prefix,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Suffix,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_AuthDN_Value,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Default,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_No,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_Yes,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_EnableTLS_Options_StartTLS,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Values,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSMinimumProtocolVersion_Default,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Default,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Demand,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Allow,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Try,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSRequireCert_Options_Never,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCertFile,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSKeyFile,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCACertDir,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCACertFile,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_TLSCipherSuite,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown_Default,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_VerificationCooldown_Invalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Server_Syntax,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_LDAPUserDirectory_MoreThanOne,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Syntax,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Empty,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Missing,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_MoreThanOne,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Server_Invalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_MoreThanOne,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Invalid,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Empty,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Configuration_Users_Parameters_Roles_Missing,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Empty,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_Long,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Username_UTF8,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Empty,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_Long,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_Password_UTF8,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Performance,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_ChangeInCoreServerParameters,
        RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication_VerificationCooldown_Reset_InvalidPassword,
        ),
    content='''
# SRS-009 ClickHouse LDAP External User Directory
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
  * 3.1 [LDAP](#ldap)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
    * 4.1.1 [User Authentication](#user-authentication)
      * 4.1.1.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication](#rqsrs-009ldapexternaluserdirectoryauthentication)
      * 4.1.1.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories](#rqsrs-009ldapexternaluserdirectorymultipleuserdirectories)
      * 4.1.1.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories.Lookup](#rqsrs-009ldapexternaluserdirectorymultipleuserdirectorieslookup)
      * 4.1.1.4 [RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Authentication.NewUsers](#rqsrs-009ldapexternaluserdirectoryusersauthenticationnewusers)
      * 4.1.1.5 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.DeletedUsers](#rqsrs-009ldapexternaluserdirectoryauthenticationdeletedusers)
      * 4.1.1.6 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Valid](#rqsrs-009ldapexternaluserdirectoryauthenticationvalid)
      * 4.1.1.7 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Invalid](#rqsrs-009ldapexternaluserdirectoryauthenticationinvalid)
      * 4.1.1.8 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.UsernameChanged](#rqsrs-009ldapexternaluserdirectoryauthenticationusernamechanged)
      * 4.1.1.9 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.PasswordChanged](#rqsrs-009ldapexternaluserdirectoryauthenticationpasswordchanged)
      * 4.1.1.10 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.LDAPServerRestart](#rqsrs-009ldapexternaluserdirectoryauthenticationldapserverrestart)
      * 4.1.1.11 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.ClickHouseServerRestart](#rqsrs-009ldapexternaluserdirectoryauthenticationclickhouseserverrestart)
      * 4.1.1.12 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel](#rqsrs-009ldapexternaluserdirectoryauthenticationparallel)
      * 4.1.1.13 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.ValidAndInvalid](#rqsrs-009ldapexternaluserdirectoryauthenticationparallelvalidandinvalid)
      * 4.1.1.14 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.MultipleServers](#rqsrs-009ldapexternaluserdirectoryauthenticationparallelmultipleservers)
      * 4.1.1.15 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalOnly](#rqsrs-009ldapexternaluserdirectoryauthenticationparallellocalonly)
      * 4.1.1.16 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalAndMultipleLDAP](#rqsrs-009ldapexternaluserdirectoryauthenticationparallellocalandmultipleldap)
      * 4.1.1.17 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.SameUser](#rqsrs-009ldapexternaluserdirectoryauthenticationparallelsameuser)
      * 4.1.1.18 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.DynamicallyAddedAndRemovedUsers](#rqsrs-009ldapexternaluserdirectoryauthenticationparalleldynamicallyaddedandremovedusers)
    * 4.1.2 [Connection](#connection)
      * 4.1.2.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.PlainText](#rqsrs-009ldapexternaluserdirectoryconnectionprotocolplaintext)
      * 4.1.2.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS](#rqsrs-009ldapexternaluserdirectoryconnectionprotocoltls)
      * 4.1.2.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.StartTLS](#rqsrs-009ldapexternaluserdirectoryconnectionprotocolstarttls)
      * 4.1.2.4 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.Validation](#rqsrs-009ldapexternaluserdirectoryconnectionprotocoltlscertificatevalidation)
      * 4.1.2.5 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SelfSigned](#rqsrs-009ldapexternaluserdirectoryconnectionprotocoltlscertificateselfsigned)
      * 4.1.2.6 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SpecificCertificationAuthority](#rqsrs-009ldapexternaluserdirectoryconnectionprotocoltlscertificatespecificcertificationauthority)
      * 4.1.2.7 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Anonymous](#rqsrs-009ldapexternaluserdirectoryconnectionauthenticationmechanismanonymous)
      * 4.1.2.8 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Unauthenticated](#rqsrs-009ldapexternaluserdirectoryconnectionauthenticationmechanismunauthenticated)
      * 4.1.2.9 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.NamePassword](#rqsrs-009ldapexternaluserdirectoryconnectionauthenticationmechanismnamepassword)
      * 4.1.2.10 [RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.UnreachableServer](#rqsrs-009ldapexternaluserdirectoryconnectionauthenticationunreachableserver)
  * 4.2 [Specific](#specific)
    * 4.2.1 [User Discovery](#user-discovery)
      * 4.2.1.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Lookup.Priority](#rqsrs-009ldapexternaluserdirectoryuserslookuppriority)
      * 4.2.1.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server](#rqsrs-009ldapexternaluserdirectoryrestartserver)
      * 4.2.1.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server.ParallelLogins](#rqsrs-009ldapexternaluserdirectoryrestartserverparallellogins)
    * 4.2.2 [Roles](#roles)
      * 4.2.2.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed](#rqsrs-009ldapexternaluserdirectoryroleremoved)
      * 4.2.2.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed.Privileges](#rqsrs-009ldapexternaluserdirectoryroleremovedprivileges)
      * 4.2.2.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Readded.Privileges](#rqsrs-009ldapexternaluserdirectoryrolereaddedprivileges)
      * 4.2.2.4 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.New](#rqsrs-009ldapexternaluserdirectoryrolenew)
      * 4.2.2.5 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NewPrivilege](#rqsrs-009ldapexternaluserdirectoryrolenewprivilege)
      * 4.2.2.6 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.RemovedPrivilege](#rqsrs-009ldapexternaluserdirectoryroleremovedprivilege)
      * 4.2.2.7 [RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NotPresent.Added](#rqsrs-009ldapexternaluserdirectoryrolenotpresentadded)
    * 4.2.3 [Configuration](#configuration)
      * 4.2.3.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Invalid](#rqsrs-009ldapexternaluserdirectoryconfigurationserverinvalid)
      * 4.2.3.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Definition](#rqsrs-009ldapexternaluserdirectoryconfigurationserverdefinition)
      * 4.2.3.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Name](#rqsrs-009ldapexternaluserdirectoryconfigurationservername)
      * 4.2.3.4 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Host](#rqsrs-009ldapexternaluserdirectoryconfigurationserverhost)
      * 4.2.3.5 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port](#rqsrs-009ldapexternaluserdirectoryconfigurationserverport)
      * 4.2.3.6 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port.Default](#rqsrs-009ldapexternaluserdirectoryconfigurationserverportdefault)
      * 4.2.3.7 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Prefix](#rqsrs-009ldapexternaluserdirectoryconfigurationserverauthdnprefix)
      * 4.2.3.8 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Suffix](#rqsrs-009ldapexternaluserdirectoryconfigurationserverauthdnsuffix)
      * 4.2.3.9 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Value](#rqsrs-009ldapexternaluserdirectoryconfigurationserverauthdnvalue)
      * 4.2.3.10 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS](#rqsrs-009ldapexternaluserdirectoryconfigurationserverenabletls)
      * 4.2.3.11 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Default](#rqsrs-009ldapexternaluserdirectoryconfigurationserverenabletlsoptionsdefault)
      * 4.2.3.12 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.No](#rqsrs-009ldapexternaluserdirectoryconfigurationserverenabletlsoptionsno)
      * 4.2.3.13 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Yes](#rqsrs-009ldapexternaluserdirectoryconfigurationserverenabletlsoptionsyes)
      * 4.2.3.14 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.StartTLS](#rqsrs-009ldapexternaluserdirectoryconfigurationserverenabletlsoptionsstarttls)
      * 4.2.3.15 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsminimumprotocolversion)
      * 4.2.3.16 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Values](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsminimumprotocolversionvalues)
      * 4.2.3.17 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Default](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsminimumprotocolversiondefault)
      * 4.2.3.18 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecert)
      * 4.2.3.19 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Default](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecertoptionsdefault)
      * 4.2.3.20 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Demand](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecertoptionsdemand)
      * 4.2.3.21 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Allow](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecertoptionsallow)
      * 4.2.3.22 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Try](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecertoptionstry)
      * 4.2.3.23 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Never](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsrequirecertoptionsnever)
      * 4.2.3.24 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCertFile](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlscertfile)
      * 4.2.3.25 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSKeyFile](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlskeyfile)
      * 4.2.3.26 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertDir](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlscacertdir)
      * 4.2.3.27 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertFile](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlscacertfile)
      * 4.2.3.28 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCipherSuite](#rqsrs-009ldapexternaluserdirectoryconfigurationservertlsciphersuite)
      * 4.2.3.29 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown](#rqsrs-009ldapexternaluserdirectoryconfigurationserververificationcooldown)
      * 4.2.3.30 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Default](#rqsrs-009ldapexternaluserdirectoryconfigurationserververificationcooldowndefault)
      * 4.2.3.31 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Invalid](#rqsrs-009ldapexternaluserdirectoryconfigurationserververificationcooldowninvalid)
      * 4.2.3.32 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Syntax](#rqsrs-009ldapexternaluserdirectoryconfigurationserversyntax)
      * 4.2.3.33 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory](#rqsrs-009ldapexternaluserdirectoryconfigurationusersldapuserdirectory)
      * 4.2.3.34 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory.MoreThanOne](#rqsrs-009ldapexternaluserdirectoryconfigurationusersldapuserdirectorymorethanone)
      * 4.2.3.35 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Syntax](#rqsrs-009ldapexternaluserdirectoryconfigurationuserssyntax)
      * 4.2.3.36 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersserver)
      * 4.2.3.37 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Empty](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersserverempty)
      * 4.2.3.38 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Missing](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersservermissing)
      * 4.2.3.39 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.MoreThanOne](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersservermorethanone)
      * 4.2.3.40 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Invalid](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersserverinvalid)
      * 4.2.3.41 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersroles)
      * 4.2.3.42 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.MoreThanOne](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersrolesmorethanone)
      * 4.2.3.43 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Invalid](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersrolesinvalid)
      * 4.2.3.44 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Empty](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersrolesempty)
      * 4.2.3.45 [RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Missing](#rqsrs-009ldapexternaluserdirectoryconfigurationusersparametersrolesmissing)
    * 4.2.4 [Authentication](#authentication)
      * 4.2.4.1 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Empty](#rqsrs-009ldapexternaluserdirectoryauthenticationusernameempty)
      * 4.2.4.2 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Long](#rqsrs-009ldapexternaluserdirectoryauthenticationusernamelong)
      * 4.2.4.3 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.UTF8](#rqsrs-009ldapexternaluserdirectoryauthenticationusernameutf8)
      * 4.2.4.4 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Empty](#rqsrs-009ldapexternaluserdirectoryauthenticationpasswordempty)
      * 4.2.4.5 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Long](#rqsrs-009ldapexternaluserdirectoryauthenticationpasswordlong)
      * 4.2.4.6 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.UTF8](#rqsrs-009ldapexternaluserdirectoryauthenticationpasswordutf8)
      * 4.2.4.7 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Performance](#rqsrs-009ldapexternaluserdirectoryauthenticationverificationcooldownperformance)
      * 4.2.4.8 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters](#rqsrs-009ldapexternaluserdirectoryauthenticationverificationcooldownresetchangeincoreserverparameters)
      * 4.2.4.9 [RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.InvalidPassword](#rqsrs-009ldapexternaluserdirectoryauthenticationverificationcooldownresetinvalidpassword)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

The [QA-SRS007 ClickHouse Authentication of Users via LDAP] enables support for authenticating
users using an [LDAP] server. This requirements specifications add addition functionality
for integrating [LDAP] with [ClickHouse].

This document will cover requirements to allow authenticatoin of users stored in the
external user discovery using an [LDAP] server without having to explicitly define users in [ClickHouse]'s
`users.xml` configuration file.

## Terminology

### LDAP

* Lightweight Directory Access Protocol

## Requirements

### Generic

#### User Authentication

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication
version: 1.0

[ClickHouse] SHALL support authenticating users that are defined only on the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories
version: 1.0

[ClickHouse] SHALL support authenticating users using multiple [LDAP] external user directories.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.MultipleUserDirectories.Lookup
version: 1.0

[ClickHouse] SHALL attempt to authenticate external [LDAP] user
using [LDAP] external user directory in the same order
in which user directories are specified in the `config.xml` file.
If a user cannot be authenticated using the first [LDAP] external user directory
then the next user directory in the list SHALL be used.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Authentication.NewUsers
version: 1.0

[ClickHouse] SHALL support authenticating users that are defined only on the [LDAP] server
as soon as they are added to the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.DeletedUsers
version: 1.0

[ClickHouse] SHALL not allow authentication of users that
were previously defined only on the [LDAP] server but were removed
from the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Valid
version: 1.0

[ClickHouse] SHALL only allow user authentication using [LDAP] server if and only if
user name and password match [LDAP] server records for the user
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Invalid
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if either user name or password
do not match [LDAP] server records for the user
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.UsernameChanged
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if the username is changed
on the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.PasswordChanged
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication if the password
for the user is changed on the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.LDAPServerRestart
version: 1.0

[ClickHouse] SHALL support authenticating users after [LDAP] server is restarted
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.ClickHouseServerRestart
version: 1.0

[ClickHouse] SHALL support authenticating users after server is restarted
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel
version: 1.0

[ClickHouse] SHALL support parallel authentication of users using [LDAP] server
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.ValidAndInvalid
version: 1.0

[ClickHouse] SHALL support authentication of valid users and
prohibit authentication of invalid users using [LDAP] server
in parallel without having invalid attempts affecting valid authentications
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.MultipleServers
version: 1.0

[ClickHouse] SHALL support parallel authentication of external [LDAP] users
authenticated using multiple [LDAP] external user directories.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalOnly
version: 1.0

[ClickHouse] SHALL support parallel authentication of users defined only locally
when one or more [LDAP] external user directories are specified in the configuration file.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.LocalAndMultipleLDAP
version: 1.0

[ClickHouse] SHALL support parallel authentication of local and external [LDAP] users
authenticated using multiple [LDAP] external user directories.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.SameUser
version: 1.0

[ClickHouse] SHALL support parallel authentication of the same external [LDAP] user
authenticated using the same [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Parallel.DynamicallyAddedAndRemovedUsers
version: 1.0

[ClickHouse] SHALL support parallel authentication of users using
[LDAP] external user directory when [LDAP] users are dynamically added and
removed.

#### Connection

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.PlainText
version: 1.0

[ClickHouse] SHALL support user authentication using plain text `ldap://` non secure protocol
while connecting to the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS
version: 1.0

[ClickHouse] SHALL support user authentication using `SSL/TLS` `ldaps://` secure protocol
while connecting to the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.StartTLS
version: 1.0

[ClickHouse] SHALL support user authentication using legacy `StartTLS` protocol which is a
plain text `ldap://` protocol that is upgraded to [TLS] when connecting to the [LDAP] server
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.Validation
version: 1.0

[ClickHouse] SHALL support certificate validation used for [TLS] connections
to the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SelfSigned
version: 1.0

[ClickHouse] SHALL support self-signed certificates for [TLS] connections
to the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Protocol.TLS.Certificate.SpecificCertificationAuthority
version: 1.0

[ClickHouse] SHALL support certificates signed by specific Certification Authority for [TLS] connections
to the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Anonymous
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication using [Anonymous Authentication Mechanism of Simple Bind]
authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.Unauthenticated
version: 1.0

[ClickHouse] SHALL return an error and prohibit authentication using [Unauthenticated Authentication Mechanism of Simple Bind]
authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.Mechanism.NamePassword
version: 1.0

[ClickHouse] SHALL allow authentication using only [Name/Password Authentication Mechanism of Simple Bind]
authentication mechanism when connecting to the [LDAP] server when using [LDAP] external server directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Connection.Authentication.UnreachableServer
version: 1.0

[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server is unreachable
when using [LDAP] external user directory.

### Specific

#### User Discovery

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Users.Lookup.Priority
version: 2.0

[ClickHouse] SHALL lookup user presence in the same order
as user directories are defined in the `config.xml`.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server
version: 1.0

[ClickHouse] SHALL support restarting server when one or more LDAP external directories
are configured.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Restart.Server.ParallelLogins
version: 1.0

[ClickHouse] SHALL support restarting server when one or more LDAP external directories
are configured during parallel [LDAP] user logins.

#### Roles

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed
version: 2.0

[ClickHouse] SHALL allow authentication even if the roles that are specified in the configuration
of the external user directory are not defined at the time of the authentication attempt.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Removed.Privileges
version: 1.0

[ClickHouse] SHALL remove the privileges provided by the role from all the LDAP
users authenticated using external user directory if it is removed
including currently cached users that are still able to authenticated where the removed
role is specified in the configuration of the external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.Readded.Privileges
version: 1.0

[ClickHouse] SHALL reassign the role and add the privileges provided by the role
when it is re-added after removal for all LDAP users authenticated using external user directory
including any cached users where the re-added role was specified in the configuration of the external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.New
version: 1.0

[ClickHouse] SHALL not allow any new roles to be assigned to any LDAP
users authenticated using external user directory unless the role is specified
in the configuration of the external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NewPrivilege
version: 1.0

[ClickHouse] SHALL add new privilege to all the LDAP users authenticated using external user directory
including cached users when new privilege is added to one of the roles specified
in the configuration of the external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.RemovedPrivilege
version: 1.0

[ClickHouse] SHALL remove privilege from all the LDAP users authenticated using external user directory
including cached users when privilege is removed from all the roles specified
in the configuration of the external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Role.NotPresent.Added
version: 1.0

[ClickHouse] SHALL add a role to the users authenticated using LDAP external user directory
that did not exist during the time of authentication but are defined in the 
configuration file as soon as the role with that name becomes
available.

#### Configuration

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Invalid
version: 1.0

[ClickHouse] SHALL return an error and prohibit user login if [LDAP] server configuration is not valid.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Definition
version: 1.0

[ClickHouse] SHALL support using the [LDAP] servers defined in the
`ldap_servers` section of the `config.xml` as the server to be used
for a external user directory that uses an [LDAP] server as a source of user definitions.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Name
version: 1.0

[ClickHouse] SHALL not support empty string as a server name.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Host
version: 1.0

[ClickHouse] SHALL support `<host>` parameter to specify [LDAP]
server hostname or IP, this parameter SHALL be mandatory and SHALL not be empty.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port
version: 1.0

[ClickHouse] SHALL support `<port>` parameter to specify [LDAP] server port.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Port.Default
version: 1.0

[ClickHouse] SHALL use default port number `636` if `enable_tls` is set to `yes` or `389` otherwise.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Prefix
version: 1.0

[ClickHouse] SHALL support `<auth_dn_prefix>` parameter to specify the prefix
of value used to construct the DN to bound to during authentication via [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Suffix
version: 1.0

[ClickHouse] SHALL support `<auth_dn_suffix>` parameter to specify the suffix
of value used to construct the DN to bound to during authentication via [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.AuthDN.Value
version: 1.0

[ClickHouse] SHALL construct DN as  `auth_dn_prefix + escape(user_name) + auth_dn_suffix` string.

> This implies that auth_dn_suffix should usually have comma ',' as its first non-space character.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS
version: 1.0

[ClickHouse] SHALL support `<enable_tls>` parameter to trigger the use of secure connection to the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Default
version: 1.0

[ClickHouse] SHALL use `yes` value as the default for `<enable_tls>` parameter
to enable SSL/TLS `ldaps://` protocol.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.No
version: 1.0

[ClickHouse] SHALL support specifying `no` as the value of `<enable_tls>` parameter to enable
plain text `ldap://` protocol.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.Yes
version: 1.0

[ClickHouse] SHALL support specifying `yes` as the value of `<enable_tls>` parameter to enable
SSL/TLS `ldaps://` protocol.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.EnableTLS.Options.StartTLS
version: 1.0

[ClickHouse] SHALL support specifying `starttls` as the value of `<enable_tls>` parameter to enable
legacy `StartTLS` protocol that used plain text `ldap://` protocol, upgraded to [TLS].

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion
version: 1.0

[ClickHouse] SHALL support `<tls_minimum_protocol_version>` parameter to specify
the minimum protocol version of SSL/TLS.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Values
version: 1.0

[ClickHouse] SHALL support specifying `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, and `tls1.2`
as a value of the `<tls_minimum_protocol_version>` parameter.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSMinimumProtocolVersion.Default
version: 1.0

[ClickHouse] SHALL set `tls1.2` as the default value of the `<tls_minimum_protocol_version>` parameter.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert
version: 1.0

[ClickHouse] SHALL support `<tls_require_cert>` parameter to specify [TLS] peer
certificate verification behavior.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Default
version: 1.0

[ClickHouse] SHALL use `demand` value as the default for the `<tls_require_cert>` parameter.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Demand
version: 1.0

[ClickHouse] SHALL support specifying `demand` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate.  If no certificate  is  provided,  or  a  bad   certificate   is
provided, the session SHALL be immediately terminated.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Allow
version: 1.0

[ClickHouse] SHALL support specifying `allow` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate. If no
certificate is provided, the session SHALL proceed normally.
If a bad certificate is provided, it SHALL be ignored and the session SHALL proceed normally.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Try
version: 1.0

[ClickHouse] SHALL support specifying `try` as the value of `<tls_require_cert>` parameter to
enable requesting of client certificate. If no certificate is provided, the session
SHALL proceed  normally.  If a bad certificate is provided, the session SHALL be
immediately terminated.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSRequireCert.Options.Never
version: 1.0

[ClickHouse] SHALL support specifying `never` as the value of `<tls_require_cert>` parameter to
disable requesting of client certificate.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCertFile
version: 1.0

[ClickHouse] SHALL support `<tls_cert_file>` to specify the path to certificate file used by
[ClickHouse] to establish connection with the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSKeyFile
version: 1.0

[ClickHouse] SHALL support `<tls_key_file>` to specify the path to key file for the certificate
specified by the `<tls_cert_file>` parameter.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertDir
version: 1.0

[ClickHouse] SHALL support `<tls_ca_cert_dir>` parameter to specify to a path to
the directory containing [CA] certificates used to verify certificates provided by the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCACertFile
version: 1.0

[ClickHouse] SHALL support `<tls_ca_cert_file>` parameter to specify a path to a specific
[CA] certificate file used to verify certificates provided by the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.TLSCipherSuite
version: 1.0

[ClickHouse] SHALL support `tls_cipher_suite` parameter to specify allowed cipher suites.
The value SHALL use the same format as the `ciphersuites` in the [OpenSSL Ciphers].

For example,

```xml
<tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
```

The available suites SHALL depend on the [OpenSSL] library version and variant used to build
[ClickHouse] and therefore might change.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown
version: 1.0

[ClickHouse] SHALL support `verification_cooldown` parameter in the [LDAP] server configuration section
that SHALL define a period of time, in seconds, after a successful bind attempt, during which a user SHALL be assumed
to be successfully authenticated for all consecutive requests without contacting the [LDAP] server.
After period of time since the last successful attempt expires then on the authentication attempt
SHALL result in contacting the [LDAP] server to verify the username and password.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Default
version: 1.0

[ClickHouse] `verification_cooldown` parameter in the [LDAP] server configuration section
SHALL have a default value of `0` that disables caching and forces contacting
the [LDAP] server for each authentication request.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.VerificationCooldown.Invalid
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

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Server.Syntax
version: 2.0

[ClickHouse] SHALL support the following example syntax to create an entry for an [LDAP] server inside the `config.xml`
configuration file or of any configuration file inside the `config.d` directory.

```xml
<yandex>
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
</yandex>
```

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory
version: 1.0

[ClickHouse] SHALL support `<ldap>` sub-section in the `<user_directories>` section of the `config.xml`
that SHALL define a external user directory that uses an [LDAP] server as a source of user definitions.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.LDAPUserDirectory.MoreThanOne
version: 2.0

[ClickHouse] SHALL support more than one `<ldap>` sub-sections in the `<user_directories>` section of the `config.xml`
that SHALL allow to define more than one external user directory that use an [LDAP] server as a source
of user definitions.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Syntax
version: 1.0

[ClickHouse] SHALL support `<ldap>` section with the following syntax

```xml
<yandex>
    <user_directories>
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
        </ldap>
    </user_directories>
</yandex>
```

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server
version: 1.0

[ClickHouse] SHALL support `server` parameter in the `<ldap>` sub-section in the `<user_directories>`
section of the `config.xml` that SHALL specify one of LDAP server names
defined in `<ldap_servers>` section.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Empty
version: 1.0

[ClickHouse] SHALL return an error if the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`
is empty.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Missing
version: 1.0

[ClickHouse] SHALL return an error if the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`
is missing.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.MoreThanOne
version: 1.0

[ClickHouse] SHALL only use the first definitition of the `server` parameter in the `<ldap>` sub-section in the `<user_directories>`
if more than one `server` parameter is defined in the configuration.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Server.Invalid
version: 1.0

[ClickHouse] SHALL return an error if the server specified as the value of the `<server>`
parameter is not defined.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles
version: 1.0

[ClickHouse] SHALL support `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`
section of the `config.xml` that SHALL specify the names of a locally defined roles that SHALL
be assigned to all users retrieved from the [LDAP] server.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.MoreThanOne
version: 1.0

[ClickHouse] SHALL only use the first definitition of the `roles` parameter
in the `<ldap>` sub-section in the `<user_directories>`
if more than one `roles` parameter is defined in the configuration.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Invalid
version: 2.0

[ClickHouse] SHALL not return an error if the role specified in the `<roles>`
parameter does not exist locally. 

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Empty
version: 1.0

[ClickHouse] SHALL not allow users authenticated using LDAP external user directory
to perform any action if the `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`
section is empty.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Configuration.Users.Parameters.Roles.Missing
version: 1.0

[ClickHouse] SHALL not allow users authenticated using LDAP external user directory
to perform any action if the `roles` parameter in the `<ldap>` sub-section in the `<user_directories>`
section is missing.

#### Authentication

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Empty
version: 1.0

[ClickHouse] SHALL not support authenticating users with empty username
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.Long
version: 1.0

[ClickHouse] SHALL support authenticating users with a long username of at least 256 bytes
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Username.UTF8
version: 1.0

[ClickHouse] SHALL support authentication users with a username that contains [UTF-8] characters
when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Empty
version: 1.0

[ClickHouse] SHALL not support authenticating users with empty passwords
even if an empty password is valid for the user and
is allowed by the [LDAP] server when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.Long
version: 1.0

[ClickHouse] SHALL support long password of at least 256 bytes
that can be used to authenticate users when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.Password.UTF8
version: 1.0

[ClickHouse] SHALL support [UTF-8] characters in passwords
used to authenticate users when using [LDAP] external user directory.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Performance
version: 1.0

[ClickHouse] SHALL provide better login performance of users authenticated using [LDAP] external user directory
when `verification_cooldown` parameter is set to a positive value when comparing
to the the case when `verification_cooldown` is turned off either for a single user or multiple users
making a large number of repeated requests.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.ChangeInCoreServerParameters
version: 1.0

[ClickHouse] SHALL reset any currently cached [LDAP] authentication bind requests enabled by the
`verification_cooldown` parameter in the [LDAP] server configuration section
if either `host`, `port`, `auth_dn_prefix`, or `auth_dn_suffix` parameter values
change in the configuration file. The reset SHALL cause any subsequent authentication attempts for any user
to result in contacting the [LDAP] server to verify user's username and password.

##### RQ.SRS-009.LDAP.ExternalUserDirectory.Authentication.VerificationCooldown.Reset.InvalidPassword
version: 1.0

[ClickHouse] SHALL reset current cached [LDAP] authentication bind request enabled by the
`verification_cooldown` parameter in the [LDAP] server configuration section
for the user if the password provided in the current authentication attempt does not match
the valid password provided during the first successful authentication request that was cached
for this exact user. The reset SHALL cause the next authentication attempt for this user
to result in contacting the [LDAP] server to verify user's username and password.

## References

* **Access Control and Account Management**: https://clickhouse.tech/docs/en/operations/access-rights/
* **LDAP**: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
* **ClickHouse:** https://clickhouse.tech

[SRS]: #srs
[Access Control and Account Management]: https://clickhouse.tech/docs/en/operations/access-rights/
[SRS-007 ClickHouse Authentication of Users via LDAP]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/authentication/requirements/requirements.md
[LDAP]: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
[ClickHouse]: https://clickhouse.tech
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/external_user_directory/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/ldap/external_user_directory/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
''')

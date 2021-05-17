# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.210505.1133630.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_014_LDAP_RoleMapping = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping of [LDAP] groups to [RBAC] roles\n'
        'for users authenticated using [LDAP] external user directory.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.1')

RQ_SRS_014_LDAP_RoleMapping_WithFixedRoles = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.WithFixedRoles',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping of [LDAP] groups to [RBAC] roles\n'
        'for users authenticated using [LDAP] external user directory when\n'
        'one or more roles are specified in the `<roles>` section.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.2')

RQ_SRS_014_LDAP_RoleMapping_Search = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Search',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL perform search on the [LDAP] server and map the results to [RBAC] role names \n'
        'when authenticating users using the [LDAP] external user directory if the `<role_mapping>` section is configured\n'
        'as part of the [LDAP] external user directory. The matched roles SHALL be assigned to the user.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.1.3')

RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithUTF8Characters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithUTF8Characters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory\n'
        'to an [RBAC] role that contains UTF-8 characters.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.1')

RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_Long = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.Long',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory\n'
        'to an [RBAC] role that has a name with more than 128 characters.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.2')

RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialXMLCharacters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialXMLCharacters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory\n'
        'to an [RBAC] role that has a name that contains special characters that need to be escaped in XML.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.3')

RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialRegexCharacters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialRegexCharacters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory\n'
        'to an [RBAC] role that has a name that contains special characters that need to be escaped in regex.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.2.4')

RQ_SRS_014_LDAP_RoleMapping_Map_MultipleRoles = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Map.MultipleRoles',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support mapping one or more [LDAP] search results for users authenticated using \n'
        '[LDAP] external user directory to one or more [RBAC] role.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.3.1')

RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_Removed = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.Removed',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not assign [RBAC] role(s) for any users authenticated using [LDAP] external user directory\n'
        'if the corresponding [LDAP] group(s) that map those role(s) are removed. Any users that have active sessions SHALL still\n'
        'have privileges provided by the role(s) until the next time they are authenticated.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.1')

RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_RemovedAndAdded_Parallel = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.RemovedAndAdded.Parallel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support authenticating users using [LDAP] external user directory \n'
        'when [LDAP] groups are removed and added \n'
        'at the same time as [LDAP] user authentications are performed in parallel.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.2')

RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemoved = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemoved',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not assign [RBAC] role(s) for the user authenticated using [LDAP] external user directory\n'
        'if the user has been removed from the corresponding [LDAP] group(s) that map those role(s). \n'
        'Any active user sessions SHALL have privileges provided by the role(s) until the next time the user is authenticated.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.3')

RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemovedAndAdded_Parallel = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemovedAndAdded.Parallel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support authenticating users using [LDAP] external user directory\n'
        'when [LDAP] users are added and removed from [LDAP] groups used to map to [RBAC] roles\n'
        'at the same time as [LDAP] user authentications are performed in parallel.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.4.4')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NotPresent = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NotPresent',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not reject authentication attempt using [LDAP] external user directory if any of the roles that are \n'
        'are mapped from [LDAP] but are not present locally.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.1')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Added = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Added',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL add the privileges provided by the [LDAP] mapped role when the\n'
        'role is not present during user authentication using [LDAP] external user directory\n'
        'as soon as the role is added.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.2')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Removed = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Removed',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL remove the privileges provided by the role from all the\n'
        'users authenticated using [LDAP] external user directory if the [RBAC] role that was mapped\n'
        'as a result of [LDAP] search is removed.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.3')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Readded = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Readded',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL reassign the [RBAC] role and add all the privileges provided by the role\n'
        'when it is re-added after removal for all [LDAP] users authenticated using external user directory\n'
        'for any role that was mapped as a result of [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.4')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedAndAdded_Parallel = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedAndAdded.Parallel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support authenticating users using [LDAP] external user directory\n'
        'when [RBAC] roles that are mapped by [LDAP] groups\n'
        'are added and removed at the same time as [LDAP] user authentications are performed in parallel.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.5')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_New = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.New',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not allow any new roles to be assigned to any\n'
        'users authenticated using [LDAP] external user directory unless the role is specified\n'
        'in the configuration of the external user directory or was mapped as a result of [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.6')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NewPrivilege = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NewPrivilege',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL add new privilege to all the users authenticated using [LDAP] external user directory\n'
        'when new privilege is added to one of the roles that were mapped as a result of [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.7')

RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedPrivilege = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedPrivilege',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL remove privilege from all the users authenticated using [LDAP] external user directory\n'
        'when the privilege that was provided by the mapped role is removed from all the roles \n'
        'that were mapped as a result of [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.5.8')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parallel authentication of users using [LDAP] server\n'
        'when using [LDAP] external user directory that has role mapping enabled.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.1')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.ValidAndInvalid',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support authentication of valid users and\n'
        'prohibit authentication of invalid users using [LDAP] server\n'
        'in parallel without having invalid attempts affecting valid authentications\n'
        'when using [LDAP] external user directory that has role mapping enabled.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.2')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_MultipleServers = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.MultipleServers',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parallel authentication of external [LDAP] users\n'
        'authenticated using multiple [LDAP] external user directories that have\n'
        'role mapping enabled.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.3')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalOnly = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalOnly',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parallel authentication of users defined only locally\n'
        'when one or more [LDAP] external user directories with role mapping\n'
        'are specified in the configuration file.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.4')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalAndMultipleLDAP = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalAndMultipleLDAP',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parallel authentication of local and external [LDAP] users\n'
        'authenticated using multiple [LDAP] external user directories with role mapping enabled.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.5')

RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_SameUser = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.SameUser',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parallel authentication of the same external [LDAP] user\n'
        'authenticated using the same [LDAP] external user directory with role mapping enabled.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='4.6.6')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<bind_dn>` parameter in the `<ldap_servers><server_name>` section\n'
        'of the `config.xml` that SHALL be used to construct the `DN` to bind to.\n'
        'The resulting `DN` SHALL be constructed by replacing all `{user_name}` substrings of the template \n'
        'with the actual user name during each authentication attempt.\n'
        '\n'
        'For example, \n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <ldap_servers>\n'
        '        <my_ldap_server>\n'
        '            <!-- ... -->\n'
        '            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>\n'
        '            <!-- ... -->\n'
        '        </my_ldap_server>\n'
        '    </ldap_servers>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.1.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN_ConflictWith_AuthDN = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN.ConflictWith.AuthDN',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if both `<bind_dn>` and `<auth_dn_prefix>` or `<auth_dn_suffix>` parameters\n'
        'are specified as part of [LDAP] server description in the `<ldap_servers>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.1.2')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `user_dn_detection` sub-section in the `<ldap_servers><server_name>` section\n'
        'of the `config.xml` that SHALL be used to enable detecting the actual user DN of the bound user. \n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.2.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.BaseDN',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `base_dn` parameter in the `user_dn_detection` sub-section in the \n'
        '`<ldap_servers><server_name>` section of the `config.xml` that SHALL specify how \n'
        'to construct the base DN for the LDAP search to detect the actual user DN.\n'
        '\n'
        'For example,\n'
        '\n'
        '```xml\n'
        '<user_dn_detection>\n'
        ' ...\n'
        ' <base_dn>CN=Users,DC=example,DC=com</base_dn>\n'
        '</user_dn_detection>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.2.2')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_Scope = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.Scope',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `scope` parameter in the `user_dn_detection` sub-section in the \n'
        '`<ldap_servers><server_name>` section of the `config.xml` that SHALL the scope of the \n'
        'LDAP search to detect the actual user DN. The `scope` parameter SHALL support the following values\n'
        '\n'
        '* `base`\n'
        '* `one_level`\n'
        '* `children`\n'
        '* `subtree`\n'
        '\n'
        'For example,\n'
        '\n'
        '```xml\n'
        '<user_dn_detection>\n'
        ' ...\n'
        ' <scope>one_level</scope>\n'
        '</user_dn_detection>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.2.3')

RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.SearchFilter',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `search_filter` parameter in the `user_dn_detection` sub-section in the \n'
        '`<ldap_servers><server_name>` section of the `config.xml` that SHALL specify the LDAP search\n'
        'filter used to detect the actual user DN.\n'
        '\n'
        'For example,\n'
        '\n'
        '```xml\n'
        '<user_dn_detection>\n'
        ' ...\n'
        ' <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>\n'
        '</user_dn_detection>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.7.2.4')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Syntax = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Syntax',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `role_mapping` sub-section in the `<user_directories><ldap>` section\n'
        'of the `config.xml`.\n'
        '\n'
        'For example,\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <user_directories>\n'
        '        <ldap>\n'
        '            <!-- ... -->\n'
        '            <role_mapping>\n'
        '                <base_dn>ou=groups,dc=example,dc=com</base_dn>\n'
        '                <attribute>cn</attribute>\n'
        '                <scope>subtree</scope>\n'
        '                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>\n'
        '                <prefix>clickhouse_</prefix>\n'
        '            </role_mapping>\n'
        '        </ldap>\n'
        '    </user_directories>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.1.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_SpecialCharactersEscaping = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SpecialCharactersEscaping',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support properly escaped special XML characters that can be present\n'
        'as part of the values for different configuration parameters inside the\n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` such as\n'
        '\n'
        '* `<search_filter>` parameter\n'
        '* `<prefix>` parameter\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.2.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support multiple `<role_mapping>` sections defined inside the same `<user_directories><ldap>` section \n'
        'of the `config.xml` and all of the `<role_mapping>` sections SHALL be applied.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.3.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections_IdenticalParameters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections.IdenticalParameters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not duplicate mapped roles when multiple `<role_mapping>` sections \n'
        'with identical parameters are defined inside the `<user_directories><ldap>` section \n'
        'of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.3.2')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_BaseDN = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.BaseDN',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<base_dn>` parameter in the `<user_directories><ldap><role_mapping>` section \n'
        'of the `config.xml` that SHALL specify the template to be used to construct the base `DN` for the [LDAP] search.\n'
        '\n'
        'The resulting `DN` SHALL be constructed by replacing all the `{user_name}`, `{bind_dn}`, and `user_dn` substrings of \n'
        'the template with the actual user name and bind `DN` during each [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.4.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Attribute = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Attribute',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<attribute>` parameter in the `<user_directories><ldap><role_mapping>` section of \n'
        'the `config.xml` that SHALL specify the name of the attribute whose values SHALL be returned by the [LDAP] search.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.5.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<scope>` parameter in the `<user_directories><ldap><role_mapping>` section of \n'
        'the `config.xml` that SHALL define the scope of the LDAP search as defined \n'
        'by the https://ldapwiki.com/wiki/LDAP%20Search%20Scopes.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Base = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Base',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `base` value for the the `<scope>` parameter in the \n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL\n'
        'limit the scope as specified by the https://ldapwiki.com/wiki/BaseObject.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.2')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_OneLevel = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.OneLevel',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `one_level` value for the the `<scope>` parameter in the \n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL\n'
        'limit the scope as specified by the https://ldapwiki.com/wiki/SingleLevel.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.3')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Children = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Children',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `children` value for the the `<scope>` parameter in the \n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL\n'
        'limit the scope as specified by the https://ldapwiki.com/wiki/SubordinateSubtree.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.4')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Subtree = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Subtree',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `children` value for the the `<scope>` parameter in the \n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL\n'
        'limit the scope as specified by the https://ldapwiki.com/wiki/WholeSubtree.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.5')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Default = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Default',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `subtree` as the default value for the the `<scope>` parameter in the \n'
        '`<user_directories><ldap><role_mapping>` section of the `config.xml` when the `<scope>` parameter is not specified.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.6.6')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_SearchFilter = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SearchFilter',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<search_filter>` parameter in the `<user_directories><ldap><role_mapping>`\n'
        'section of the `config.xml` that SHALL specify the template used to construct \n'
        'the [LDAP filter](https://ldap.com/ldap-filters/) for the search.\n'
        '\n'
        'The resulting filter SHALL be constructed by replacing all `{user_name}`, `{bind_dn}`, `{base_dn}`, and `{user_dn}` substrings \n'
        'of the template with the actual user name, bind `DN`, and base `DN` during each the [LDAP] search.\n'
        ' \n'
        ),
    link=None,
    level=4,
    num='4.8.7.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `<prefix>` parameter in the `<user directories><ldap><role_mapping>`\n'
        'section of the `config.xml` that SHALL be expected to be in front of each string in \n'
        'the original list of strings returned by the [LDAP] search. \n'
        'Prefix SHALL be removed from the original strings and resulting strings SHALL be treated as [RBAC] role names. \n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.8.1')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_Default = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.Default',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support empty string as the default value of the `<prefix>` parameter in \n'
        'the `<user directories><ldap><role_mapping>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.8.2')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithUTF8Characters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithUTF8Characters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support UTF8 characters as the value of the `<prefix>` parameter in\n'
        'the `<user directories><ldap><role_mapping>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.8.3')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialXMLCharacters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialXMLCharacters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support XML special characters as the value of the `<prefix>` parameter in\n'
        'the `<user directories><ldap><role_mapping>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.8.4')

RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialRegexCharacters = Requirement(
    name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialRegexCharacters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support regex special characters as the value of the `<prefix>` parameter in\n'
        'the `<user directories><ldap><role_mapping>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='4.8.8.5')

SRS_014_ClickHouse_LDAP_Role_Mapping = Specification(
    name='SRS-014 ClickHouse LDAP Role Mapping', 
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
        Heading(name='General', level=2, num='4.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping', level=3, num='4.1.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.WithFixedRoles', level=3, num='4.1.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Search', level=3, num='4.1.3'),
        Heading(name='Mapped Role Names', level=2, num='4.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithUTF8Characters', level=3, num='4.2.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.Long', level=3, num='4.2.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialXMLCharacters', level=3, num='4.2.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialRegexCharacters', level=3, num='4.2.4'),
        Heading(name='Multiple Roles', level=2, num='4.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Map.MultipleRoles', level=3, num='4.3.1'),
        Heading(name='LDAP Groups', level=2, num='4.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.Removed', level=3, num='4.4.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.RemovedAndAdded.Parallel', level=3, num='4.4.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemoved', level=3, num='4.4.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemovedAndAdded.Parallel', level=3, num='4.4.4'),
        Heading(name='RBAC Roles', level=2, num='4.5'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NotPresent', level=3, num='4.5.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Added', level=3, num='4.5.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Removed', level=3, num='4.5.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Readded', level=3, num='4.5.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedAndAdded.Parallel', level=3, num='4.5.5'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.New', level=3, num='4.5.6'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NewPrivilege', level=3, num='4.5.7'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedPrivilege', level=3, num='4.5.8'),
        Heading(name='Authentication', level=2, num='4.6'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel', level=3, num='4.6.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.ValidAndInvalid', level=3, num='4.6.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.MultipleServers', level=3, num='4.6.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalOnly', level=3, num='4.6.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalAndMultipleLDAP', level=3, num='4.6.5'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.SameUser', level=3, num='4.6.6'),
        Heading(name='Server Configuration', level=2, num='4.7'),
        Heading(name='BindDN Parameter', level=3, num='4.7.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN', level=4, num='4.7.1.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN.ConflictWith.AuthDN', level=4, num='4.7.1.2'),
        Heading(name='User DN Detection', level=3, num='4.7.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection', level=4, num='4.7.2.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.BaseDN', level=4, num='4.7.2.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.Scope', level=4, num='4.7.2.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.SearchFilter', level=4, num='4.7.2.4'),
        Heading(name='External User Directory Configuration', level=2, num='4.8'),
        Heading(name='Syntax', level=3, num='4.8.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Syntax', level=4, num='4.8.1.1'),
        Heading(name='Special Characters Escaping', level=3, num='4.8.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SpecialCharactersEscaping', level=4, num='4.8.2.1'),
        Heading(name='Multiple Sections', level=3, num='4.8.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections', level=4, num='4.8.3.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections.IdenticalParameters', level=4, num='4.8.3.2'),
        Heading(name='BaseDN Parameter', level=3, num='4.8.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.BaseDN', level=4, num='4.8.4.1'),
        Heading(name='Attribute Parameter', level=3, num='4.8.5'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Attribute', level=4, num='4.8.5.1'),
        Heading(name='Scope Parameter', level=3, num='4.8.6'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope', level=4, num='4.8.6.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Base', level=4, num='4.8.6.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.OneLevel', level=4, num='4.8.6.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Children', level=4, num='4.8.6.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Subtree', level=4, num='4.8.6.5'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Default', level=4, num='4.8.6.6'),
        Heading(name='Search Filter Parameter', level=3, num='4.8.7'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SearchFilter', level=4, num='4.8.7.1'),
        Heading(name='Prefix Parameter', level=3, num='4.8.8'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix', level=4, num='4.8.8.1'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.Default', level=4, num='4.8.8.2'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithUTF8Characters', level=4, num='4.8.8.3'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialXMLCharacters', level=4, num='4.8.8.4'),
        Heading(name='RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialRegexCharacters', level=4, num='4.8.8.5'),
        Heading(name='References', level=1, num='5'),
        ),
    requirements=(
        RQ_SRS_014_LDAP_RoleMapping,
        RQ_SRS_014_LDAP_RoleMapping_WithFixedRoles,
        RQ_SRS_014_LDAP_RoleMapping_Search,
        RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithUTF8Characters,
        RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_Long,
        RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialXMLCharacters,
        RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialRegexCharacters,
        RQ_SRS_014_LDAP_RoleMapping_Map_MultipleRoles,
        RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_Removed,
        RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_RemovedAndAdded_Parallel,
        RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemoved,
        RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemovedAndAdded_Parallel,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NotPresent,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Added,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Removed,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Readded,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedAndAdded_Parallel,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_New,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NewPrivilege,
        RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedPrivilege,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_MultipleServers,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalOnly,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalAndMultipleLDAP,
        RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_SameUser,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN_ConflictWith_AuthDN,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_Scope,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Syntax,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_SpecialCharactersEscaping,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections_IdenticalParameters,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_BaseDN,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Attribute,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Base,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_OneLevel,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Children,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Subtree,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Scope_Value_Default,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_SearchFilter,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_Default,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithUTF8Characters,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialXMLCharacters,
        RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialRegexCharacters,
        ),
    content='''
# SRS-014 ClickHouse LDAP Role Mapping
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
  * 3.1 [LDAP](#ldap)
* 4 [Requirements](#requirements)
  * 4.1 [General](#general)
    * 4.1.1 [RQ.SRS-014.LDAP.RoleMapping](#rqsrs-014ldaprolemapping)
    * 4.1.2 [RQ.SRS-014.LDAP.RoleMapping.WithFixedRoles](#rqsrs-014ldaprolemappingwithfixedroles)
    * 4.1.3 [RQ.SRS-014.LDAP.RoleMapping.Search](#rqsrs-014ldaprolemappingsearch)
  * 4.2 [Mapped Role Names](#mapped-role-names)
    * 4.2.1 [RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithUTF8Characters](#rqsrs-014ldaprolemappingmaprolenamewithutf8characters)
    * 4.2.2 [RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.Long](#rqsrs-014ldaprolemappingmaprolenamelong)
    * 4.2.3 [RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialXMLCharacters](#rqsrs-014ldaprolemappingmaprolenamewithspecialxmlcharacters)
    * 4.2.4 [RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialRegexCharacters](#rqsrs-014ldaprolemappingmaprolenamewithspecialregexcharacters)
  * 4.3 [Multiple Roles](#multiple-roles)
    * 4.3.1 [RQ.SRS-014.LDAP.RoleMapping.Map.MultipleRoles](#rqsrs-014ldaprolemappingmapmultipleroles)
  * 4.4 [LDAP Groups](#ldap-groups)
    * 4.4.1 [RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.Removed](#rqsrs-014ldaprolemappingldapgroupremoved)
    * 4.4.2 [RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.RemovedAndAdded.Parallel](#rqsrs-014ldaprolemappingldapgroupremovedandaddedparallel)
    * 4.4.3 [RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemoved](#rqsrs-014ldaprolemappingldapgroupuserremoved)
    * 4.4.4 [RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemovedAndAdded.Parallel](#rqsrs-014ldaprolemappingldapgroupuserremovedandaddedparallel)
  * 4.5 [RBAC Roles](#rbac-roles)
    * 4.5.1 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NotPresent](#rqsrs-014ldaprolemappingrbacrolenotpresent)
    * 4.5.2 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Added](#rqsrs-014ldaprolemappingrbacroleadded)
    * 4.5.3 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Removed](#rqsrs-014ldaprolemappingrbacroleremoved)
    * 4.5.4 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Readded](#rqsrs-014ldaprolemappingrbacrolereadded)
    * 4.5.5 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedAndAdded.Parallel](#rqsrs-014ldaprolemappingrbacroleremovedandaddedparallel)
    * 4.5.6 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.New](#rqsrs-014ldaprolemappingrbacrolenew)
    * 4.5.7 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NewPrivilege](#rqsrs-014ldaprolemappingrbacrolenewprivilege)
    * 4.5.8 [RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedPrivilege](#rqsrs-014ldaprolemappingrbacroleremovedprivilege)
  * 4.6 [Authentication](#authentication)
    * 4.6.1 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel](#rqsrs-014ldaprolemappingauthenticationparallel)
    * 4.6.2 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.ValidAndInvalid](#rqsrs-014ldaprolemappingauthenticationparallelvalidandinvalid)
    * 4.6.3 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.MultipleServers](#rqsrs-014ldaprolemappingauthenticationparallelmultipleservers)
    * 4.6.4 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalOnly](#rqsrs-014ldaprolemappingauthenticationparallellocalonly)
    * 4.6.5 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalAndMultipleLDAP](#rqsrs-014ldaprolemappingauthenticationparallellocalandmultipleldap)
    * 4.6.6 [RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.SameUser](#rqsrs-014ldaprolemappingauthenticationparallelsameuser)
  * 4.7 [Server Configuration](#server-configuration)
    * 4.7.1 [BindDN Parameter](#binddn-parameter)
      * 4.7.1.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN](#rqsrs-014ldaprolemappingconfigurationserverbinddn)
      * 4.7.1.2 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN.ConflictWith.AuthDN](#rqsrs-014ldaprolemappingconfigurationserverbinddnconflictwithauthdn)
    * 4.7.2 [User DN Detection](#user-dn-detection)
      * 4.7.2.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection](#rqsrs-014ldaprolemappingconfigurationserveruserdndetection)
      * 4.7.2.2 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.BaseDN](#rqsrs-014ldaprolemappingconfigurationserveruserdndetectionbasedn)
      * 4.7.2.3 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.Scope](#rqsrs-014ldaprolemappingconfigurationserveruserdndetectionscope)
      * 4.7.2.4 [RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.SearchFilter](#rqsrs-014ldaprolemappingconfigurationserveruserdndetectionsearchfilter)
  * 4.8 [External User Directory Configuration](#external-user-directory-configuration)
    * 4.8.1 [Syntax](#syntax)
      * 4.8.1.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Syntax](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingsyntax)
    * 4.8.2 [Special Characters Escaping](#special-characters-escaping)
      * 4.8.2.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SpecialCharactersEscaping](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingspecialcharactersescaping)
    * 4.8.3 [Multiple Sections](#multiple-sections)
      * 4.8.3.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingmultiplesections)
      * 4.8.3.2 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections.IdenticalParameters](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingmultiplesectionsidenticalparameters)
    * 4.8.4 [BaseDN Parameter](#basedn-parameter)
      * 4.8.4.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.BaseDN](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingbasedn)
    * 4.8.5 [Attribute Parameter](#attribute-parameter)
      * 4.8.5.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Attribute](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingattribute)
    * 4.8.6 [Scope Parameter](#scope-parameter)
      * 4.8.6.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscope)
      * 4.8.6.2 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Base](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscopevaluebase)
      * 4.8.6.3 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.OneLevel](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscopevalueonelevel)
      * 4.8.6.4 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Children](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscopevaluechildren)
      * 4.8.6.5 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Subtree](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscopevaluesubtree)
      * 4.8.6.6 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Default](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingscopevaluedefault)
    * 4.8.7 [Search Filter Parameter](#search-filter-parameter)
      * 4.8.7.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SearchFilter](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingsearchfilter)
    * 4.8.8 [Prefix Parameter](#prefix-parameter)
      * 4.8.8.1 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingprefix)
      * 4.8.8.2 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.Default](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingprefixdefault)
      * 4.8.8.3 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithUTF8Characters](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingprefixwithutf8characters)
      * 4.8.8.4 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialXMLCharacters](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingprefixwithspecialxmlcharacters)
      * 4.8.8.5 [RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialRegexCharacters](#rqsrs-014ldaprolemappingconfigurationuserdirectoryrolemappingprefixwithspecialregexcharacters)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

The [SRS-007 ClickHouse Authentication of Users via LDAP] added support for authenticating
users using an [LDAP] server and the [SRS-009 ClickHouse LDAP External User Directory] added
support for authenticating users using an [LDAP] external user directory. 

This requirements specification adds additional functionality for mapping [LDAP] groups to 
the corresponding [ClickHouse] [RBAC] roles when [LDAP] external user directory is configured.
This functionality will enable easier access management for [LDAP] authenticated users
as the privileges granted by the roles can be granted or revoked by granting or revoking
a corresponding [LDAP] group to one or more [LDAP] users.

For the use case when only [LDAP] user authentication is used, the roles can be
managed using [RBAC] in the same way as for non-[LDAP] authenticated users.

## Terminology

### LDAP

* Lightweight Directory Access Protocol

## Requirements

### General

#### RQ.SRS-014.LDAP.RoleMapping
version: 1.0

[ClickHouse] SHALL support mapping of [LDAP] groups to [RBAC] roles
for users authenticated using [LDAP] external user directory.

#### RQ.SRS-014.LDAP.RoleMapping.WithFixedRoles
version: 1.0

[ClickHouse] SHALL support mapping of [LDAP] groups to [RBAC] roles
for users authenticated using [LDAP] external user directory when
one or more roles are specified in the `<roles>` section.

#### RQ.SRS-014.LDAP.RoleMapping.Search
version: 1.0

[ClickHouse] SHALL perform search on the [LDAP] server and map the results to [RBAC] role names 
when authenticating users using the [LDAP] external user directory if the `<role_mapping>` section is configured
as part of the [LDAP] external user directory. The matched roles SHALL be assigned to the user.

### Mapped Role Names

#### RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithUTF8Characters
version: 1.0

[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory
to an [RBAC] role that contains UTF-8 characters.

#### RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.Long
version: 1.0

[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory
to an [RBAC] role that has a name with more than 128 characters.

#### RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialXMLCharacters
version: 1.0

[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory
to an [RBAC] role that has a name that contains special characters that need to be escaped in XML.

#### RQ.SRS-014.LDAP.RoleMapping.Map.Role.Name.WithSpecialRegexCharacters
version: 1.0

[ClickHouse] SHALL support mapping [LDAP] search results for users authenticated using [LDAP] external user directory
to an [RBAC] role that has a name that contains special characters that need to be escaped in regex.

### Multiple Roles

#### RQ.SRS-014.LDAP.RoleMapping.Map.MultipleRoles
version: 1.0

[ClickHouse] SHALL support mapping one or more [LDAP] search results for users authenticated using 
[LDAP] external user directory to one or more [RBAC] role.

### LDAP Groups

#### RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.Removed
version: 1.0

[ClickHouse] SHALL not assign [RBAC] role(s) for any users authenticated using [LDAP] external user directory
if the corresponding [LDAP] group(s) that map those role(s) are removed. Any users that have active sessions SHALL still
have privileges provided by the role(s) until the next time they are authenticated.

#### RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.RemovedAndAdded.Parallel
version: 1.0

[ClickHouse] SHALL support authenticating users using [LDAP] external user directory 
when [LDAP] groups are removed and added 
at the same time as [LDAP] user authentications are performed in parallel.

#### RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemoved
version: 1.0

[ClickHouse] SHALL not assign [RBAC] role(s) for the user authenticated using [LDAP] external user directory
if the user has been removed from the corresponding [LDAP] group(s) that map those role(s). 
Any active user sessions SHALL have privileges provided by the role(s) until the next time the user is authenticated.

#### RQ.SRS-014.LDAP.RoleMapping.LDAP.Group.UserRemovedAndAdded.Parallel
version: 1.0

[ClickHouse] SHALL support authenticating users using [LDAP] external user directory
when [LDAP] users are added and removed from [LDAP] groups used to map to [RBAC] roles
at the same time as [LDAP] user authentications are performed in parallel.

### RBAC Roles

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NotPresent
version: 1.0

[ClickHouse] SHALL not reject authentication attempt using [LDAP] external user directory if any of the roles that are 
are mapped from [LDAP] but are not present locally.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Added
version: 1.0

[ClickHouse] SHALL add the privileges provided by the [LDAP] mapped role when the
role is not present during user authentication using [LDAP] external user directory
as soon as the role is added.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Removed
version: 1.0

[ClickHouse] SHALL remove the privileges provided by the role from all the
users authenticated using [LDAP] external user directory if the [RBAC] role that was mapped
as a result of [LDAP] search is removed.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.Readded
version: 1.0

[ClickHouse] SHALL reassign the [RBAC] role and add all the privileges provided by the role
when it is re-added after removal for all [LDAP] users authenticated using external user directory
for any role that was mapped as a result of [LDAP] search.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedAndAdded.Parallel
version: 1.0

[ClickHouse] SHALL support authenticating users using [LDAP] external user directory
when [RBAC] roles that are mapped by [LDAP] groups
are added and removed at the same time as [LDAP] user authentications are performed in parallel.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.New
version: 1.0

[ClickHouse] SHALL not allow any new roles to be assigned to any
users authenticated using [LDAP] external user directory unless the role is specified
in the configuration of the external user directory or was mapped as a result of [LDAP] search.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.NewPrivilege
version: 1.0

[ClickHouse] SHALL add new privilege to all the users authenticated using [LDAP] external user directory
when new privilege is added to one of the roles that were mapped as a result of [LDAP] search.

#### RQ.SRS-014.LDAP.RoleMapping.RBAC.Role.RemovedPrivilege
version: 1.0

[ClickHouse] SHALL remove privilege from all the users authenticated using [LDAP] external user directory
when the privilege that was provided by the mapped role is removed from all the roles 
that were mapped as a result of [LDAP] search.

### Authentication

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel
version: 1.0

[ClickHouse] SHALL support parallel authentication of users using [LDAP] server
when using [LDAP] external user directory that has role mapping enabled.

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.ValidAndInvalid
version: 1.0

[ClickHouse] SHALL support authentication of valid users and
prohibit authentication of invalid users using [LDAP] server
in parallel without having invalid attempts affecting valid authentications
when using [LDAP] external user directory that has role mapping enabled.

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.MultipleServers
version: 1.0

[ClickHouse] SHALL support parallel authentication of external [LDAP] users
authenticated using multiple [LDAP] external user directories that have
role mapping enabled.

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalOnly
version: 1.0

[ClickHouse] SHALL support parallel authentication of users defined only locally
when one or more [LDAP] external user directories with role mapping
are specified in the configuration file.

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.LocalAndMultipleLDAP
version: 1.0

[ClickHouse] SHALL support parallel authentication of local and external [LDAP] users
authenticated using multiple [LDAP] external user directories with role mapping enabled.

#### RQ.SRS-014.LDAP.RoleMapping.Authentication.Parallel.SameUser
version: 1.0

[ClickHouse] SHALL support parallel authentication of the same external [LDAP] user
authenticated using the same [LDAP] external user directory with role mapping enabled.

### Server Configuration

#### BindDN Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN
version: 1.0

[ClickHouse] SHALL support the `<bind_dn>` parameter in the `<ldap_servers><server_name>` section
of the `config.xml` that SHALL be used to construct the `DN` to bind to.
The resulting `DN` SHALL be constructed by replacing all `{user_name}` substrings of the template 
with the actual user name during each authentication attempt.

For example, 

```xml
<yandex>
    <ldap_servers>
        <my_ldap_server>
            <!-- ... -->
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <!-- ... -->
        </my_ldap_server>
    </ldap_servers>
</yandex>
```

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.BindDN.ConflictWith.AuthDN
version: 1.0

[ClickHouse] SHALL return an error if both `<bind_dn>` and `<auth_dn_prefix>` or `<auth_dn_suffix>` parameters
are specified as part of [LDAP] server description in the `<ldap_servers>` section of the `config.xml`.

#### User DN Detection

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection
version: 1.0

[ClickHouse] SHALL support the `user_dn_detection` sub-section in the `<ldap_servers><server_name>` section
of the `config.xml` that SHALL be used to enable detecting the actual user DN of the bound user. 

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.BaseDN
version: 1.0

[ClickHouse] SHALL support `base_dn` parameter in the `user_dn_detection` sub-section in the 
`<ldap_servers><server_name>` section of the `config.xml` that SHALL specify how 
to construct the base DN for the LDAP search to detect the actual user DN.

For example,

```xml
<user_dn_detection>
 ...
 <base_dn>CN=Users,DC=example,DC=com</base_dn>
</user_dn_detection>
```

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.Scope
version: 1.0

[ClickHouse] SHALL support `scope` parameter in the `user_dn_detection` sub-section in the 
`<ldap_servers><server_name>` section of the `config.xml` that SHALL the scope of the 
LDAP search to detect the actual user DN. The `scope` parameter SHALL support the following values

* `base`
* `one_level`
* `children`
* `subtree`

For example,

```xml
<user_dn_detection>
 ...
 <scope>one_level</scope>
</user_dn_detection>
```

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.Server.UserDNDetection.SearchFilter
version: 1.0

[ClickHouse] SHALL support `search_filter` parameter in the `user_dn_detection` sub-section in the 
`<ldap_servers><server_name>` section of the `config.xml` that SHALL specify the LDAP search
filter used to detect the actual user DN.

For example,

```xml
<user_dn_detection>
 ...
 <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
</user_dn_detection>
```

### External User Directory Configuration

#### Syntax

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Syntax
version: 1.0

[ClickHouse] SHALL support the `role_mapping` sub-section in the `<user_directories><ldap>` section
of the `config.xml`.

For example,

```xml
<yandex>
    <user_directories>
        <ldap>
            <!-- ... -->
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <attribute>cn</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</yandex>
```

#### Special Characters Escaping

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SpecialCharactersEscaping
version: 1.0

[ClickHouse] SHALL support properly escaped special XML characters that can be present
as part of the values for different configuration parameters inside the
`<user_directories><ldap><role_mapping>` section of the `config.xml` such as

* `<search_filter>` parameter
* `<prefix>` parameter

#### Multiple Sections

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections
version: 1.0

[ClickHouse] SHALL support multiple `<role_mapping>` sections defined inside the same `<user_directories><ldap>` section 
of the `config.xml` and all of the `<role_mapping>` sections SHALL be applied.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.MultipleSections.IdenticalParameters
version: 1.0

[ClickHouse] SHALL not duplicate mapped roles when multiple `<role_mapping>` sections 
with identical parameters are defined inside the `<user_directories><ldap>` section 
of the `config.xml`.

#### BaseDN Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.BaseDN
version: 1.0

[ClickHouse] SHALL support the `<base_dn>` parameter in the `<user_directories><ldap><role_mapping>` section 
of the `config.xml` that SHALL specify the template to be used to construct the base `DN` for the [LDAP] search.

The resulting `DN` SHALL be constructed by replacing all the `{user_name}`, `{bind_dn}`, and `user_dn` substrings of 
the template with the actual user name and bind `DN` during each [LDAP] search.

#### Attribute Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Attribute
version: 1.0

[ClickHouse] SHALL support the `<attribute>` parameter in the `<user_directories><ldap><role_mapping>` section of 
the `config.xml` that SHALL specify the name of the attribute whose values SHALL be returned by the [LDAP] search.

#### Scope Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope
version: 1.0

[ClickHouse] SHALL support the `<scope>` parameter in the `<user_directories><ldap><role_mapping>` section of 
the `config.xml` that SHALL define the scope of the LDAP search as defined 
by the https://ldapwiki.com/wiki/LDAP%20Search%20Scopes.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Base
version: 1.0

[ClickHouse] SHALL support the `base` value for the the `<scope>` parameter in the 
`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL
limit the scope as specified by the https://ldapwiki.com/wiki/BaseObject.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.OneLevel
version: 1.0

[ClickHouse] SHALL support the `one_level` value for the the `<scope>` parameter in the 
`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL
limit the scope as specified by the https://ldapwiki.com/wiki/SingleLevel.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Children
version: 1.0

[ClickHouse] SHALL support the `children` value for the the `<scope>` parameter in the 
`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL
limit the scope as specified by the https://ldapwiki.com/wiki/SubordinateSubtree.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Subtree
version: 1.0

[ClickHouse] SHALL support the `children` value for the the `<scope>` parameter in the 
`<user_directories><ldap><role_mapping>` section of the `config.xml` that SHALL
limit the scope as specified by the https://ldapwiki.com/wiki/WholeSubtree.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Scope.Value.Default
version: 1.0

[ClickHouse] SHALL support the `subtree` as the default value for the the `<scope>` parameter in the 
`<user_directories><ldap><role_mapping>` section of the `config.xml` when the `<scope>` parameter is not specified.

#### Search Filter Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.SearchFilter
version: 1.0

[ClickHouse] SHALL support the `<search_filter>` parameter in the `<user_directories><ldap><role_mapping>`
section of the `config.xml` that SHALL specify the template used to construct 
the [LDAP filter](https://ldap.com/ldap-filters/) for the search.

The resulting filter SHALL be constructed by replacing all `{user_name}`, `{bind_dn}`, `{base_dn}`, and `{user_dn}` substrings 
of the template with the actual user name, bind `DN`, and base `DN` during each the [LDAP] search.
 
#### Prefix Parameter

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix
version: 1.0

[ClickHouse] SHALL support the `<prefix>` parameter in the `<user directories><ldap><role_mapping>`
section of the `config.xml` that SHALL be expected to be in front of each string in 
the original list of strings returned by the [LDAP] search. 
Prefix SHALL be removed from the original strings and resulting strings SHALL be treated as [RBAC] role names. 

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.Default
version: 1.0

[ClickHouse] SHALL support empty string as the default value of the `<prefix>` parameter in 
the `<user directories><ldap><role_mapping>` section of the `config.xml`.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithUTF8Characters
version: 1.0

[ClickHouse] SHALL support UTF8 characters as the value of the `<prefix>` parameter in
the `<user directories><ldap><role_mapping>` section of the `config.xml`.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialXMLCharacters
version: 1.0

[ClickHouse] SHALL support XML special characters as the value of the `<prefix>` parameter in
the `<user directories><ldap><role_mapping>` section of the `config.xml`.

##### RQ.SRS-014.LDAP.RoleMapping.Configuration.UserDirectory.RoleMapping.Prefix.WithSpecialRegexCharacters
version: 1.0

[ClickHouse] SHALL support regex special characters as the value of the `<prefix>` parameter in
the `<user directories><ldap><role_mapping>` section of the `config.xml`.

## References

* **Access Control and Account Management**: https://clickhouse.tech/docs/en/operations/access-rights/
* **LDAP**: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
* **ClickHouse:** https://clickhouse.tech
* **GitHub Repository**: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/role_mapping/requirements/requirements.md
* **Revision History**: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/ldap/role_mapping/requirements/requirements.md 
* **Git:** https://git-scm.com/

[RBAC]: https://clickhouse.tech/docs/en/operations/access-rights/
[SRS]: #srs
[Access Control and Account Management]: https://clickhouse.tech/docs/en/operations/access-rights/
[SRS-009 ClickHouse LDAP External User Directory]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/external_user_directory/requirements/requirements.md
[SRS-007 ClickHouse Authentication of Users via LDAP]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/authentication/requirements/requirements.md
[LDAP]: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
[ClickHouse]: https://clickhouse.tech
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/role_mapping/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/ldap/role_mapping/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
''')

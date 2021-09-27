# QA-SRS016 ClickHouse Kerberos Authentication
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
    * 4.1.1 [RQ.SRS-016.Kerberos](#rqsrs-016kerberos)
  * 4.2 [Configuration](#configuration)
    * 4.2.1 [RQ.SRS-016.Kerberos.Configuration.MultipleAuthMethods](#rqsrs-016kerberosconfigurationmultipleauthmethods)
    * 4.2.2 [RQ.SRS-016.Kerberos.Configuration.KerberosNotEnabled](#rqsrs-016kerberosconfigurationkerberosnotenabled)
    * 4.2.3 [RQ.SRS-016.Kerberos.Configuration.MultipleKerberosSections](#rqsrs-016kerberosconfigurationmultiplekerberossections)
    * 4.2.4 [RQ.SRS-016.Kerberos.Configuration.WrongUserRealm](#rqsrs-016kerberosconfigurationwronguserrealm)
    * 4.2.5 [RQ.SRS-016.Kerberos.Configuration.PrincipalAndRealmSpecified](#rqsrs-016kerberosconfigurationprincipalandrealmspecified)
    * 4.2.6 [RQ.SRS-016.Kerberos.Configuration.MultiplePrincipalSections](#rqsrs-016kerberosconfigurationmultipleprincipalsections)
    * 4.2.7 [RQ.SRS-016.Kerberos.Configuration.MultipleRealmSections](#rqsrs-016kerberosconfigurationmultiplerealmsections)
  * 4.3 [Valid User](#valid-user)
    * 4.3.1 [RQ.SRS-016.Kerberos.ValidUser.XMLConfiguredUser](#rqsrs-016kerberosvaliduserxmlconfigureduser)
    * 4.3.2 [RQ.SRS-016.Kerberos.ValidUser.RBACConfiguredUser](#rqsrs-016kerberosvaliduserrbacconfigureduser)
    * 4.3.3 [RQ.SRS-016.Kerberos.ValidUser.KerberosNotConfigured](#rqsrs-016kerberosvaliduserkerberosnotconfigured)
  * 4.4 [Invalid User](#invalid-user)
    * 4.4.1 [RQ.SRS-016.Kerberos.InvalidUser](#rqsrs-016kerberosinvaliduser)
    * 4.4.2 [RQ.SRS-016.Kerberos.InvalidUser.UserDeleted](#rqsrs-016kerberosinvaliduseruserdeleted)
  * 4.5 [Kerberos Not Available](#kerberos-not-available)
    * 4.5.1 [RQ.SRS-016.Kerberos.KerberosNotAvailable.InvalidServerTicket](#rqsrs-016kerberoskerberosnotavailableinvalidserverticket)
    * 4.5.2 [RQ.SRS-016.Kerberos.KerberosNotAvailable.InvalidClientTicket](#rqsrs-016kerberoskerberosnotavailableinvalidclientticket)
    * 4.5.3 [RQ.SRS-016.Kerberos.KerberosNotAvailable.ValidTickets](#rqsrs-016kerberoskerberosnotavailablevalidtickets)
  * 4.6 [Kerberos Restarted](#kerberos-restarted)
    * 4.6.1 [RQ.SRS-016.Kerberos.KerberosServerRestarted](#rqsrs-016kerberoskerberosserverrestarted)
  * 4.7 [Performance](#performance)
    * 4.7.1 [RQ.SRS-016.Kerberos.Performance](#rqsrs-016kerberosperformance)
  * 4.8 [Parallel Requests processing](#parallel-requests-processing)
    * 4.8.1 [RQ.SRS-016.Kerberos.Parallel](#rqsrs-016kerberosparallel)
    * 4.8.2 [RQ.SRS-016.Kerberos.Parallel.ValidRequests.KerberosAndNonKerberos](#rqsrs-016kerberosparallelvalidrequestskerberosandnonkerberos)
    * 4.8.3 [RQ.SRS-016.Kerberos.Parallel.ValidRequests.SameCredentials](#rqsrs-016kerberosparallelvalidrequestssamecredentials)
    * 4.8.4 [RQ.SRS-016.Kerberos.Parallel.ValidRequests.DifferentCredentials](#rqsrs-016kerberosparallelvalidrequestsdifferentcredentials)
    * 4.8.5 [RQ.SRS-016.Kerberos.Parallel.ValidInvalid](#rqsrs-016kerberosparallelvalidinvalid)
    * 4.8.6 [RQ.SRS-016.Kerberos.Parallel.Deletion](#rqsrs-016kerberosparalleldeletion)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].  
All the updates are tracked using the [Git]'s [Revision History].

## Introduction

This document specifies the behavior for authenticating existing users via [Kerberos] authentication protocol.
Existing [ClickHouse] users, that are properly configured, have an ability to authenticate using [Kerberos]. Kerberos authentication is only supported for HTTP requests, and users configured to authenticate via Kerberos cannot be authenticated by any other means of authentication.

In order to use Kerberos authentication, Kerberos needs to be properly configured in the environment: Kerberos server must be present and user's and server's credentials must be set up. Configuring the Kerberos environment is outside the scope of this document.

## Terminology

* **Principal** -
  A unique identity that uses [Kerberos].

* **Realm** -
  A logical group of resources and identities that use [Kerberos].

* **Ticket** -
  An encrypted block of data that authenticates principal.

* **Credentials** -
  A Kerberos ticket and a session key.

* **Kerberized request** -
  A HTTP query to ClickHouse server, which uses GSS [SPNEGO] and [Kerberos] to authenticate client.

* **Unkerberized request** -
  A HTTP query to ClickHouse server, which uses any other mean of authentication than GSS [SPNEGO] or [Kerberos].

For a more detailed descriprion, visit [Kerberos terminology].

## Requirements

### Generic

#### RQ.SRS-016.Kerberos
version: 1.0

[ClickHouse] SHALL support user authentication using [Kerberos] server.

### Configuration

#### RQ.SRS-016.Kerberos.Configuration.MultipleAuthMethods
version: 1.0

[ClickHouse] SHALL generate an exception and TERMINATE in case some user in `users.xml` has a `<kerberos>` section specified alongside with any other authentication method's section, e.g. `ldap`, `password`.

#### RQ.SRS-016.Kerberos.Configuration.KerberosNotEnabled
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication in case user is properly configured for using Kerberos, but Kerberos itself is not enabled in `config.xml`. For example:

```xml
<yandex>
    <!- ... -->
    <kerberos />
</yandex>
```
```xml
<yandex>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</yandex>
```
```xml
<yandex>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</yandex>
```

#### RQ.SRS-016.Kerberos.Configuration.MultipleKerberosSections
version: 1.0

[ClickHouse] SHALL disable [Kerberos] and reject [Kerberos] authentication in case multiple `kerberos` sections are present in `config.xml`.

#### RQ.SRS-016.Kerberos.Configuration.WrongUserRealm
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if user's realm specified in `users.xml` doesn't match the realm of the principal trying to authenticate.

#### RQ.SRS-016.Kerberos.Configuration.PrincipalAndRealmSpecified
version: 1.0

[ClickHouse] SHALL generate an exception and disable [Kerberos] in case both `realm` and `principal` sections are defined in `config.xml`.

#### RQ.SRS-016.Kerberos.Configuration.MultiplePrincipalSections
version: 1.0

[ClickHouse] SHALL generate an exception and disable [Kerberos] in case multiple `principal` sections are specified inside `kerberos` section in `config.xml`.

#### RQ.SRS-016.Kerberos.Configuration.MultipleRealmSections
version: 1.0

[ClickHouse] SHALL generate an exception and disable [Kerberos] in case multiple `realm` sections are specified inside `kerberos` section in `config.xml`.

### Valid User

#### RQ.SRS-016.Kerberos.ValidUser.XMLConfiguredUser
version: 1.0

[ClickHouse] SHALL accept [Kerberos] authentication for a user that is configured in `users.xml` and has [Kerberos] enabled, i.e.:

```xml
<yandex>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</yandex>
```

#### RQ.SRS-016.Kerberos.ValidUser.RBACConfiguredUser
version: 1.0

[ClickHouse] SHALL accept [Kerberos] authentication if user is configured to authenticate via [Kerberos] using SQL queries

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

or

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```

#### RQ.SRS-016.Kerberos.ValidUser.KerberosNotConfigured
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if username is valid but [ClickHouse] user is not configured to be authenticated using [Kerberos].

### Invalid User

#### RQ.SRS-016.Kerberos.InvalidUser
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if name of the principal attempting to authenticate does not translate to a valid [ClickHouse] username configured in `users.xml` or via SQL workflow.

#### RQ.SRS-016.Kerberos.InvalidUser.UserDeleted
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if [ClickHouse] user was removed from the database using an SQL query.

### Kerberos Not Available

#### RQ.SRS-016.Kerberos.KerberosNotAvailable.InvalidServerTicket
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if [ClickHouse] user is configured to be authenticated using [Kerberos] and [Kerberos] server is unavailable, but [ClickHouse] doesn't have a valid Kerberos ticket or the ticket is expired.

#### RQ.SRS-016.Kerberos.KerberosNotAvailable.InvalidClientTicket
version: 1.0

[ClickHouse] SHALL reject [Kerberos] authentication if [ClickHouse] user is configured to to be authenticated using [Kerberos] and [Kerberos] server is unavailable, but the client doesn't have a valid Kerberos ticket or the ticket is expired.

#### RQ.SRS-016.Kerberos.KerberosNotAvailable.ValidTickets
version: 1.0

[ClickHouse] SHALL accept [Kerberos] authentication if no [Kerberos] server is reachable, but [ClickHouse] is configured to use valid credentials and [ClickHouse] has already processed some valid kerberized request (so it was granted a ticket), and the client has a valid ticket as well.

### Kerberos Restarted

#### RQ.SRS-016.Kerberos.KerberosServerRestarted
version: 1.0

[ClickHouse] SHALL accept [Kerberos] authentication if [Kerberos] server was restarted.

### Performance

#### RQ.SRS-016.Kerberos.Performance
version: 1.0

[ClickHouse]'s performance for [Kerberos] authentication SHALL be comparable to regular authentication.

### Parallel Requests processing

#### RQ.SRS-016.Kerberos.Parallel
version: 1.0

[ClickHouse] SHALL support parallel authentication using [Kerberos].

#### RQ.SRS-016.Kerberos.Parallel.ValidRequests.KerberosAndNonKerberos
version: 1.0

[ClickHouse] SHALL support processing of simultaneous kerberized (for users configured to authenticate via [Kerberos]) and non-kerberized (for users configured to authenticate with any other means) requests.

#### RQ.SRS-016.Kerberos.Parallel.ValidRequests.SameCredentials
version: 1.0

[ClickHouse] SHALL support processing of simultaneously sent [Kerberos] requests under the same credentials.

#### RQ.SRS-016.Kerberos.Parallel.ValidRequests.DifferentCredentials
version: 1.0

[ClickHouse] SHALL support processing of simultaneously sent [Kerberos] requests under different credentials.

#### RQ.SRS-016.Kerberos.Parallel.ValidInvalid
version: 1.0

[ClickHouse] SHALL support parallel authentication of users using [Kerberos] server, some of which are valid and some invalid. Valid users' authentication should not be affected by invalid users' attempts.

#### RQ.SRS-016.Kerberos.Parallel.Deletion
version: 1.0

[ClickHouse] SHALL not crash when two or more [Kerberos] users are simultaneously deleting one another.

## References

* **ClickHouse:** https://clickhouse.tech
* **GitHub Repository:** https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/kerberos/requirements/requirements.md
* **Revision History:** https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/kerberos/requirements/requirements.md
* **Git:** https://git-scm.com/
* **Kerberos terminology:** https://web.mit.edu/kerberos/kfw-4.1/kfw-4.1/kfw-4.1-help/html/kerberos_terminology.htm

[Kerberos]: https://en.wikipedia.org/wiki/Kerberos_(protocol)
[SPNEGO]: https://en.wikipedia.org/wiki/SPNEGO
[ClickHouse]: https://clickhouse.tech
[GitHub]: https://gitlab.com
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/kerberos/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/kerberos/requirements/requirements.md
[Git]: https://git-scm.com/
[Kerberos terminology]: https://web.mit.edu/kerberos/kfw-4.1/kfw-4.1/kfw-4.1-help/html/kerberos_terminology.htm


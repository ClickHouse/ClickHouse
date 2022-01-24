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
<clickhouse>
    <user_directories>
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
        </ldap>
    </user_directories>
</clickhouse>
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

* **Access Control and Account Management**: https://clickhouse.com/docs/en/operations/access-rights/
* **LDAP**: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
* **ClickHouse:** https://clickhouse.com

[SRS]: #srs
[Access Control and Account Management]: https://clickhouse.com/docs/en/operations/access-rights/
[SRS-007 ClickHouse Authentication of Users via LDAP]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/authentication/requirements/requirements.md
[LDAP]: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
[ClickHouse]: https://clickhouse.com
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/ldap/external_user_directory/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/ldap/external_user_directory/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com

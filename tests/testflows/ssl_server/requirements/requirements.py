# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.7.210802.1144954.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_017_ClickHouse_Security_SSL_Server_HTTPS_Port = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.HTTPS.Port',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support acting as a [SSL] server to provide secure client connections\n'
        'over the [HTTPS] port.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_TCP_Port = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.TCP.Port',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support acting as a [SSL] server to provide secure client connections\n'
        'over secure [TCP] port.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling protocols using\n'
        'the `<disbaleProtocols>` parameter in the `<yandex><openSSL><server>` section\n'
        'of the config.xml. Multiple protocols SHALL be specified using the comma\n'
        '`,` character as a separator. Any handshakes using one of the\n'
        'disabled protocol SHALL fail and downgrading to these protocols SHALL not be allowed.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            ...\n'
        '            <disableProtocols>sslv2,sslv3</disableProtocols>\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_SSLv2 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv2',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling vulnerable `SSLv2`\n'
        'using the `sslv2` as the protocol name in the `<disableProtocols>` parameter.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_SSLv3 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv3',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling vulnerable `SSLv3`\n'
        'using the `sslv3` as the protocol name in the `<disableProtocols>` parameter.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.3')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling vulnerable `TLSv1`\n'
        'using the `tlsv1` as the protocol name in the `<disableProtocols>` parameter.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.4')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_1 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_1',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling `TLSv1_1`\n'
        'using the `tlsv1_1` as the protocol name in the `<disableProtocols>` parameter.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.5')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_2 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_2',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support disabling `TLSv1_2`\n'
        'using the `tlsv1_2` as the protocol name in the `<disableProtocols>` parameter.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.6')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_3_NotAllowed = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_3.NotAllowed',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not support disabling `TLSv1_3` protocol and if\n'
        'the `tlsv1_3` is specified as the protocol name in the `<disableProtocols>` parameter\n'
        'then an error SHALL be returned.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.1.7')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLS = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLS',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support requiring specific TLS protocol version\n'
        'using parameters in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` and when specified SHALL only establish\n'
        'the connection if and only if the required protocol version will be used\n'
        'without downgrading.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support requiring `TLSv1` connection\n'
        'using the `<requireTLSv1>` parameter to boolean `true`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.2.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_1 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_1',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support requiring `TLSv1.1` connection\n'
        'using the `<requireTLSv1_1>` parameter to boolean `true`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.2.3')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_2 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_2',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support requiring `TLSv1.2` connection\n'
        'using the `<requireTLSv1_2>` parameter to boolean `true`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.2.4')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_3 = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_3',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support requiring `TLSv1.3` connection\n'
        'using the `<requireTLSv1_3>` parameter to boolean `true`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.2.2.5')

RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying cipher suites to be used\n'
        'for [SSL] connection using the `<cipherList>` parameter\n'
        'in form of a string that SHALL use [OpenSSL cipher list format] notation\n'
        '```\n'
        '(e.g. "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH")\n'
        '```\n'
        'in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` and when specified the server SHALL only establish\n'
        'the connection if and only if one of the specified cipher suites\n'
        'will be used for the connection without downgrading.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            ...\n'
        '            <cipherList>ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH</cipherList>\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.3.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_PreferServerCiphers = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.PreferServerCiphers',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL use own cipher suite preferences when\n'
        'the `<preferServerCiphers>` in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` is set to `true`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            ...\n'
        '            <preferServerCiphers>true</preferServerCiphers>\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.3.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_ECDHCurve = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the curve to be used for [ECDH] key agreement protocol\n'
        'using the `<ecdhCurve>` parameter in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` that SHALL contain one of the name of the\n'
        'the curves specified in the [RFC 4492].\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            ...\n'
        '            <ecdhCurve>prime256v1</ecdhCurve>\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.3.3.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_ECDHCurve_DefaultValue = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve.DefaultValue',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL use the `prime256v1` as the default value\n'
        'for the `<ecdhCurve>` parameter in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` that SHALL force the server to use the corresponding\n'
        'curve for establishing [Shared Secret] using the [ECDH] key agreement protocol.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.3.3.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_FIPS_Mode = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support enabling [FIPS Mode] using the\n'
        '`<fips>` parameter set to `true` in the  `<yandex><openSSL><server>` section\n'
        'of the `config.xml` that SHALL enable [FIPS 140-2] mode of operation\n'
        'if the [SSL] library used by the server supports it. If the\n'
        '[FIPS mode] is not supported by the library\n'
        'then the server SHALL return an error.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            ...\n'
        '        </server>\n'
        '        <fips>true</fips>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_FIPS_Mode_ApprovedSecurityFunctions = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode.ApprovedSecurityFunctions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL only support the use of security functions\n'
        'approved by [FIPS 140-2] when [FIPS Mode] is enabled.\n'
        '\n'
        '* Symmetric Key Encryption and Decryption\n'
        '  * AES\n'
        '  * TDEA\n'
        '\n'
        '* Digital Signatures\n'
        '  * DSS\n'
        '\n'
        '* Secure Hash Standard\n'
        '  * SHA-1\n'
        '  * SHA-224\n'
        '  * SHA-256\n'
        '  * SHA-384\n'
        '  * SHA-512\n'
        '  * SHA-512/224\n'
        '  * SHA-512/256\n'
        '\n'
        '* SHA-3 Standard\n'
        '  * SHA3-224\n'
        '  * SHA3-256\n'
        '  * SHA3-384\n'
        '  * SHA3-512\n'
        '  * SHAKE128\n'
        '  * SHAKE256\n'
        '  * cSHAKE\n'
        '  * KMAC\n'
        '  * TupleHash\n'
        '  * ParallelHash\n'
        '\n'
        '* Message Authentication\n'
        '  * Triple-DES\n'
        '  * AES\n'
        '  * HMAC\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_DH_Parameters = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DH.Parameters',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying a file containing Diffie-Hellman (DH) parameters.\n'
        'using a string as value for the `<dhParamsFile>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml` and\n'
        'if not specified or empty, the default parameters SHALL be used.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <dhParamsFile>dh.pem</dhParamsFile>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.5.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKey = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKey',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying private key as the path to the\n'
        'file containing the private key for the certificate in the [PEM format]\n'
        'or containing both the private key and the certificate using the\n'
        '`<privateKeyFile>` parameter in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '           <privateKeyFile>/path/to/private/key</privateKeyFile>\n'
        '           ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.6.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKeyHandler = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the name of the class that is subclass of `PrivateKeyPassphraseHandler`\n'
        'used for obtaining the passphrase for accessing the private key using a string as a value of the\n'
        '`<privateKeyPassphraseHandler><name>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <privateKeyPassphraseHandler>\n'
        '                <name>KeyFileHandler</name>\n'
        '                ...\n'
        '            </privateKeyPassphraseHandler>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.1.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKeyHandler_Password = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler.Password',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the password to be used by the private key handler using a string\n'
        'as a value of the `<privateKeyPassphraseHandler><options><password>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <privateKeyPassphraseHandler>\n'
        '                <name>KeyFileHandler</name>\n'
        '                <options>\n'
        '                    <password>private key password</password>\n'
        '                </options>\n'
        '            </privateKeyPassphraseHandler>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.1.2.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_Certificate = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.Certificate',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        "[ClickHouse] SHALL support specifying the path to the file containing the server's\n"
        'certificate in the [PEM format] using the `<certificateFile>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml`.\n'
        'When the private key specified by the `<privateKeyFile>` parameter contains\n'
        'the certificate then this parameter SHALL be ignored.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '           <certificateFile>/path/to/the/certificate</certificateFile>\n'
        '           ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.6.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_CAConfig = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.CAConfig',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the path to the file or directory containing the trusted root certificates\n'
        'using the `<caConfig>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '           <caConfig>/path/to/the/file/or/directory/containing/trusted/root/cerificates</caConfig>\n'
        '           ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.3.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_LoadDefaultCAFile = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.LoadDefaultCAFile',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying whether the builtin CA certificates provided by the [SSL]\n'
        'library SHALL be used using the `<loadDefaultCAFile>` parameter with a boolean value in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.3.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying whether and how client certificates are validated\n'
        'using the `<verificationMode>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.1.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_None = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.None',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL not perform client certificate validation when the `<verificationMode>` parameter\n'
        'in the `<yandex><openSSL><server>` section of the `config.xml` is set to `none`\n'
        'by not sending a `client certificate request` to the client so that the client will not send a certificate.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.1.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Relaxed = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Relaxed',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL perform relaxed client certificate validation when the `<verificationMode>` parameter\n'
        'in the `<yandex><openSSL><server>` section of the `config.xml` is set to `relaxed` by\n'
        'sending a `client certificate request` to the client. The certificate SHALL only be checked if client sends it.\n'
        'If the client certificate verification process fails, the TLS/SSL handshake SHALL be immediately\n'
        'terminated with an alert message containing the reason for the verification failure.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.1.3')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Strict = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Strict',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL perform strict client certificate validation when the `<verificationMode>` parameter\n'
        'in the `<yandex><openSSL><server>` section of the `config.xml` is set to `strict` by\n'
        'immediately terminating TLS/SSL handshake if the client did not return a certificate or\n'
        'certificate validation process fails with a handshake failure alert.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.1.4')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Once = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Once',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL perform client certificate validation only once when the `<verificationMode>` parameter\n'
        'in the `<yandex><openSSL><server>` section of the `config.xml` is set to `once`\n'
        'by only requesting a client certificate on the initial [TLS/SSL handshake].\n'
        'During renegotiation client certificate SHALL not be requested and verified again.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.1.5')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationExtended = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationExtended',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support enabling or disabling automatic post-connection extended certificate verification\n'
        'using a `boolean` as a value of the `<extendedVerification>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml` and connection SHALL be aborted\n'
        'if extended certificate verification is enabled and fails.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.2.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationDepth = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationDepth',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the upper limit for the client certificate verification chain size\n'
        'using the `<verificationDepth>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        'Verification SHALL fail if a certificate chain is larger than the value specified by this parameter\n'
        'is encountered.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.4.3.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_InvalidCertificateHandler = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.InvalidCertificateHandler',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the name of the class that is a subclass of `CertificateHandler`\n'
        'that SHALL be used for confirming invalid certificates using a string as the value of the\n'
        '`<invalidCertificateHandler><name>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <invalidCertificateHandler>\n'
        '                <name>ConsoleCertificateHandler</name>\n'
        '            </invalidCertificateHandler>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.6.5.3.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_Cache = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Cache',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support enabling or disabling session caching using a boolean as a value of the\n'
        '`<cacheSessions>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            cacheSessions>true|false</cacheSessions>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_CacheSize = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.CacheSize',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying the maximum size of the server session cache as the number of sessions\n'
        'using an integer as a value of the `<sessionCacheSize>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '* The default size SHALL be `1024*20`.\n'
        '* Specifying a size of 0 SHALL set an unlimited cache size.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <sessionCacheSize>0..n</sessionCacheSize>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.2')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_IdContext = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.IdContext',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support specifying unique session ID context, which SHALL become part of each\n'
        'session identifier generated by the server using a string as a value of the `<sessionIdContext>` parameter\n'
        'in the `<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '* The value SHALL support an arbitrary sequence of bytes with a maximum length of `SSL_MAX_SSL_SESSION_ID_LENGTH`.\n'
        '* This parameter SHALL be specified for a server to enable session caching.\n'
        '* This parameter SHALL be specified even if session caching is disabled to avoid problems with clients that request\n'
        '  session caching (e.g. Firefox 3.6).\n'
        '* If not specified, the default value SHALL be set to `${application.name}`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <sessionIdContext>someString</sessionIdContext>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.3')

RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_Timeout = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Timeout',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support setting the timeout in seconds of sessions cached on the server\n'
        'using an integer as a value of the <sessionTimeout>` parameter in the\n'
        '`<yandex><openSSL><server>` section of the `config.xml`.\n'
        '\n'
        '```xml\n'
        '<yandex>\n'
        '    <openSSL>\n'
        '        <server>\n'
        '            <sessionTimeout>0..n</sessionTimeout>\n'
        '            ...\n'
        '        </server>\n'
        '    </openSSL>\n'
        '</yandex>\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.4')

RQ_SRS_017_ClickHouse_Security_SSL_Server_DynamicContext_Certificate_Reload = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Certificate.Reload',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL reload server SSL certificate specified \n'
        'by the `<certificateFile>` parameter in the `<yandex><openSSL><server>`\n'
        'section of the `config.xml` any time [Dynamic SSL Context] is reloaded. \n'
        'and the reloaded SSL server certificate SHALL be immediately applied\n'
        'to any to new SSL connections made to the server.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.8.5.1')

RQ_SRS_017_ClickHouse_Security_SSL_Server_DynamicContext_PrivateKey_Reload = Requirement(
    name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.PrivateKey.Reload',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL reload server SSL private key specified \n'
        'by the `<privateKeyFile>` parameter in the `<yandex><openSSL><server>` section\n'
        'of the `config.xml` any time [Dynamic SSL Context] is reloaded  \n'
        'and the reloaded SSL private key SHALL be immediately applied\n'
        'to any new SSL connections made to the server.\n'
        '\n'
        '[SRS]: #srs\n'
        '[SSL Protocol]: #ssl-protocol\n'
        '[TLS Protocol]: #tls-protocol\n'
        '[J. Kalsi, D. Mossop]: https://www.contextis.com/en/blog/manually-testing-ssl-tls-weaknesses\n'
        '[RSA]: https://en.wikipedia.org/wiki/RSA_(cryptosystem)\n'
        '[MD5]: https://en.wikipedia.org/wiki/MD5\n'
        '[SHA1]: https://en.wikipedia.org/wiki/SHA-1\n'
        '[SHA2]: https://en.wikipedia.org/wiki/SHA-2\n'
        '[TLS/SSL handshake]: https://en.wikipedia.org/wiki/Transport_Layer_Security#TLS_handshake\n'
        '[PEM format]: https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail#Format\n'
        '[FIPS 140-2]: https://csrc.nist.gov/publications/detail/fips/140/2/final\n'
        '[FIPS Mode]: https://wiki.openssl.org/index.php/FIPS_mode()\n'
        '[RFC 4492]: https://www.ietf.org/rfc/rfc4492.txt\n'
        '[Shared Secret]: https://en.wikipedia.org/wiki/Shared_secret\n'
        '[ECDH]: https://en.wikipedia.org/wiki/Elliptic-curve_Diffie%E2%80%93Hellman\n'
        '[OpenSSL cipher list format]: https://www.openssl.org/docs/man1.1.1/man1/ciphers.html\n'
        '[HTTPS]: https://en.wikipedia.org/wiki/HTTPS\n'
        '[TCP]: https://en.wikipedia.org/wiki/Transmission_Control_Protocol\n'
        '[SSL]: https://www.ssl.com/faqs/faq-what-is-ssl/\n'
        '[ClickHouse]: https://clickhouse.tech\n'
        '[Dynamic SSL Context]: #dynamic-ssl-context\n'
        ),
    link=None,
    level=4,
    num='3.8.6.1')

SRS017_ClickHouse_Security_SSL_Server = Specification(
    name='SRS017 ClickHouse Security SSL Server', 
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
        Heading(name='Introduction', level=1, num='1'),
        Heading(name='Terminology', level=1, num='2'),
        Heading(name='SSL Protocol', level=2, num='2.1'),
        Heading(name='TLS Protocol', level=2, num='2.2'),
        Heading(name='Requirements', level=1, num='3'),
        Heading(name='Secure Connections', level=2, num='3.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.HTTPS.Port', level=3, num='3.1.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.TCP.Port', level=3, num='3.1.2'),
        Heading(name='Protocols', level=2, num='3.2'),
        Heading(name='Disabling', level=3, num='3.2.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable', level=4, num='3.2.1.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv2', level=4, num='3.2.1.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv3', level=4, num='3.2.1.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1', level=4, num='3.2.1.4'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_1', level=4, num='3.2.1.5'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_2', level=4, num='3.2.1.6'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_3.NotAllowed', level=4, num='3.2.1.7'),
        Heading(name='Require TLS', level=3, num='3.2.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLS', level=4, num='3.2.2.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1', level=4, num='3.2.2.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_1', level=4, num='3.2.2.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_2', level=4, num='3.2.2.4'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_3', level=4, num='3.2.2.5'),
        Heading(name='Cipher Suites', level=2, num='3.3'),
        Heading(name='Cipher List', level=3, num='3.3.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites', level=4, num='3.3.1.1'),
        Heading(name='Prefer Server Ciphers', level=3, num='3.3.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.PreferServerCiphers', level=4, num='3.3.2.1'),
        Heading(name='ECDH Curve', level=3, num='3.3.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve', level=4, num='3.3.3.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve.DefaultValue', level=4, num='3.3.3.2'),
        Heading(name='FIPS Mode', level=2, num='3.4'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode', level=3, num='3.4.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode.ApprovedSecurityFunctions', level=3, num='3.4.2'),
        Heading(name='Diffie-Hellman (DH) Parameters', level=2, num='3.5'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DH.Parameters', level=3, num='3.5.1'),
        Heading(name='Certificates', level=2, num='3.6'),
        Heading(name='Private Key', level=3, num='3.6.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKey', level=4, num='3.6.1.1'),
        Heading(name='Private Key Handler', level=4, num='3.6.1.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler', level=5, num='3.6.1.2.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler.Password', level=5, num='3.6.1.2.2'),
        Heading(name='Certificate', level=3, num='3.6.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.Certificate', level=4, num='3.6.2.1'),
        Heading(name='CA Certificates', level=3, num='3.6.3'),
        Heading(name='Config', level=4, num='3.6.3.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.CAConfig', level=5, num='3.6.3.1.1'),
        Heading(name='Default', level=4, num='3.6.3.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.LoadDefaultCAFile', level=5, num='3.6.3.2.1'),
        Heading(name='Verification', level=3, num='3.6.4'),
        Heading(name='Mode', level=4, num='3.6.4.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode', level=5, num='3.6.4.1.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.None', level=5, num='3.6.4.1.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Relaxed', level=5, num='3.6.4.1.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Strict', level=5, num='3.6.4.1.4'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Once', level=5, num='3.6.4.1.5'),
        Heading(name='Extended Mode', level=4, num='3.6.4.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationExtended', level=5, num='3.6.4.2.1'),
        Heading(name='Depth', level=4, num='3.6.4.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationDepth', level=5, num='3.6.4.3.1'),
        Heading(name='Invalid Certificate Handler', level=3, num='3.6.5'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.InvalidCertificateHandler', level=5, num='3.6.5.3.1'),
        Heading(name='Session', level=2, num='3.7'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Cache', level=3, num='3.7.1'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.CacheSize', level=3, num='3.7.2'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.IdContext', level=3, num='3.7.3'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Timeout', level=3, num='3.7.4'),
        Heading(name='Dynamic SSL Context', level=2, num='3.8'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Reload', level=4, num='3.8.4.1'),
        Heading(name='Certificate Reload', level=3, num='3.8.5'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Certificate.Reload', level=4, num='3.8.5.1'),
        Heading(name='Private Key Reload', level=3, num='3.8.6'),
        Heading(name='RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.PrivateKey.Reload', level=4, num='3.8.6.1'),
        ),
    requirements=(
        RQ_SRS_017_ClickHouse_Security_SSL_Server_HTTPS_Port,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_TCP_Port,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_SSLv2,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_SSLv3,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_1,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_2,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Disable_TLSv1_3_NotAllowed,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLS,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_1,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_2,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Protocols_Require_TLSv1_3,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_PreferServerCiphers,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_ECDHCurve,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_CipherSuites_ECDHCurve_DefaultValue,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_FIPS_Mode,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_FIPS_Mode_ApprovedSecurityFunctions,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_DH_Parameters,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKey,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKeyHandler,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_PrivateKeyHandler_Password,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_Certificate,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_CAConfig,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_LoadDefaultCAFile,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_None,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Relaxed,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Strict,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationMode_Once,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationExtended,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_VerificationDepth,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Certificates_InvalidCertificateHandler,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_Cache,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_CacheSize,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_IdContext,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_Session_Timeout,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_DynamicContext_Certificate_Reload,
        RQ_SRS_017_ClickHouse_Security_SSL_Server_DynamicContext_PrivateKey_Reload,
        ),
    content='''
# SRS017 ClickHouse Security SSL Server
# Software Requirements Specification

## Table of Contents

* 1 [Introduction](#introduction)
* 2 [Terminology](#terminology)
  * 2.1 [SSL Protocol](#ssl-protocol)
  * 2.2 [TLS Protocol](#tls-protocol)
* 3 [Requirements](#requirements)
  * 3.1 [Secure Connections](#secure-connections)
    * 3.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.HTTPS.Port](#rqsrs-017clickhousesecuritysslserverhttpsport)
    * 3.1.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.TCP.Port](#rqsrs-017clickhousesecuritysslservertcpport)
  * 3.2 [Protocols](#protocols)
    * 3.2.1 [Disabling](#disabling)
      * 3.2.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable](#rqsrs-017clickhousesecuritysslserverprotocolsdisable)
      * 3.2.1.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv2](#rqsrs-017clickhousesecuritysslserverprotocolsdisablesslv2)
      * 3.2.1.3 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv3](#rqsrs-017clickhousesecuritysslserverprotocolsdisablesslv3)
      * 3.2.1.4 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1](#rqsrs-017clickhousesecuritysslserverprotocolsdisabletlsv1)
      * 3.2.1.5 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_1](#rqsrs-017clickhousesecuritysslserverprotocolsdisabletlsv1_1)
      * 3.2.1.6 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_2](#rqsrs-017clickhousesecuritysslserverprotocolsdisabletlsv1_2)
      * 3.2.1.7 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_3.NotAllowed](#rqsrs-017clickhousesecuritysslserverprotocolsdisabletlsv1_3notallowed)
    * 3.2.2 [Require TLS](#require-tls)
      * 3.2.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLS](#rqsrs-017clickhousesecuritysslserverprotocolsrequiretls)
      * 3.2.2.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1](#rqsrs-017clickhousesecuritysslserverprotocolsrequiretlsv1)
      * 3.2.2.3 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_1](#rqsrs-017clickhousesecuritysslserverprotocolsrequiretlsv1_1)
      * 3.2.2.4 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_2](#rqsrs-017clickhousesecuritysslserverprotocolsrequiretlsv1_2)
      * 3.2.2.5 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_3](#rqsrs-017clickhousesecuritysslserverprotocolsrequiretlsv1_3)
  * 3.3 [Cipher Suites](#cipher-suites)
    * 3.3.1 [Cipher List](#cipher-list)
      * 3.3.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites](#rqsrs-017clickhousesecuritysslserverciphersuites)
    * 3.3.2 [Prefer Server Ciphers](#prefer-server-ciphers)
      * 3.3.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.PreferServerCiphers](#rqsrs-017clickhousesecuritysslserverciphersuitespreferserverciphers)
    * 3.3.3 [ECDH Curve](#ecdh-curve)
      * 3.3.3.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve](#rqsrs-017clickhousesecuritysslserverciphersuitesecdhcurve)
      * 3.3.3.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve.DefaultValue](#rqsrs-017clickhousesecuritysslserverciphersuitesecdhcurvedefaultvalue)
  * 3.4 [FIPS Mode](#fips-mode)
    * 3.4.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode](#rqsrs-017clickhousesecuritysslserverfipsmode)
    * 3.4.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode.ApprovedSecurityFunctions](#rqsrs-017clickhousesecuritysslserverfipsmodeapprovedsecurityfunctions)
  * 3.5 [Diffie-Hellman (DH) Parameters](#diffie-hellman-dh-parameters)
    * 3.5.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.DH.Parameters](#rqsrs-017clickhousesecuritysslserverdhparameters)
  * 3.6 [Certificates](#certificates)
    * 3.6.1 [Private Key](#private-key)
      * 3.6.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKey](#rqsrs-017clickhousesecuritysslservercertificatesprivatekey)
      * 3.6.1.2 [Private Key Handler](#private-key-handler)
        * 3.6.1.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler](#rqsrs-017clickhousesecuritysslservercertificatesprivatekeyhandler)
        * 3.6.1.2.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler.Password](#rqsrs-017clickhousesecuritysslservercertificatesprivatekeyhandlerpassword)
    * 3.6.2 [Certificate](#certificate)
      * 3.6.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.Certificate](#rqsrs-017clickhousesecuritysslservercertificatescertificate)
    * 3.6.3 [CA Certificates](#ca-certificates)
      * 3.6.3.1 [Config](#config)
        * 3.6.3.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.CAConfig](#rqsrs-017clickhousesecuritysslservercertificatescaconfig)
      * 3.6.3.2 [Default](#default)
        * 3.6.3.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.LoadDefaultCAFile](#rqsrs-017clickhousesecuritysslservercertificatesloaddefaultcafile)
    * 3.6.4 [Verification](#verification)
      * 3.6.4.1 [Mode](#mode)
        * 3.6.4.1.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode](#rqsrs-017clickhousesecuritysslservercertificatesverificationmode)
        * 3.6.4.1.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.None](#rqsrs-017clickhousesecuritysslservercertificatesverificationmodenone)
        * 3.6.4.1.3 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Relaxed](#rqsrs-017clickhousesecuritysslservercertificatesverificationmoderelaxed)
        * 3.6.4.1.4 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Strict](#rqsrs-017clickhousesecuritysslservercertificatesverificationmodestrict)
        * 3.6.4.1.5 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Once](#rqsrs-017clickhousesecuritysslservercertificatesverificationmodeonce)
      * 3.6.4.2 [Extended Mode](#extended-mode)
        * 3.6.4.2.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationExtended](#rqsrs-017clickhousesecuritysslservercertificatesverificationextended)
      * 3.6.4.3 [Depth](#depth)
        * 3.6.4.3.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationDepth](#rqsrs-017clickhousesecuritysslservercertificatesverificationdepth)
    * 3.6.5 [Invalid Certificate Handler](#invalid-certificate-handler)
        * 3.6.5.3.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.InvalidCertificateHandler](#rqsrs-017clickhousesecuritysslservercertificatesinvalidcertificatehandler)
  * 3.7 [Session](#session)
    * 3.7.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Cache](#rqsrs-017clickhousesecuritysslserversessioncache)
    * 3.7.2 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.CacheSize](#rqsrs-017clickhousesecuritysslserversessioncachesize)
    * 3.7.3 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.IdContext](#rqsrs-017clickhousesecuritysslserversessionidcontext)
    * 3.7.4 [RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Timeout](#rqsrs-017clickhousesecuritysslserversessiontimeout)
  * 3.8 [Dynamic SSL Context](#dynamic-ssl-context)
      * 3.8.4.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Reload](#rqsrs-017clickhousesecuritysslserverdynamiccontextreload)
    * 3.8.5 [Certificate Reload](#certificate-reload)
      * 3.8.5.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Certificate.Reload](#rqsrs-017clickhousesecuritysslserverdynamiccontextcertificatereload)
    * 3.8.6 [Private Key Reload](#private-key-reload)
      * 3.8.6.1 [RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.PrivateKey.Reload](#rqsrs-017clickhousesecuritysslserverdynamiccontextprivatekeyreload)

## Introduction

This software requirements specification covers requirements related to [ClickHouse] acting as
a [SSL] server for secure [HTTPS] and [TCP] connections between a database
client and [ClickHouse] server.

## Terminology

### SSL Protocol

* Secure Sockets Layer ([SSL])

### TLS Protocol

* Transport Layer Security (successor to the [SSL Protocol])

## Requirements

### Secure Connections

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.HTTPS.Port
version: 1.0

[ClickHouse] SHALL support acting as a [SSL] server to provide secure client connections
over the [HTTPS] port.

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.TCP.Port
version: 1.0

[ClickHouse] SHALL support acting as a [SSL] server to provide secure client connections
over secure [TCP] port.

### Protocols

#### Disabling

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable
version: 1.0

[ClickHouse] SHALL support disabling protocols using
the `<disbaleProtocols>` parameter in the `<yandex><openSSL><server>` section
of the config.xml. Multiple protocols SHALL be specified using the comma
`,` character as a separator. Any handshakes using one of the
disabled protocol SHALL fail and downgrading to these protocols SHALL not be allowed.

```xml
<yandex>
    <openSSL>
        <server>
            ...
            <disableProtocols>sslv2,sslv3</disableProtocols>
        </server>
    </openSSL>
</yandex>
```

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv2
version: 1.0

[ClickHouse] SHALL support disabling vulnerable `SSLv2`
using the `sslv2` as the protocol name in the `<disableProtocols>` parameter.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.SSLv3
version: 1.0

[ClickHouse] SHALL support disabling vulnerable `SSLv3`
using the `sslv3` as the protocol name in the `<disableProtocols>` parameter.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1
version: 1.0

[ClickHouse] SHALL support disabling vulnerable `TLSv1`
using the `tlsv1` as the protocol name in the `<disableProtocols>` parameter.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_1
version: 1.0

[ClickHouse] SHALL support disabling `TLSv1_1`
using the `tlsv1_1` as the protocol name in the `<disableProtocols>` parameter.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_2
version: 1.0

[ClickHouse] SHALL support disabling `TLSv1_2`
using the `tlsv1_2` as the protocol name in the `<disableProtocols>` parameter.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Disable.TLSv1_3.NotAllowed
version: 1.0

[ClickHouse] SHALL not support disabling `TLSv1_3` protocol and if
the `tlsv1_3` is specified as the protocol name in the `<disableProtocols>` parameter
then an error SHALL be returned.

#### Require TLS

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLS
version: 1.0

[ClickHouse] SHALL support requiring specific TLS protocol version
using parameters in the `<yandex><openSSL><server>` section
of the `config.xml` and when specified SHALL only establish
the connection if and only if the required protocol version will be used
without downgrading.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1
version: 1.0

[ClickHouse] SHALL support requiring `TLSv1` connection
using the `<requireTLSv1>` parameter to boolean `true`.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_1
version: 1.0

[ClickHouse] SHALL support requiring `TLSv1.1` connection
using the `<requireTLSv1_1>` parameter to boolean `true`.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_2
version: 1.0

[ClickHouse] SHALL support requiring `TLSv1.2` connection
using the `<requireTLSv1_2>` parameter to boolean `true`.

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Protocols.Require.TLSv1_3
version: 1.0

[ClickHouse] SHALL support requiring `TLSv1.3` connection
using the `<requireTLSv1_3>` parameter to boolean `true`.

### Cipher Suites

#### Cipher List

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites
version: 1.0

[ClickHouse] SHALL support specifying cipher suites to be used
for [SSL] connection using the `<cipherList>` parameter
in form of a string that SHALL use [OpenSSL cipher list format] notation
```
(e.g. "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH")
```
in the `<yandex><openSSL><server>` section
of the `config.xml` and when specified the server SHALL only establish
the connection if and only if one of the specified cipher suites
will be used for the connection without downgrading.

```xml
<yandex>
    <openSSL>
        <server>
            ...
            <cipherList>ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH</cipherList>
        </server>
    </openSSL>
</yandex>
```

#### Prefer Server Ciphers

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.PreferServerCiphers
version: 1.0

[ClickHouse] SHALL use own cipher suite preferences when
the `<preferServerCiphers>` in the `<yandex><openSSL><server>` section
of the `config.xml` is set to `true`.

```xml
<yandex>
    <openSSL>
        <server>
            ...
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</yandex>
```

#### ECDH Curve

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve
version: 1.0

[ClickHouse] SHALL support specifying the curve to be used for [ECDH] key agreement protocol
using the `<ecdhCurve>` parameter in the `<yandex><openSSL><server>` section
of the `config.xml` that SHALL contain one of the name of the
the curves specified in the [RFC 4492].

```xml
<yandex>
    <openSSL>
        <server>
            ...
            <ecdhCurve>prime256v1</ecdhCurve>
        </server>
    </openSSL>
</yandex>
```

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.CipherSuites.ECDHCurve.DefaultValue
version: 1.0

[ClickHouse] SHALL use the `prime256v1` as the default value
for the `<ecdhCurve>` parameter in the `<yandex><openSSL><server>` section
of the `config.xml` that SHALL force the server to use the corresponding
curve for establishing [Shared Secret] using the [ECDH] key agreement protocol.

### FIPS Mode

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode
version: 1.0

[ClickHouse] SHALL support enabling [FIPS Mode] using the
`<fips>` parameter set to `true` in the  `<yandex><openSSL><server>` section
of the `config.xml` that SHALL enable [FIPS 140-2] mode of operation
if the [SSL] library used by the server supports it. If the
[FIPS mode] is not supported by the library
then the server SHALL return an error.

```xml
<yandex>
    <openSSL>
        <server>
            ...
        </server>
        <fips>true</fips>
    </openSSL>
</yandex>
```

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.FIPS.Mode.ApprovedSecurityFunctions
version: 1.0

[ClickHouse] SHALL only support the use of security functions
approved by [FIPS 140-2] when [FIPS Mode] is enabled.

* Symmetric Key Encryption and Decryption
  * AES
  * TDEA

* Digital Signatures
  * DSS

* Secure Hash Standard
  * SHA-1
  * SHA-224
  * SHA-256
  * SHA-384
  * SHA-512
  * SHA-512/224
  * SHA-512/256

* SHA-3 Standard
  * SHA3-224
  * SHA3-256
  * SHA3-384
  * SHA3-512
  * SHAKE128
  * SHAKE256
  * cSHAKE
  * KMAC
  * TupleHash
  * ParallelHash

* Message Authentication
  * Triple-DES
  * AES
  * HMAC

### Diffie-Hellman (DH) Parameters

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.DH.Parameters
version: 1.0

[ClickHouse] SHALL support specifying a file containing Diffie-Hellman (DH) parameters.
using a string as value for the `<dhParamsFile>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml` and
if not specified or empty, the default parameters SHALL be used.

```xml
<yandex>
    <openSSL>
        <server>
            <dhParamsFile>dh.pem</dhParamsFile>
            ...
        </server>
    </openSSL>
</yandex>
```

### Certificates

#### Private Key

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKey
version: 1.0

[ClickHouse] SHALL support specifying private key as the path to the
file containing the private key for the certificate in the [PEM format]
or containing both the private key and the certificate using the
`<privateKeyFile>` parameter in the `<yandex><openSSL><server>` section
of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
           <privateKeyFile>/path/to/private/key</privateKeyFile>
           ...
        </server>
    </openSSL>
</yandex>
```

##### Private Key Handler

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler
version: 1.0

[ClickHouse] SHALL support specifying the name of the class that is subclass of `PrivateKeyPassphraseHandler`
used for obtaining the passphrase for accessing the private key using a string as a value of the
`<privateKeyPassphraseHandler><name>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
            <privateKeyPassphraseHandler>
                <name>KeyFileHandler</name>
                ...
            </privateKeyPassphraseHandler>
            ...
        </server>
    </openSSL>
</yandex>
```

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.PrivateKeyHandler.Password
version: 1.0

[ClickHouse] SHALL support specifying the password to be used by the private key handler using a string
as a value of the `<privateKeyPassphraseHandler><options><password>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
            <privateKeyPassphraseHandler>
                <name>KeyFileHandler</name>
                <options>
                    <password>private key password</password>
                </options>
            </privateKeyPassphraseHandler>
            ...
        </server>
    </openSSL>
</yandex>
```

#### Certificate

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.Certificate
version: 1.0

[ClickHouse] SHALL support specifying the path to the file containing the server's
certificate in the [PEM format] using the `<certificateFile>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml`.
When the private key specified by the `<privateKeyFile>` parameter contains
the certificate then this parameter SHALL be ignored.

```xml
<yandex>
    <openSSL>
        <server>
           <certificateFile>/path/to/the/certificate</certificateFile>
           ...
        </server>
    </openSSL>
</yandex>
```

#### CA Certificates

##### Config

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.CAConfig
version: 1.0

[ClickHouse] SHALL support specifying the path to the file or directory containing the trusted root certificates
using the `<caConfig>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
           <caConfig>/path/to/the/file/or/directory/containing/trusted/root/cerificates</caConfig>
           ...
        </server>
    </openSSL>
</yandex>
```

##### Default

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.LoadDefaultCAFile
version: 1.0

[ClickHouse] SHALL support specifying whether the builtin CA certificates provided by the [SSL]
library SHALL be used using the `<loadDefaultCAFile>` parameter with a boolean value in the
`<yandex><openSSL><server>` section of the `config.xml`.

#### Verification

##### Mode

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode
version: 1.0

[ClickHouse] SHALL support specifying whether and how client certificates are validated
using the `<verificationMode>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.None
version: 1.0

[ClickHouse] SHALL not perform client certificate validation when the `<verificationMode>` parameter
in the `<yandex><openSSL><server>` section of the `config.xml` is set to `none`
by not sending a `client certificate request` to the client so that the client will not send a certificate.

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Relaxed
version: 1.0

[ClickHouse] SHALL perform relaxed client certificate validation when the `<verificationMode>` parameter
in the `<yandex><openSSL><server>` section of the `config.xml` is set to `relaxed` by
sending a `client certificate request` to the client. The certificate SHALL only be checked if client sends it.
If the client certificate verification process fails, the TLS/SSL handshake SHALL be immediately
terminated with an alert message containing the reason for the verification failure.

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Strict
version: 1.0

[ClickHouse] SHALL perform strict client certificate validation when the `<verificationMode>` parameter
in the `<yandex><openSSL><server>` section of the `config.xml` is set to `strict` by
immediately terminating TLS/SSL handshake if the client did not return a certificate or
certificate validation process fails with a handshake failure alert.

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationMode.Once
version: 1.0

[ClickHouse] SHALL perform client certificate validation only once when the `<verificationMode>` parameter
in the `<yandex><openSSL><server>` section of the `config.xml` is set to `once`
by only requesting a client certificate on the initial [TLS/SSL handshake].
During renegotiation client certificate SHALL not be requested and verified again.

##### Extended Mode

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationExtended
version: 1.0

[ClickHouse] SHALL support enabling or disabling automatic post-connection extended certificate verification
using a `boolean` as a value of the `<extendedVerification>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml` and connection SHALL be aborted
if extended certificate verification is enabled and fails.

##### Depth

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.VerificationDepth
version: 1.0

[ClickHouse] SHALL support specifying the upper limit for the client certificate verification chain size
using the `<verificationDepth>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.
Verification SHALL fail if a certificate chain is larger than the value specified by this parameter
is encountered.

#### Invalid Certificate Handler

###### RQ.SRS-017.ClickHouse.Security.SSL.Server.Certificates.InvalidCertificateHandler
version: 1.0

[ClickHouse] SHALL support specifying the name of the class that is a subclass of `CertificateHandler`
that SHALL be used for confirming invalid certificates using a string as the value of the
`<invalidCertificateHandler><name>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
            <invalidCertificateHandler>
                <name>ConsoleCertificateHandler</name>
            </invalidCertificateHandler>
            ...
        </server>
    </openSSL>
</yandex>
```

### Session

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Cache
version: 1.0

[ClickHouse] SHALL support enabling or disabling session caching using a boolean as a value of the
`<cacheSessions>` parameter in the `<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
            cacheSessions>true|false</cacheSessions>
            ...
        </server>
    </openSSL>
</yandex>
```

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.CacheSize
version: 1.0

[ClickHouse] SHALL support specifying the maximum size of the server session cache as the number of sessions
using an integer as a value of the `<sessionCacheSize>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml`.

* The default size SHALL be `1024*20`.
* Specifying a size of 0 SHALL set an unlimited cache size.

```xml
<yandex>
    <openSSL>
        <server>
            <sessionCacheSize>0..n</sessionCacheSize>
            ...
        </server>
    </openSSL>
</yandex>
```

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.IdContext
version: 1.0

[ClickHouse] SHALL support specifying unique session ID context, which SHALL become part of each
session identifier generated by the server using a string as a value of the `<sessionIdContext>` parameter
in the `<yandex><openSSL><server>` section of the `config.xml`.

* The value SHALL support an arbitrary sequence of bytes with a maximum length of `SSL_MAX_SSL_SESSION_ID_LENGTH`.
* This parameter SHALL be specified for a server to enable session caching.
* This parameter SHALL be specified even if session caching is disabled to avoid problems with clients that request
  session caching (e.g. Firefox 3.6).
* If not specified, the default value SHALL be set to `${application.name}`.

```xml
<yandex>
    <openSSL>
        <server>
            <sessionIdContext>someString</sessionIdContext>
            ...
        </server>
    </openSSL>
</yandex>
```

#### RQ.SRS-017.ClickHouse.Security.SSL.Server.Session.Timeout
version: 1.0

[ClickHouse] SHALL support setting the timeout in seconds of sessions cached on the server
using an integer as a value of the <sessionTimeout>` parameter in the
`<yandex><openSSL><server>` section of the `config.xml`.

```xml
<yandex>
    <openSSL>
        <server>
            <sessionTimeout>0..n</sessionTimeout>
            ...
        </server>
    </openSSL>
</yandex>
```

### Dynamic SSL Context

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Reload

[ClickHouse] SHALL reload dynamic SSL context any time `config.xml` configuration file
or any of its parts defined inside the `/etc/clickhouse-server/configs.d/`
directory changes and causes `/var/lib/clickhouse/preprocessed_configs/config.xml`
configuration file generation and reload.

#### Certificate Reload

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.Certificate.Reload
version: 1.0

[ClickHouse] SHALL reload server SSL certificate specified 
by the `<certificateFile>` parameter in the `<yandex><openSSL><server>`
section of the `config.xml` any time [Dynamic SSL Context] is reloaded. 
and the reloaded SSL server certificate SHALL be immediately applied
to any to new SSL connections made to the server.

#### Private Key Reload

##### RQ.SRS-017.ClickHouse.Security.SSL.Server.DynamicContext.PrivateKey.Reload
version: 1.0

[ClickHouse] SHALL reload server SSL private key specified 
by the `<privateKeyFile>` parameter in the `<yandex><openSSL><server>` section
of the `config.xml` any time [Dynamic SSL Context] is reloaded  
and the reloaded SSL private key SHALL be immediately applied
to any new SSL connections made to the server.

[SRS]: #srs
[SSL Protocol]: #ssl-protocol
[TLS Protocol]: #tls-protocol
[J. Kalsi, D. Mossop]: https://www.contextis.com/en/blog/manually-testing-ssl-tls-weaknesses
[RSA]: https://en.wikipedia.org/wiki/RSA_(cryptosystem)
[MD5]: https://en.wikipedia.org/wiki/MD5
[SHA1]: https://en.wikipedia.org/wiki/SHA-1
[SHA2]: https://en.wikipedia.org/wiki/SHA-2
[TLS/SSL handshake]: https://en.wikipedia.org/wiki/Transport_Layer_Security#TLS_handshake
[PEM format]: https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail#Format
[FIPS 140-2]: https://csrc.nist.gov/publications/detail/fips/140/2/final
[FIPS Mode]: https://wiki.openssl.org/index.php/FIPS_mode()
[RFC 4492]: https://www.ietf.org/rfc/rfc4492.txt
[Shared Secret]: https://en.wikipedia.org/wiki/Shared_secret
[ECDH]: https://en.wikipedia.org/wiki/Elliptic-curve_Diffie%E2%80%93Hellman
[OpenSSL cipher list format]: https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
[HTTPS]: https://en.wikipedia.org/wiki/HTTPS
[TCP]: https://en.wikipedia.org/wiki/Transmission_Control_Protocol
[SSL]: https://www.ssl.com/faqs/faq-what-is-ssl/
[ClickHouse]: https://clickhouse.tech
[Dynamic SSL Context]: #dynamic-ssl-context
''')

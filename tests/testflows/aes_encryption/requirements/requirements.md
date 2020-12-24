# SRS-008 ClickHouse AES Encryption Functions
# Software Requirements Specification

## Table of Contents
* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
* 4 [Requirements](#requirements)
  * 4.1 [Generic](#generic)
    * 4.1.1 [RQ.SRS008.AES.Functions](#rqsrs008aesfunctions)
    * 4.1.2 [RQ.SRS008.AES.Functions.Compatability.MySQL](#rqsrs008aesfunctionscompatabilitymysql)
    * 4.1.3 [RQ.SRS008.AES.Functions.Compatability.Dictionaries](#rqsrs008aesfunctionscompatabilitydictionaries)
    * 4.1.4 [RQ.SRS008.AES.Functions.Compatability.Engine.Database.MySQL](#rqsrs008aesfunctionscompatabilityenginedatabasemysql)
    * 4.1.5 [RQ.SRS008.AES.Functions.Compatability.Engine.Table.MySQL](#rqsrs008aesfunctionscompatabilityenginetablemysql)
    * 4.1.6 [RQ.SRS008.AES.Functions.Compatability.TableFunction.MySQL](#rqsrs008aesfunctionscompatabilitytablefunctionmysql)
    * 4.1.7 [RQ.SRS008.AES.Functions.DifferentModes](#rqsrs008aesfunctionsdifferentmodes)
    * 4.1.8 [RQ.SRS008.AES.Functions.DataFromMultipleSources](#rqsrs008aesfunctionsdatafrommultiplesources)
    * 4.1.9 [RQ.SRS008.AES.Functions.SuppressOutputOfSensitiveValues](#rqsrs008aesfunctionssuppressoutputofsensitivevalues)
    * 4.1.10 [RQ.SRS008.AES.Functions.InvalidParameters](#rqsrs008aesfunctionsinvalidparameters)
    * 4.1.11 [RQ.SRS008.AES.Functions.MismatchedKey](#rqsrs008aesfunctionsmismatchedkey)
    * 4.1.12 [RQ.SRS008.AES.Functions.Check.Performance](#rqsrs008aesfunctionscheckperformance)
    * 4.1.13 [RQ.SRS008.AES.Function.Check.Performance.BestCase](#rqsrs008aesfunctioncheckperformancebestcase)
    * 4.1.14 [RQ.SRS008.AES.Function.Check.Performance.WorstCase](#rqsrs008aesfunctioncheckperformanceworstcase)
    * 4.1.15 [RQ.SRS008.AES.Functions.Check.Compression](#rqsrs008aesfunctionscheckcompression)
    * 4.1.16 [RQ.SRS008.AES.Functions.Check.Compression.LowCardinality](#rqsrs008aesfunctionscheckcompressionlowcardinality)
  * 4.2 [Specific](#specific)
    * 4.2.1 [RQ.SRS008.AES.Encrypt.Function](#rqsrs008aesencryptfunction)
    * 4.2.2 [RQ.SRS008.AES.Encrypt.Function.Syntax](#rqsrs008aesencryptfunctionsyntax)
    * 4.2.3 [RQ.SRS008.AES.Encrypt.Function.NIST.TestVectors](#rqsrs008aesencryptfunctionnisttestvectors)
    * 4.2.4 [RQ.SRS008.AES.Encrypt.Function.Parameters.PlainText](#rqsrs008aesencryptfunctionparametersplaintext)
    * 4.2.5 [RQ.SRS008.AES.Encrypt.Function.Parameters.Key](#rqsrs008aesencryptfunctionparameterskey)
    * 4.2.6 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode](#rqsrs008aesencryptfunctionparametersmode)
    * 4.2.7 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.ValuesFormat](#rqsrs008aesencryptfunctionparametersmodevaluesformat)
    * 4.2.8 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.Invalid](#rqsrs008aesencryptfunctionparametersmodevalueinvalid)
    * 4.2.9 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-ecb)
    * 4.2.10 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-ecb)
    * 4.2.11 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-ecb)
    * 4.2.12 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-cbc)
    * 4.2.13 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-cbc)
    * 4.2.14 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-cbc)
    * 4.2.15 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-cfb1)
    * 4.2.16 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-cfb1)
    * 4.2.17 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-cfb1)
    * 4.2.18 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-cfb8)
    * 4.2.19 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-cfb8)
    * 4.2.20 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-cfb8)
    * 4.2.21 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-cfb128)
    * 4.2.22 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-cfb128)
    * 4.2.23 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-cfb128)
    * 4.2.24 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-ofb)
    * 4.2.25 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-ofb)
    * 4.2.26 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-ofb)
    * 4.2.27 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-gcm)
    * 4.2.28 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-gcm)
    * 4.2.29 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-gcm)
    * 4.2.30 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR](#rqsrs008aesencryptfunctionparametersmodevalueaes-128-ctr)
    * 4.2.31 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR](#rqsrs008aesencryptfunctionparametersmodevalueaes-192-ctr)
    * 4.2.32 [RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR](#rqsrs008aesencryptfunctionparametersmodevalueaes-256-ctr)
    * 4.2.33 [RQ.SRS008.AES.Encrypt.Function.Parameters.InitializationVector](#rqsrs008aesencryptfunctionparametersinitializationvector)
    * 4.2.34 [RQ.SRS008.AES.Encrypt.Function.Parameters.AdditionalAuthenticatedData](#rqsrs008aesencryptfunctionparametersadditionalauthenticateddata)
    * 4.2.35 [RQ.SRS008.AES.Encrypt.Function.Parameters.ReturnValue](#rqsrs008aesencryptfunctionparametersreturnvalue)
    * 4.2.36 [RQ.SRS008.AES.Encrypt.Function.Key.Length.InvalidLengthError](#rqsrs008aesencryptfunctionkeylengthinvalidlengtherror)
    * 4.2.37 [RQ.SRS008.AES.Encrypt.Function.InitializationVector.Length.InvalidLengthError](#rqsrs008aesencryptfunctioninitializationvectorlengthinvalidlengtherror)
    * 4.2.38 [RQ.SRS008.AES.Encrypt.Function.InitializationVector.NotValidForMode](#rqsrs008aesencryptfunctioninitializationvectornotvalidformode)
    * 4.2.39 [RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.NotValidForMode](#rqsrs008aesencryptfunctionadditionalauthenticationdatanotvalidformode)
    * 4.2.40 [RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.Length](#rqsrs008aesencryptfunctionadditionalauthenticationdatalength)
    * 4.2.41 [RQ.SRS008.AES.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-ecbkeyandinitializationvectorlength)
    * 4.2.42 [RQ.SRS008.AES.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-ecbkeyandinitializationvectorlength)
    * 4.2.43 [RQ.SRS008.AES.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-ecbkeyandinitializationvectorlength)
    * 4.2.44 [RQ.SRS008.AES.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-cbckeyandinitializationvectorlength)
    * 4.2.45 [RQ.SRS008.AES.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-cbckeyandinitializationvectorlength)
    * 4.2.46 [RQ.SRS008.AES.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-cbckeyandinitializationvectorlength)
    * 4.2.47 [RQ.SRS008.AES.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-cfb1keyandinitializationvectorlength)
    * 4.2.48 [RQ.SRS008.AES.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-cfb1keyandinitializationvectorlength)
    * 4.2.49 [RQ.SRS008.AES.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-cfb1keyandinitializationvectorlength)
    * 4.2.50 [RQ.SRS008.AES.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-cfb8keyandinitializationvectorlength)
    * 4.2.51 [RQ.SRS008.AES.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-cfb8keyandinitializationvectorlength)
    * 4.2.52 [RQ.SRS008.AES.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-cfb8keyandinitializationvectorlength)
    * 4.2.53 [RQ.SRS008.AES.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-cfb128keyandinitializationvectorlength)
    * 4.2.54 [RQ.SRS008.AES.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-cfb128keyandinitializationvectorlength)
    * 4.2.55 [RQ.SRS008.AES.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-cfb128keyandinitializationvectorlength)
    * 4.2.56 [RQ.SRS008.AES.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-ofbkeyandinitializationvectorlength)
    * 4.2.57 [RQ.SRS008.AES.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-ofbkeyandinitializationvectorlength)
    * 4.2.58 [RQ.SRS008.AES.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-ofbkeyandinitializationvectorlength)
    * 4.2.59 [RQ.SRS008.AES.Encrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-gcmkeyandinitializationvectorlength)
    * 4.2.60 [RQ.SRS008.AES.Encrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-gcmkeyandinitializationvectorlength)
    * 4.2.61 [RQ.SRS008.AES.Encrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-gcmkeyandinitializationvectorlength)
    * 4.2.62 [RQ.SRS008.AES.Encrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-128-ctrkeyandinitializationvectorlength)
    * 4.2.63 [RQ.SRS008.AES.Encrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-192-ctrkeyandinitializationvectorlength)
    * 4.2.64 [RQ.SRS008.AES.Encrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length](#rqsrs008aesencryptfunctionaes-256-ctrkeyandinitializationvectorlength)
    * 4.2.65 [RQ.SRS008.AES.Decrypt.Function](#rqsrs008aesdecryptfunction)
    * 4.2.66 [RQ.SRS008.AES.Decrypt.Function.Syntax](#rqsrs008aesdecryptfunctionsyntax)
    * 4.2.67 [RQ.SRS008.AES.Decrypt.Function.Parameters.CipherText](#rqsrs008aesdecryptfunctionparametersciphertext)
    * 4.2.68 [RQ.SRS008.AES.Decrypt.Function.Parameters.Key](#rqsrs008aesdecryptfunctionparameterskey)
    * 4.2.69 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode](#rqsrs008aesdecryptfunctionparametersmode)
    * 4.2.70 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.ValuesFormat](#rqsrs008aesdecryptfunctionparametersmodevaluesformat)
    * 4.2.71 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.Invalid](#rqsrs008aesdecryptfunctionparametersmodevalueinvalid)
    * 4.2.72 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-ecb)
    * 4.2.73 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-ecb)
    * 4.2.74 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-ecb)
    * 4.2.75 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-cbc)
    * 4.2.76 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-cbc)
    * 4.2.77 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-cbc)
    * 4.2.78 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-cfb1)
    * 4.2.79 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-cfb1)
    * 4.2.80 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-cfb1)
    * 4.2.81 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-cfb8)
    * 4.2.82 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-cfb8)
    * 4.2.83 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-cfb8)
    * 4.2.84 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-cfb128)
    * 4.2.85 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-cfb128)
    * 4.2.86 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-cfb128)
    * 4.2.87 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-ofb)
    * 4.2.88 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-ofb)
    * 4.2.89 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-ofb)
    * 4.2.90 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-gcm)
    * 4.2.91 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-gcm)
    * 4.2.92 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-gcm)
    * 4.2.93 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR](#rqsrs008aesdecryptfunctionparametersmodevalueaes-128-ctr)
    * 4.2.94 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR](#rqsrs008aesdecryptfunctionparametersmodevalueaes-192-ctr)
    * 4.2.95 [RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR](#rqsrs008aesdecryptfunctionparametersmodevalueaes-256-ctr)
    * 4.2.96 [RQ.SRS008.AES.Decrypt.Function.Parameters.InitializationVector](#rqsrs008aesdecryptfunctionparametersinitializationvector)
    * 4.2.97 [RQ.SRS008.AES.Decrypt.Function.Parameters.AdditionalAuthenticatedData](#rqsrs008aesdecryptfunctionparametersadditionalauthenticateddata)
    * 4.2.98 [RQ.SRS008.AES.Decrypt.Function.Parameters.ReturnValue](#rqsrs008aesdecryptfunctionparametersreturnvalue)
    * 4.2.99 [RQ.SRS008.AES.Decrypt.Function.Key.Length.InvalidLengthError](#rqsrs008aesdecryptfunctionkeylengthinvalidlengtherror)
    * 4.2.100 [RQ.SRS008.AES.Decrypt.Function.InitializationVector.Length.InvalidLengthError](#rqsrs008aesdecryptfunctioninitializationvectorlengthinvalidlengtherror)
    * 4.2.101 [RQ.SRS008.AES.Decrypt.Function.InitializationVector.NotValidForMode](#rqsrs008aesdecryptfunctioninitializationvectornotvalidformode)
    * 4.2.102 [RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.NotValidForMode](#rqsrs008aesdecryptfunctionadditionalauthenticationdatanotvalidformode)
    * 4.2.103 [RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.Length](#rqsrs008aesdecryptfunctionadditionalauthenticationdatalength)
    * 4.2.104 [RQ.SRS008.AES.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-ecbkeyandinitializationvectorlength)
    * 4.2.105 [RQ.SRS008.AES.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-ecbkeyandinitializationvectorlength)
    * 4.2.106 [RQ.SRS008.AES.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-ecbkeyandinitializationvectorlength)
    * 4.2.107 [RQ.SRS008.AES.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-cbckeyandinitializationvectorlength)
    * 4.2.108 [RQ.SRS008.AES.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-cbckeyandinitializationvectorlength)
    * 4.2.109 [RQ.SRS008.AES.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-cbckeyandinitializationvectorlength)
    * 4.2.110 [RQ.SRS008.AES.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-cfb1keyandinitializationvectorlength)
    * 4.2.111 [RQ.SRS008.AES.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-cfb1keyandinitializationvectorlength)
    * 4.2.112 [RQ.SRS008.AES.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-cfb1keyandinitializationvectorlength)
    * 4.2.113 [RQ.SRS008.AES.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-cfb8keyandinitializationvectorlength)
    * 4.2.114 [RQ.SRS008.AES.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-cfb8keyandinitializationvectorlength)
    * 4.2.115 [RQ.SRS008.AES.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-cfb8keyandinitializationvectorlength)
    * 4.2.116 [RQ.SRS008.AES.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-cfb128keyandinitializationvectorlength)
    * 4.2.117 [RQ.SRS008.AES.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-cfb128keyandinitializationvectorlength)
    * 4.2.118 [RQ.SRS008.AES.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-cfb128keyandinitializationvectorlength)
    * 4.2.119 [RQ.SRS008.AES.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-ofbkeyandinitializationvectorlength)
    * 4.2.120 [RQ.SRS008.AES.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-ofbkeyandinitializationvectorlength)
    * 4.2.121 [RQ.SRS008.AES.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-ofbkeyandinitializationvectorlength)
    * 4.2.122 [RQ.SRS008.AES.Decrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-gcmkeyandinitializationvectorlength)
    * 4.2.123 [RQ.SRS008.AES.Decrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-gcmkeyandinitializationvectorlength)
    * 4.2.124 [RQ.SRS008.AES.Decrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-gcmkeyandinitializationvectorlength)
    * 4.2.125 [RQ.SRS008.AES.Decrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-128-ctrkeyandinitializationvectorlength)
    * 4.2.126 [RQ.SRS008.AES.Decrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-192-ctrkeyandinitializationvectorlength)
    * 4.2.127 [RQ.SRS008.AES.Decrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length](#rqsrs008aesdecryptfunctionaes-256-ctrkeyandinitializationvectorlength)
  * 4.3 [MySQL Specific Functions](#mysql-specific-functions)
    * 4.3.1 [RQ.SRS008.AES.MySQL.Encrypt.Function](#rqsrs008aesmysqlencryptfunction)
    * 4.3.2 [RQ.SRS008.AES.MySQL.Encrypt.Function.Syntax](#rqsrs008aesmysqlencryptfunctionsyntax)
    * 4.3.3 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.PlainText](#rqsrs008aesmysqlencryptfunctionparametersplaintext)
    * 4.3.4 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Key](#rqsrs008aesmysqlencryptfunctionparameterskey)
    * 4.3.5 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode](#rqsrs008aesmysqlencryptfunctionparametersmode)
    * 4.3.6 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.ValuesFormat](#rqsrs008aesmysqlencryptfunctionparametersmodevaluesformat)
    * 4.3.7 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.Invalid](#rqsrs008aesmysqlencryptfunctionparametersmodevalueinvalid)
    * 4.3.8 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-ecb)
    * 4.3.9 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-ecb)
    * 4.3.10 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-ecb)
    * 4.3.11 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-cbc)
    * 4.3.12 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-cbc)
    * 4.3.13 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-cbc)
    * 4.3.14 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-cfb1)
    * 4.3.15 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-cfb1)
    * 4.3.16 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-cfb1)
    * 4.3.17 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-cfb8)
    * 4.3.18 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-cfb8)
    * 4.3.19 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-cfb8)
    * 4.3.20 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-cfb128)
    * 4.3.21 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-cfb128)
    * 4.3.22 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-cfb128)
    * 4.3.23 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-ofb)
    * 4.3.24 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-ofb)
    * 4.3.25 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-ofb)
    * 4.3.26 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-gcmerror)
    * 4.3.27 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-gcmerror)
    * 4.3.28 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-gcmerror)
    * 4.3.29 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-128-ctrerror)
    * 4.3.30 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-192-ctrerror)
    * 4.3.31 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error](#rqsrs008aesmysqlencryptfunctionparametersmodevalueaes-256-ctrerror)
    * 4.3.32 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.InitializationVector](#rqsrs008aesmysqlencryptfunctionparametersinitializationvector)
    * 4.3.33 [RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.ReturnValue](#rqsrs008aesmysqlencryptfunctionparametersreturnvalue)
    * 4.3.34 [RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooShortError](#rqsrs008aesmysqlencryptfunctionkeylengthtooshorterror)
    * 4.3.35 [RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooLong](#rqsrs008aesmysqlencryptfunctionkeylengthtoolong)
    * 4.3.36 [RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooShortError](#rqsrs008aesmysqlencryptfunctioninitializationvectorlengthtooshorterror)
    * 4.3.37 [RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooLong](#rqsrs008aesmysqlencryptfunctioninitializationvectorlengthtoolong)
    * 4.3.38 [RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.NotValidForMode](#rqsrs008aesmysqlencryptfunctioninitializationvectornotvalidformode)
    * 4.3.39 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-ecbkeyandinitializationvectorlength)
    * 4.3.40 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-ecbkeyandinitializationvectorlength)
    * 4.3.41 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-ecbkeyandinitializationvectorlength)
    * 4.3.42 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-cbckeyandinitializationvectorlength)
    * 4.3.43 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-cbckeyandinitializationvectorlength)
    * 4.3.44 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-cbckeyandinitializationvectorlength)
    * 4.3.45 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-cfb1keyandinitializationvectorlength)
    * 4.3.46 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-cfb1keyandinitializationvectorlength)
    * 4.3.47 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-cfb1keyandinitializationvectorlength)
    * 4.3.48 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-cfb8keyandinitializationvectorlength)
    * 4.3.49 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-cfb8keyandinitializationvectorlength)
    * 4.3.50 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-cfb8keyandinitializationvectorlength)
    * 4.3.51 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-cfb128keyandinitializationvectorlength)
    * 4.3.52 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-cfb128keyandinitializationvectorlength)
    * 4.3.53 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-cfb128keyandinitializationvectorlength)
    * 4.3.54 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-128-ofbkeyandinitializationvectorlength)
    * 4.3.55 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-192-ofbkeyandinitializationvectorlength)
    * 4.3.56 [RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqlencryptfunctionaes-256-ofbkeyandinitializationvectorlength)
    * 4.3.57 [RQ.SRS008.AES.MySQL.Decrypt.Function](#rqsrs008aesmysqldecryptfunction)
    * 4.3.58 [RQ.SRS008.AES.MySQL.Decrypt.Function.Syntax](#rqsrs008aesmysqldecryptfunctionsyntax)
    * 4.3.59 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.CipherText](#rqsrs008aesmysqldecryptfunctionparametersciphertext)
    * 4.3.60 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Key](#rqsrs008aesmysqldecryptfunctionparameterskey)
    * 4.3.61 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode](#rqsrs008aesmysqldecryptfunctionparametersmode)
    * 4.3.62 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.ValuesFormat](#rqsrs008aesmysqldecryptfunctionparametersmodevaluesformat)
    * 4.3.63 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.Invalid](#rqsrs008aesmysqldecryptfunctionparametersmodevalueinvalid)
    * 4.3.64 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-ecb)
    * 4.3.65 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-ecb)
    * 4.3.66 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-ecb)
    * 4.3.67 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-cbc)
    * 4.3.68 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-cbc)
    * 4.3.69 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-cbc)
    * 4.3.70 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-cfb1)
    * 4.3.71 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-cfb1)
    * 4.3.72 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-cfb1)
    * 4.3.73 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-cfb8)
    * 4.3.74 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-cfb8)
    * 4.3.75 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-cfb8)
    * 4.3.76 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-cfb128)
    * 4.3.77 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-cfb128)
    * 4.3.78 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-cfb128)
    * 4.3.79 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-ofb)
    * 4.3.80 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-ofb)
    * 4.3.81 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-ofb)
    * 4.3.82 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-gcmerror)
    * 4.3.83 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-gcmerror)
    * 4.3.84 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-gcmerror)
    * 4.3.85 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-128-ctrerror)
    * 4.3.86 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-192-ctrerror)
    * 4.3.87 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error](#rqsrs008aesmysqldecryptfunctionparametersmodevalueaes-256-ctrerror)
    * 4.3.88 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.InitializationVector](#rqsrs008aesmysqldecryptfunctionparametersinitializationvector)
    * 4.3.89 [RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.ReturnValue](#rqsrs008aesmysqldecryptfunctionparametersreturnvalue)
    * 4.3.90 [RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooShortError](#rqsrs008aesmysqldecryptfunctionkeylengthtooshorterror)
    * 4.3.91 [RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooLong](#rqsrs008aesmysqldecryptfunctionkeylengthtoolong)
    * 4.3.92 [RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooShortError](#rqsrs008aesmysqldecryptfunctioninitializationvectorlengthtooshorterror)
    * 4.3.93 [RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooLong](#rqsrs008aesmysqldecryptfunctioninitializationvectorlengthtoolong)
    * 4.3.94 [RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.NotValidForMode](#rqsrs008aesmysqldecryptfunctioninitializationvectornotvalidformode)
    * 4.3.95 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-ecbkeyandinitializationvectorlength)
    * 4.3.96 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-ecbkeyandinitializationvectorlength)
    * 4.3.97 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-ecbkeyandinitializationvectorlength)
    * 4.3.98 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-cbckeyandinitializationvectorlength)
    * 4.3.99 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-cbckeyandinitializationvectorlength)
    * 4.3.100 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-cbckeyandinitializationvectorlength)
    * 4.3.101 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-cfb1keyandinitializationvectorlength)
    * 4.3.102 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-cfb1keyandinitializationvectorlength)
    * 4.3.103 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-cfb1keyandinitializationvectorlength)
    * 4.3.104 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-cfb8keyandinitializationvectorlength)
    * 4.3.105 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-cfb8keyandinitializationvectorlength)
    * 4.3.106 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-cfb8keyandinitializationvectorlength)
    * 4.3.107 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-cfb128keyandinitializationvectorlength)
    * 4.3.108 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-cfb128keyandinitializationvectorlength)
    * 4.3.109 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-cfb128keyandinitializationvectorlength)
    * 4.3.110 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-128-ofbkeyandinitializationvectorlength)
    * 4.3.111 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-192-ofbkeyandinitializationvectorlength)
    * 4.3.112 [RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length](#rqsrs008aesmysqldecryptfunctionaes-256-ofbkeyandinitializationvectorlength)
* 5 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

Users need an ability to encrypt and decrypt column data with tenant specific keys.
Use cases include protection of sensitive column values and [GDPR] right to forget policies.
The implementation will support capabilities of the [MySQL aes_encrypt] and [MySQL aes_decrypt]
functions which encrypt and decrypt values using the [AES] (Advanced Encryption Standard)
algorithm. This functionality will enable encryption and decryption of data
accessed on remote [MySQL] servers via [MySQL Dictionary] or [MySQL Database Engine],
[MySQL Table Engine], or [MySQL Table Function].

## Terminology

* **AES** -
  Advanced Encryption Standard ([AES])

## Requirements

### Generic

#### RQ.SRS008.AES.Functions
version: 1.0

[ClickHouse] SHALL support [AES] encryption functions to encrypt and decrypt data.

#### RQ.SRS008.AES.Functions.Compatability.MySQL
version: 1.0

[ClickHouse] SHALL support [AES] encryption functions compatible with [MySQL 5.7].

#### RQ.SRS008.AES.Functions.Compatability.Dictionaries
version: 1.0

[ClickHouse] SHALL support encryption and decryption of data accessed on remote
[MySQL] servers using [MySQL Dictionary].

#### RQ.SRS008.AES.Functions.Compatability.Engine.Database.MySQL
version: 1.0

[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Database Engine],

#### RQ.SRS008.AES.Functions.Compatability.Engine.Table.MySQL
version: 1.0

[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Table Engine].

#### RQ.SRS008.AES.Functions.Compatability.TableFunction.MySQL
version: 1.0

[ClickHouse] SHALL support encryption and decryption of data accessed using [MySQL Table Function].

#### RQ.SRS008.AES.Functions.DifferentModes
version: 1.0

[ClickHouse] SHALL allow different modes to be supported in a single SQL statement
using explicit function parameters.

#### RQ.SRS008.AES.Functions.DataFromMultipleSources
version: 1.0

[ClickHouse] SHALL support handling encryption and decryption of data from multiple sources
in the `SELECT` statement, including [ClickHouse] [MergeTree] table as well as [MySQL Dictionary],
[MySQL Database Engine], [MySQL Table Engine], and [MySQL Table Function]
with possibly different encryption schemes.

#### RQ.SRS008.AES.Functions.SuppressOutputOfSensitiveValues
version: 1.0

[ClickHouse] SHALL suppress output of [AES] `string` and `key` parameters to the system log,
error log, and `query_log` table to prevent leakage of sensitive values.

#### RQ.SRS008.AES.Functions.InvalidParameters
version: 1.0

[ClickHouse] SHALL return an error when parameters are invalid.

#### RQ.SRS008.AES.Functions.Mismatched.Key
version: 1.0

[ClickHouse] SHALL return garbage for mismatched keys.

#### RQ.SRS008.AES.Functions.Mismatched.IV
version: 1.0

[ClickHouse] SHALL return garbage for mismatched initialization vector for the modes that use it.

#### RQ.SRS008.AES.Functions.Mismatched.AAD
version: 1.0

[ClickHouse] SHALL return garbage for mismatched additional authentication data for the modes that use it.

#### RQ.SRS008.AES.Functions.Mismatched.Mode
version: 1.0

[ClickHouse] SHALL return an error or garbage for mismatched mode.

#### RQ.SRS008.AES.Functions.Check.Performance
version: 1.0

Performance of [AES] encryption functions SHALL be measured.

#### RQ.SRS008.AES.Function.Check.Performance.BestCase
version: 1.0

Performance of [AES] encryption functions SHALL be checked for the best case
scenario where there is one key, one initialization vector, and one large stream of data.

#### RQ.SRS008.AES.Function.Check.Performance.WorstCase
version: 1.0

Performance of [AES] encryption functions SHALL be checked for the worst case
where there are `N` keys, `N` initialization vectors and `N` very small streams of data.

#### RQ.SRS008.AES.Functions.Check.Compression
version: 1.0

Effect of [AES] encryption on column compression SHALL be measured.

#### RQ.SRS008.AES.Functions.Check.Compression.LowCardinality
version: 1.0

Effect of [AES] encryption on the compression of a column with [LowCardinality] data type
SHALL be measured.

### Specific

#### RQ.SRS008.AES.Encrypt.Function
version: 1.0

[ClickHouse] SHALL support `aes_encrypt` function to encrypt data using [AES].

#### RQ.SRS008.AES.Encrypt.Function.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `aes_encrypt` function

```sql
aes_encrypt(plaintext, key, mode, [iv, aad])
```

#### RQ.SRS008.AES.Encrypt.Function.NIST.TestVectors
version: 1.0

[ClickHouse] `aes_encrypt` function output SHALL produce output that matches [NIST test vectors].

#### RQ.SRS008.AES.Encrypt.Function.Parameters.PlainText
version: 1.0

[ClickHouse] SHALL support `plaintext` accepting any data type as
the first parameter to the `aes_encrypt` function that SHALL specify the data to be encrypted.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Key
version: 1.0

[ClickHouse] SHALL support `key` with `String` or `FixedString` data types
as the second parameter to the `aes_encrypt` function that SHALL specify the encryption key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode
version: 1.0

[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter
to the `aes_encrypt` function that SHALL specify encryption key length and block encryption mode.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.ValuesFormat
version: 1.0

[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter
of the `aes_encrypt` function where
the `key_length` SHALL specifies the length of the key and SHALL accept
`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption
mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB] as well as
[CTR] and [GCM] as the values. For example, `aes-256-ofb`.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.Invalid
version: 1.0

[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_encrypt`
function is not valid with the exception where such a mode is supported by the underlying
[OpenSSL] implementation.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB
version: 1.0

[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB
version: 1.0

[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB
version: 1.0

[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC
version: 1.0

[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC
version: 1.0

[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC
version: 1.0

[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB
version: 1.0

[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB
version: 1.0

[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB
version: 1.0

[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM
version: 1.0

[ClickHouse] SHALL support `aes-128-gcm` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 128 bit key.
An `AEAD` 16-byte tag is appended to the resulting ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM
version: 1.0

[ClickHouse] SHALL support `aes-192-gcm` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 192 bit key.
An `AEAD` 16-byte tag is appended to the resulting ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM
version: 1.0

[ClickHouse] SHALL support `aes-256-gcm` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 256 bit key.
An `AEAD` 16-byte tag is appended to the resulting ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR
version: 1.0

[ClickHouse] SHALL support `aes-128-ctr` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR
version: 1.0

[ClickHouse] SHALL support `aes-192-ctr` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR
version: 1.0

[ClickHouse] SHALL support `aes-256-ctr` as the value for the `mode` parameter of the `aes_encrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.InitializationVector
version: 1.0

[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth
parameter to the `aes_encrypt` function that SHALL specify the initialization vector for block modes that require
it.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.AdditionalAuthenticatedData
version: 1.0

[ClickHouse] SHALL support `aad` with `String` or `FixedString` data types as the optional fifth
parameter to the `aes_encrypt` function that SHALL specify the additional authenticated data
for block modes that require it.

#### RQ.SRS008.AES.Encrypt.Function.Parameters.ReturnValue
version: 1.0

[ClickHouse] SHALL return the encrypted value of the data
using `String` data type as the result of `aes_encrypt` function.

#### RQ.SRS008.AES.Encrypt.Function.Key.Length.InvalidLengthError
version: 1.0

[ClickHouse] SHALL return an error if the `key` length is not exact for the `aes_encrypt` function for a given block mode.

#### RQ.SRS008.AES.Encrypt.Function.InitializationVector.Length.InvalidLengthError
version: 1.0

[ClickHouse] SHALL return an error if the `iv` length is specified and not of the exact size for the `aes_encrypt` function for a given block mode.

#### RQ.SRS008.AES.Encrypt.Function.InitializationVector.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_encrypt` function for a mode that does not need it.

#### RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `aad` is specified for the `aes_encrypt` function for a mode that does not need it.

#### RQ.SRS008.AES.Encrypt.Function.AdditionalAuthenticationData.Length
version: 1.0

[ClickHouse] SHALL not limit the size of the `aad` parameter passed to the `aes_encrypt` function.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ecb` and `key` is not 16 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ecb` and `key` is not 24 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ecb` and `key` is not 32 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cbc` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cbc` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cbc` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb1` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb1` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb1` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb8` and `key` is not 16 bytes
and if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb8` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb8` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-cfb128` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-cfb128` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-cfb128` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ofb` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ofb` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ofb` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-gcm` and `key` is not 16 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-gcm` and `key` is not 24 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-gcm` and `key` is not 32 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-128-ctr` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-192-ctr` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Encrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt` function is set to `aes-256-ctr` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Decrypt.Function
version: 1.0

[ClickHouse] SHALL support `aes_decrypt` function to decrypt data using [AES].

#### RQ.SRS008.AES.Decrypt.Function.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `aes_decrypt` function

```sql
aes_decrypt(ciphertext, key, mode, [iv, aad])
```

#### RQ.SRS008.AES.Decrypt.Function.Parameters.CipherText
version: 1.0

[ClickHouse] SHALL support `ciphertext` accepting `FixedString` or `String` data types as
the first parameter to the `aes_decrypt` function that SHALL specify the data to be decrypted.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Key
version: 1.0

[ClickHouse] SHALL support `key` with `String` or `FixedString` data types
as the second parameter to the `aes_decrypt` function that SHALL specify the encryption key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode
version: 1.0

[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter
to the `aes_decrypt` function that SHALL specify encryption key length and block encryption mode.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.ValuesFormat
version: 1.0

[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter
of the `aes_decrypt` function where
the `key_length` SHALL specifies the length of the key and SHALL accept
`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption
mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB] as well as
[CTR] and [GCM] as the values. For example, `aes-256-ofb`.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.Invalid
version: 1.0

[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_decrypt`
function is not valid with the exception where such a mode is supported by the underlying
[OpenSSL] implementation.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB
version: 1.0

[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB
version: 1.0

[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB
version: 1.0

[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC
version: 1.0

[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC
version: 1.0

[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC
version: 1.0

[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB
version: 1.0

[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB
version: 1.0

[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB
version: 1.0

[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM
version: 1.0

[ClickHouse] SHALL support `aes-128-gcm` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 128 bit key.
An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM
version: 1.0

[ClickHouse] SHALL support `aes-192-gcm` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 192 bit key.
An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM
version: 1.0

[ClickHouse] SHALL support `aes-256-gcm` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [GCM] block mode encryption with a 256 bit key.
An [AEAD] 16-byte tag is expected present at the end of the ciphertext according to
the [RFC5116].

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR
version: 1.0

[ClickHouse] SHALL support `aes-128-ctr` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR
version: 1.0

[ClickHouse] SHALL support `aes-192-ctr` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR
version: 1.0

[ClickHouse] SHALL support `aes-256-ctr` as the value for the `mode` parameter of the `aes_decrypt` function
and [AES] algorithm SHALL use the [CTR] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.InitializationVector
version: 1.0

[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth
parameter to the `aes_decrypt` function that SHALL specify the initialization vector for block modes that require
it.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.AdditionalAuthenticatedData
version: 1.0

[ClickHouse] SHALL support `aad` with `String` or `FixedString` data types as the optional fifth
parameter to the `aes_decrypt` function that SHALL specify the additional authenticated data
for block modes that require it.

#### RQ.SRS008.AES.Decrypt.Function.Parameters.ReturnValue
version: 1.0

[ClickHouse] SHALL return the decrypted value of the data
using `String` data type as the result of `aes_decrypt` function.

#### RQ.SRS008.AES.Decrypt.Function.Key.Length.InvalidLengthError
version: 1.0

[ClickHouse] SHALL return an error if the `key` length is not exact for the `aes_decrypt` function for a given block mode.

#### RQ.SRS008.AES.Decrypt.Function.InitializationVector.Length.InvalidLengthError
version: 1.0

[ClickHouse] SHALL return an error if the `iv` is speficified and the length is not exact for the `aes_decrypt` function for a given block mode.

#### RQ.SRS008.AES.Decrypt.Function.InitializationVector.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_decrypt` function
for a mode that does not need it.

#### RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `aad` is specified for the `aes_decrypt` function
for a mode that does not need it.

#### RQ.SRS008.AES.Decrypt.Function.AdditionalAuthenticationData.Length
version: 1.0

[ClickHouse] SHALL not limit the size of the `aad` parameter passed to the `aes_decrypt` function.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ecb` and `key` is not 16 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ecb` and `key` is not 24 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ecb` and `key` is not 32 bytes
or `iv` or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cbc` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cbc` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cbc` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb1` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb1` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb1` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb8` and `key` is not 16 bytes
and if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb8` and `key` is not 24 bytes
or `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb8` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-cfb128` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-cfb128` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-cfb128` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ofb` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ofb` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ofb` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes or `aad` is specified.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-gcm` and `key` is not 16 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-gcm` and `key` is not 24 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-GCM.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-gcm` and `key` is not 32 bytes
or `iv` is not specified or is less than 8 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-128-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-128-ctr` and `key` is not 16 bytes
or if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-192-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-192-ctr` and `key` is not 24 bytes
or if specified `iv` is not 16 bytes.

#### RQ.SRS008.AES.Decrypt.Function.AES-256-CTR.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt` function is set to `aes-256-ctr` and `key` is not 32 bytes
or if specified `iv` is not 16 bytes.

### MySQL Specific Functions

#### RQ.SRS008.AES.MySQL.Encrypt.Function
version: 1.0

[ClickHouse] SHALL support `aes_encrypt_mysql` function to encrypt data using [AES].

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `aes_encrypt_mysql` function

```sql
aes_encrypt_mysql(plaintext, key, mode, [iv])
```

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.PlainText
version: 1.0

[ClickHouse] SHALL support `plaintext` accepting any data type as
the first parameter to the `aes_encrypt_mysql` function that SHALL specify the data to be encrypted.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Key
version: 1.0

[ClickHouse] SHALL support `key` with `String` or `FixedString` data types
as the second parameter to the `aes_encrypt_mysql` function that SHALL specify the encryption key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode
version: 1.0

[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter
to the `aes_encrypt_mysql` function that SHALL specify encryption key length and block encryption mode.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.ValuesFormat
version: 1.0

[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter
of the `aes_encrypt_mysql` function where
the `key_length` SHALL specifies the length of the key and SHALL accept
`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption
mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB]. For example, `aes-256-ofb`.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.Invalid
version: 1.0

[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_encrypt_mysql`
function is not valid with the exception where such a mode is supported by the underlying
[OpenSSL] implementation.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-ECB
version: 1.0

[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-ECB
version: 1.0

[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-ECB
version: 1.0

[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CBC
version: 1.0

[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CBC
version: 1.0

[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CBC
version: 1.0

[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-OFB
version: 1.0

[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-OFB
version: 1.0

[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-OFB
version: 1.0

[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_encrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-128-gcm` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-192-gcm` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-256-gcm` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-128-ctr` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-192-ctr` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-256-ctr` is specified as the value for the `mode` parameter of the
`aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.InitializationVector
version: 1.0

[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth
parameter to the `aes_encrypt_mysql` function that SHALL specify the initialization vector for block modes that require
it.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Parameters.ReturnValue
version: 1.0

[ClickHouse] SHALL return the encrypted value of the data
using `String` data type as the result of `aes_encrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooShortError
version: 1.0

[ClickHouse] SHALL return an error if the `key` length is less than the minimum for the `aes_encrypt_mysql`
function for a given block mode.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.Key.Length.TooLong
version: 1.0

[ClickHouse] SHALL use folding algorithm specified below if the `key` length is longer than required
for the `aes_encrypt_mysql` function for a given block mode.

```python
def fold_key(key, cipher_key_size):
    key = list(key) if not isinstance(key, (list, tuple)) else key
	  folded_key = key[:cipher_key_size]
	  for i in range(cipher_key_size, len(key)):
		    print(i % cipher_key_size, i)
		    folded_key[i % cipher_key_size] ^= key[i]
	  return folded_key
```

#### RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooShortError
version: 1.0

[ClickHouse] SHALL return an error if the `iv` length is specified and is less than the minimum
that is required for the `aes_encrypt_mysql` function for a given block mode.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.Length.TooLong
version: 1.0

[ClickHouse] SHALL use the first `N` bytes that are required if the `iv` is specified and
its length is longer than required for the `aes_encrypt_mysql` function for a given block mode.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.InitializationVector.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_encrypt_mysql`
function for a mode that does not need it.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-ecb` and `key` is less than 16 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-ecb` and `key` is less than 24 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-ecb` and `key` is less than 32 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cbc` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cbc` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cbc` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb1` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb1` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb1` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb8` and `key` is less than 16 bytes
and if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb8` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb8` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-cfb128` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-cfb128` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-cfb128` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-128-ofb` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-192-ofb` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Encrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_encrypt_mysql` function is set to `aes-256-ofb` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function
version: 1.0

[ClickHouse] SHALL support `aes_decrypt_mysql` function to decrypt data using [AES].

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `aes_decrypt_mysql` function

```sql
aes_decrypt_mysql(ciphertext, key, mode, [iv])
```

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.CipherText
version: 1.0

[ClickHouse] SHALL support `ciphertext` accepting any data type as
the first parameter to the `aes_decrypt_mysql` function that SHALL specify the data to be decrypted.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Key
version: 1.0

[ClickHouse] SHALL support `key` with `String` or `FixedString` data types
as the second parameter to the `aes_decrypt_mysql` function that SHALL specify the encryption key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode
version: 1.0

[ClickHouse] SHALL support `mode` with `String` or `FixedString` data types as the third parameter
to the `aes_decrypt_mysql` function that SHALL specify encryption key length and block encryption mode.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.ValuesFormat
version: 1.0

[ClickHouse] SHALL support values of the form `aes-[key length]-[mode]` for the `mode` parameter
of the `aes_decrypt_mysql` function where
the `key_length` SHALL specifies the length of the key and SHALL accept
`128`, `192`, or `256` as the values and the `mode` SHALL specify the block encryption
mode and SHALL accept [ECB], [CBC], [CFB1], [CFB8], [CFB128], or [OFB]. For example, `aes-256-ofb`.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.Invalid
version: 1.0

[ClickHouse] SHALL return an error if the specified value for the `mode` parameter of the `aes_decrypt_mysql`
function is not valid with the exception where such a mode is supported by the underlying
[OpenSSL] implementation.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-ECB
version: 1.0

[ClickHouse] SHALL support `aes-128-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-ECB
version: 1.0

[ClickHouse] SHALL support `aes-192-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-ECB
version: 1.0

[ClickHouse] SHALL support `aes-256-ecb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [ECB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CBC
version: 1.0

[ClickHouse] SHALL support `aes-128-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CBC
version: 1.0

[ClickHouse] SHALL support `aes-192-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CBC
version: 1.0

[ClickHouse] SHALL support `aes-256-cbc` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CBC] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB1
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb1` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB1] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB8
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb8` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB8] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-128-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-192-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CFB128
version: 1.0

[ClickHouse] SHALL support `aes-256-cfb128` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [CFB128] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-OFB
version: 1.0

[ClickHouse] SHALL support `aes-128-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 128 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-OFB
version: 1.0

[ClickHouse] SHALL support `aes-192-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 192 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-OFB
version: 1.0

[ClickHouse] SHALL support `aes-256-ofb` as the value for the `mode` parameter of the `aes_decrypt_mysql` function
and [AES] algorithm SHALL use the [OFB] block mode encryption with a 256 bit key.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-128-gcm` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-192-gcm` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-GCM.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-256-gcm` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-128-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-128-ctr` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-192-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-192-ctr` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.Mode.Value.AES-256-CTR.Error
version: 1.0

[ClickHouse] SHALL return an error if `aes-256-ctr` is specified as the value for the `mode` parameter of the
`aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.InitializationVector
version: 1.0

[ClickHouse] SHALL support `iv` with `String` or `FixedString` data types as the optional fourth
parameter to the `aes_decrypt_mysql` function that SHALL specify the initialization vector for block modes that require
it.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Parameters.ReturnValue
version: 1.0

[ClickHouse] SHALL return the decrypted value of the data
using `String` data type as the result of `aes_decrypt_mysql` function.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooShortError
version: 1.0

[ClickHouse] SHALL return an error if the `key` length is less than the minimum for the `aes_decrypt_mysql`
function for a given block mode.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.Key.Length.TooLong
version: 1.0

[ClickHouse] SHALL use folding algorithm specified below if the `key` length is longer than required
for the `aes_decrypt_mysql` function for a given block mode.

```python
def fold_key(key, cipher_key_size):
    key = list(key) if not isinstance(key, (list, tuple)) else key
	  folded_key = key[:cipher_key_size]
	  for i in range(cipher_key_size, len(key)):
		    print(i % cipher_key_size, i)
		    folded_key[i % cipher_key_size] ^= key[i]
	  return folded_key
```

#### RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooShortError
version: 1.0

[ClickHouse] SHALL return an error if the `iv` length is specified and is less than the minimum
that is required for the `aes_decrypt_mysql` function for a given block mode.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.Length.TooLong
version: 1.0

[ClickHouse] SHALL use the first `N` bytes that are required if the `iv` is specified and
its length is longer than required for the `aes_decrypt_mysql` function for a given block mode.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.InitializationVector.NotValidForMode
version: 1.0

[ClickHouse] SHALL return an error if the `iv` is specified for the `aes_decrypt_mysql`
function for a mode that does not need it.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-ecb` and `key` is less than 16 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-ecb` and `key` is less than 24 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-ECB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-ecb` and `key` is less than 32 bytes
or `iv` is specified.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cbc` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cbc` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CBC.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cbc` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb1` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb1` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB1.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb1` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb8` and `key` is less than 16 bytes
and if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb8` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB8.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb8` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-cfb128` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-cfb128` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-CFB128.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-cfb128` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-128-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-128-ofb` and `key` is less than 16 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-192-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-192-ofb` and `key` is less than 24 bytes
or if specified `iv` is less than 16 bytes.

#### RQ.SRS008.AES.MySQL.Decrypt.Function.AES-256-OFB.KeyAndInitializationVector.Length
version: 1.0

[ClickHouse] SHALL return an error when `mode` for the `aes_decrypt_mysql` function is set to `aes-256-ofb` and `key` is less than 32 bytes
or if specified `iv` is less than 16 bytes.

## References

* **GDPR:** https://en.wikipedia.org/wiki/General_Data_Protection_Regulation
* **MySQL:** https://www.mysql.com/
* **AES:** https://en.wikipedia.org/wiki/Advanced_Encryption_Standard
* **ClickHouse:** https://clickhouse.tech
* **Git:** https://git-scm.com/

[OpenSSL]: https://www.openssl.org/
[LowCardinality]: https://clickhouse.tech/docs/en/sql-reference/data-types/lowcardinality/
[MergeTree]: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/
[MySQL Database Engine]: https://clickhouse.tech/docs/en/engines/database-engines/mysql/
[MySQL Table Engine]: https://clickhouse.tech/docs/en/engines/table-engines/integrations/mysql/
[MySQL Table Function]: https://clickhouse.tech/docs/en/sql-reference/table-functions/mysql/
[MySQL Dictionary]: https://clickhouse.tech/docs/en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources/#dicts-external_dicts_dict_sources-mysql
[GCM]: https://en.wikipedia.org/wiki/Galois/Counter_Mode
[CTR]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_(CTR)
[CBC]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_block_chaining_(CBC)
[ECB]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Electronic_codebook_(ECB)
[CFB]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_feedback_(CFB)
[CFB1]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_feedback_(CFB)
[CFB8]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_feedback_(CFB)
[CFB128]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_feedback_(CFB)
[OFB]: https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Output_feedback_(OFB)
[GDPR]: https://en.wikipedia.org/wiki/General_Data_Protection_Regulation
[RFC5116]: https://tools.ietf.org/html/rfc5116#section-5.1
[MySQL]: https://www.mysql.com/
[MySQL 5.7]: https://dev.mysql.com/doc/refman/5.7/en/
[MySQL aes_encrypt]: https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-encrypt
[MySQL aes_decrypt]: https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
[AES]: https://en.wikipedia.org/wiki/Advanced_Encryption_Standard
[ClickHouse]: https://clickhouse.tech
[GitHub repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/aes_encryption/requirements/requirements.md
[Revision history]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/aes_encryption/requirements/requirements.md
[Git]: https://git-scm.com/
[NIST test vectors]: https://csrc.nist.gov/Projects/Cryptographic-Algorithm-Validation-Program

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""SSL Certificate generation module.

Based on the LGPL code with the authors:
Copyright (C) Martin Sjögren and AB Strakt 2001, All rights reserved.
Copyright (C) Jean-Paul Calderone 2008, All rights reserved

Modified for the Freebrie project:
Copyright (C) Freebrie Project 2010, All rights reserved
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import numbers
from datetime import datetime, timedelta
from functools import partial
from types import NoneType
from uuid import UUID

from OpenSSL import crypto, SSL

from twisted.internet import ssl as t_i_ssl

from contrib.dbc import contract_epydoc

from . import version
from .utils import exceptions_logged



#
# Constants
#

OPENSSL_KEY_TYPE = crypto.TYPE_RSA
OPENSSL_KEY_SIZE = 2048

OPENSSL_HQ_KEY_DURATION = timedelta(days=365 * 5)  # 5 years
"""How long is the HQ SSL key valid by default?"""

_OPENSSL_TRUSTED_KEY_DURATION = timedelta(days=365 * 2)  # 2 years

OPENSSL_NODE_KEY_DURATION =_OPENSSL_TRUSTED_KEY_DURATION
"""How long is the Node SSL key valid by default?"""

OPENSSL_TRUSTED_HOST_KEY_DURATION = _OPENSSL_TRUSTED_KEY_DURATION
"""How long is the Trusted Host SSL key valid by default?"""

OPENSSL_HOST_KEY_DURATION = timedelta(days=30)  # 1 month
"""How long is the Host SSL key valid by default?"""

OPENSSL_DIGEST = 'sha512'
"""What crypto algorithm is used in SSL for digesting?"""

CERT_O = '{} project'.format(version.project_internal_codename)
CERT_CN_HQ_PRIMARY = 'Primary Certificate Authority'
CERT_CN_HQ_DEBUG = 'Debug Certificate Authority'
CERT_OU_HQ = 'HQ server'
CERT_OU_NODE = 'Node'
CERT_OU_HOST = 'Host'

SSL_VALIDATION_DEPTH = 3

# What CNs are good to use as HQs/certificate issuers.
CERT_CNS_HQ = (CERT_CN_HQ_PRIMARY, CERT_CN_HQ_DEBUG)
# What CNs are good to use as HTTP servers.
CERT_CNS_SERVERS = (CERT_OU_NODE, CERT_OU_HOST)

#
# TODO: it would be much better to take the permitted certificate
# as a configuration option.
#
TRUSTED_CERTS = map(partial(crypto.load_certificate, crypto.FILETYPE_PEM),
                    (  # Primary
                     """
-----BEGIN CERTIFICATE-----
MIIDPDCCAiQCCQEXOh3ffAUACjANBgkqhkiG9w0BAQ0FADBUMRIwEAYDVQQLEwlI
USBzZXJ2ZXIxJDAiBgNVBAMTG0RlYnVnIENlcnRpZmljYXRlIEF1dGhvcml0eTEY
MBYGA1UEChMPQ2FsYXRoaSBwcm9qZWN0MB4XDTEyMDQyNzEzMDkyNloXDTE3MDQy
NjEzMDkyNlowVDESMBAGA1UECxMJSFEgc2VydmVyMSQwIgYDVQQDExtEZWJ1ZyBD
ZXJ0aWZpY2F0ZSBBdXRob3JpdHkxGDAWBgNVBAoTD0NhbGF0aGkgcHJvamVjdDCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALjCWwYslnJJGGT8SxbSg2am
v7hpLkk8Ely6xKtNN8MDUqCk/dj6nm7HIhnU1sc2JrKsvNpmivX5I8ACWDA0hocH
EvjW6BpU/cQUEMc+OnNUyJkqDqu9Wq8Ycq/zRf6pIoLCDyPDRtpGMhfA245n/q06
AFl+K1S/w040QzXP/GxdKaGi+PnJVBQonTkiUdhUnt2ESS8rniQKLsJalSSg2HhJ
5Cgu82AOKhaVhZ0q8Jfp109frH2hrmj7P0FHOiAKTKO87IOlXS1uWIJJi//2xfcO
tdOKw6noSSxMgdsB8x88nqPD9v3gJRoMya0626tzdYJBImyB1QMlCwCyWGdLjW8C
AwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIBATANBgkqhkiG9w0BAQ0FAAOCAQEA
QQ6dNDOOJl2Z9ejdnWlyyYp00hYyeiWvqCuQma6DmLKw5fu5VUrdNx3MHh6bGPXv
T46JAQWgEK6uG1AtoPdMy9jYEvtAkmMM39PH/uPg3TpL7jtQctp1/ZCv6OiuI2Wo
Kb4Y42VrGmcsWP3t10Zfjgd7GOnI5fnFGy5jMESLBcLJML6VS8W7Grtn2bm2BAja
37vaqRSAAXmZk+igauWZciuZryCE8aADOAITr9fbLeEKSnouaNda0GfTutn2RS7a
9kvHAlnYrzEhQvJ4dccNEJNxfywymbkYg3zw11nSLeVRMBvwr8XSxLIl2zqrNzni
L6hWvSgBeTxhE8xewYVF3w==
-----END CERTIFICATE-----
                     """,
#                        # Debug
#                      """
# -----BEGIN CERTIFICATE-----
# MIIDPDCCAiQCCQEW9drslyzH4jANBgkqhkiG9w0BAQ0FADBUMRIwEAYDVQQLEwlI
# USBzZXJ2ZXIxJDAiBgNVBAMTG0RlYnVnIENlcnRpZmljYXRlIEF1dGhvcml0eTEY
# MBYGA1UEChMPQ2FsYXRoaSBwcm9qZWN0MB4XDTEwMTIxMzIyMTUyMVoXDTE1MTIx
# MjIyMTUyMVowVDESMBAGA1UECxMJSFEgc2VydmVyMSQwIgYDVQQDExtEZWJ1ZyBD
# ZXJ0aWZpY2F0ZSBBdXRob3JpdHkxGDAWBgNVBAoTD0NhbGF0aGkgcHJvamVjdDCC
# ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMLuWch61ZiIW9iJGnBjh6v2
# BE/yq4FWKYw8TJOu9UMfZuH6mcn+5ZYeaF0z6Vg4JvYr8f51e/PAknYfVV316uGi
# P3ONL36hQDIeCPg/b032kGxdZqDFccuJcw/ACx4P5ApK5z+0p3QlEIKYe2iZ3tsz
# uPvJA57LFLgDZTdopBIxAH4DtvqIYZrZfMWp2nhneH1sT8stvayLng8NO2kEfj+l
# lYsBxUviBQ0BkeAy/5bATrXMVpKTmGpkc6zSHkBLCmf75pKIS+P/VrFHKHWjVqUt
# bgXbk6RXU60Xu/QrEU+sqhqPQysTmvFs8Hn7T0OtkZgc1l3KE3dQK7KTHDV8uJMC
# AwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIBATANBgkqhkiG9w0BAQ0FAAOCAQEA
# R3hZCql45weF5XXClzA2J1I6fM0BkqvlrMwn3rAsXt8xNYgQc8hNcDdzESN6yFx6
# QcxPRetws4LWTUIj96o7oRO6cIdE/a6aa1UlBiZszCDJLB8LPwju+LyZO1hduTBp
# BBcinaXiPWAdiGDN7eHpMMKdGNrjE65O6FNr06nziFcoByp9KNyp4n76o8tCshzp
# nrpWuq6Wsp/ozZg+uHkZsgVRGQvBW91fHZbagVYLQlT+NHhfDwnBd8dq0HKrsYP1
# /0VfqLj+JvT7AmaRA8qHTivrMBvf83fYUiKk7z3FNBmmd0OLQEY/5y3mbAENFHqT
# QMam8NiJxPn9aHMkWxERjA==
# -----END CERTIFICATE-----
#                      """,
                     ))



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Functions

@contract_epydoc
def createKeyPair():
    """
    Create a public/private key pair.

    @returns: The public/private key pair in a PKey object.
    @rtype: crypto.PKey
    """
    pkey = crypto.PKey()
    pkey.generate_key(OPENSSL_KEY_TYPE, OPENSSL_KEY_SIZE)
    return pkey


@contract_epydoc
def createCertRequest(pkey, **name):
    """
    Create a certificate request.

    @param pkey: The key to associate with the request
    @type pkey: crypto.PKey

    @keyword name: The name of the subject of the request, possible
        arguments are:
        C     - Country name
        ST    - State or province name
        L     - Locality name
        O     - Organization name (always forced to be "Calathi project")
        OU    - Organizational unit name
        CN    - Common name
        emailAddress - E-mail address
    @returns: The certificate request in an X509Req object
    @rtype: crypto.X509Req
    """
    req = crypto.X509Req()
    subj = req.get_subject()

    assert 'O' not in name, repr(name)
    for (key, value) in name.iteritems():
        setattr(subj, key, value)
    setattr(subj, 'O', CERT_O)

    x = crypto.X509Extension('subjectAltName',
                             True,
                             'otherName:1.2;UTF8:some other identifier')
    req.add_extensions([x])

    req.set_pubkey(pkey)
    req.sign(pkey, OPENSSL_DIGEST)
    return req


@exceptions_logged(logger)
@contract_epydoc
def createCertificate(req,
                      issuerCert,
                      issuerKey,
                      notBefore=0,
                      notAfter=long(OPENSSL_HOST_KEY_DURATION.total_seconds()),
                      extensions=None):
    """Generate a certificate given a certificate request.

    @param req: Certificate request to use
    @type req: crypto.X509Req

    @param issuerCert: The certificate of the issuer
    @type issuerCert: (crypto.X509Req, crypto.X509)

    @param issuerKey: The private key of the issuer
    @type issuerKey: crypto.PKey

    @param notBefore: Timestamp (relative to now) when the certificate
                      starts being valid
    @type notBefore: numbers.Integral

    @param notAfter: Timestamp (relative to now) when the certificate
                     stops being valid
    @type notAfter: numbers.Integral

    @returns: The signed certificate in an X509 object
    @rtype: crypto.X509
    """
    if extensions is None:
        extensions = []

    cert = crypto.X509()
    cert.set_serial_number(int(datetime.utcnow().strftime('%Y%m%d%H%M%S%f')))
    cert.gmtime_adj_notBefore(notBefore)
    cert.gmtime_adj_notAfter(notAfter)
    cert.set_issuer(issuerCert.get_subject())
    cert.set_subject(req.get_subject())
    cert.set_pubkey(req.get_pubkey())
    if extensions:
        cert.add_extensions(extensions)
    cert.sign(issuerKey, OPENSSL_DIGEST)
    return cert


@exceptions_logged(logger)
@contract_epydoc
def validate_hq_name(x509name):
    """
    @type x509name: crypto.X509Name

    @returns: Whether the x509 name refers to a valid HQ
              (this does not include checking the certificate chain).
    @rtype: bool
    """
    cl_o = (x509name.O == CERT_O)
    cl_ou = (x509name.OU == CERT_OU_HQ)
    cl_cn = (x509name.CN in CERT_CNS_HQ)
    result = cl_o and cl_ou and cl_cn
    # Should parse X509 elements to strings early,
    # as they cannot be pickled and passed to the GUI.
    logger.debug('validate_hq_name(%r) = %r (%r, %r, %r)',
                 str(x509name), result,
                 cl_o, cl_ou, cl_cn)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_node_name(x509name, uuid=None):
    """
    @type x509name: crypto.X509Name

    @param uuid: If passed, this should be the node UUID.
    @type uuid: NoneType, UUID

    @returns: Whether the x509 name refers to a valid Node
              (this does not include checking the certificate chain).
    @rtype: bool
    """
    # Parse UUID, but only if needed.
    if uuid is not None:
        try:
            uuid_str, count_str = x509name.CN.split(',')
            first_uuid = UUID(uuid_str)
            last_uuid = UUID(int=first_uuid.int + int(count_str))
        except:
            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            # That is: DO NOT REMOVE str()!!!
            logger.error('%s cannot be parsed to CN in %s for %r',
                         str(x509name.CN), str(x509name), uuid)
            return False
    else:
        first_uuid = last_uuid = uuid

    cl_o = (x509name.O == CERT_O)
    cl_ou = (x509name.OU == CERT_OU_NODE)
    cl_uuid = (uuid is None or first_uuid <= uuid <= last_uuid)
    result = cl_o and cl_ou and cl_uuid
    logger.debug('validate_node_name(%r, %r) = %r (%r, %r, %r; %r <= %r)',
                 str(x509name), uuid, result,
                 cl_o, cl_ou, cl_uuid, first_uuid, last_uuid)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_host_name(x509name, uuid=None):
    """
    @type x509name: crypto.X509Name

    @param uuid: If passed, this should be the host UUID.
    @type uuid: NoneType, UUID

    @returns: Whether the x509 name refers to a valid Host
              (this does not include checking the certificate chain).
    @rtype: bool
    """
    # Parse UUID, but only if needed.
    if uuid is not None:
        try:
            host_uuid = UUID(x509name.CN)
        except:
            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.error('%s is not a valid CN in %s for %r',
                         str(x509name.CN), str(x509name), uuid)
            return False
    else:
        host_uuid = None

    cl_o = (x509name.O == CERT_O)
    cl_ou = (x509name.OU == CERT_OU_HOST)
    cl_uuid = uuid in (None, host_uuid)
    result = cl_o and cl_ou and cl_uuid
    logger.debug('validate_host_name(%r, %r) = %r (%r, %r, %r; exp %r)',
                 str(x509name), uuid, result,
                 cl_o, cl_ou, cl_uuid, host_uuid)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_hq(x509):
    """
    @type x509: crypto.X509

    @returns: Whether the x509 object refers to a valid HQ
              (this does not include checking the certificate chain).
    @rtype: bool
    """
    issuer = x509.get_issuer()
    subject = x509.get_subject()

    cl_issuer = validate_hq_name(issuer)
    cl_subject = validate_hq_name(subject)
    result = cl_issuer and cl_subject
    logger.debug('validate_hq(%r) = %r (%r: %r, %r: %r)',
                 str(x509), result,
                 str(issuer), cl_issuer, str(subject), cl_subject)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_node(x509, node_uuid=None):
    """
    @type x509: crypto.X509

    @param node_uuid: If passed, this should be the node UUID.
    @type node_uuid: NoneType, UUID

    @returns: Whether the x509 object refers to a valid Node.
    @rtype: bool
    """
    issuer = x509.get_issuer()
    subject = x509.get_subject()

    cl_issuer = validate_hq_name(issuer)
    cl_subject = validate_node_name(subject, node_uuid)
    result = cl_issuer and cl_subject
    logger.debug('validate_node(%r, %r) = %r (%r: %r, %r: %r)',
                 str(x509), node_uuid, result,
                 str(issuer), cl_issuer, str(subject), cl_subject)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_host(x509, node_uuid=None, host_uuid=None):
    """
    @type x509: crypto.X509

    @param node_uuid: If passed, this should be the node UUID.
    @type node_uuid: NoneType, UUID

    @param host_uuid: If passed, this should be the host UUID.
    @type host_uuid: NoneType, UUID

    @returns: Whether the x509 object refers to a valid Host.
    @rtype: bool
    """
    issuer = x509.get_issuer()
    subject = x509.get_subject()

    cl_issuer = validate_node_name(issuer, node_uuid)
    cl_subject = validate_host_name(subject, host_uuid)
    result = cl_issuer and cl_subject
    logger.debug('validate_host(%r, %r, %r) = %r (%r: %r, %r: %r)',
                 str(x509), node_uuid, host_uuid, result,
                 str(issuer), cl_issuer, str(subject), cl_subject)
    return result


@exceptions_logged(logger)
@contract_epydoc
def validate_host_req(x509):
    """
    @type x509: crypto.X509Req

    @returns: Whether the x509 object refers to a valid Host.
    @rtype: bool
    """
    subject = x509.get_subject()

    cl_subject = validate_host_name(subject)

    result = cl_subject
    logger.debug('validate_host_req(%r) = %r (%r: %r)',
                 str(x509), result,
                 str(subject), cl_subject)
    return result



#
# Classes
#



class Error(object):
    """
    Error code for SSL operations.

    The error code names match ones from OpenSSL library,
    with the exclusion that the X509_V_OK is named OK
    and the  prefix is dropped.
    """
    __slots__ = ('code',)

    OK                                 = 0
    UNABLE_TO_GET_ISSUER_CERT          = 2
    UNABLE_TO_GET_CRL                  = 3
    UNABLE_TO_DECRYPT_CERT_SIGNATURE   = 4
    UNABLE_TO_DECRYPT_CRL_SIGNATURE    = 5
    UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY = 6
    CERT_SIGNATURE_FAILURE             = 7
    CRL_SIGNATURE_FAILURE              = 8
    CERT_NOT_YET_VALID                 = 9
    CERT_HAS_EXPIRED                   = 10
    CRL_NOT_YET_VALID                  = 11
    CRL_HAS_EXPIRED                    = 12
    ERROR_IN_CERT_NOT_BEFORE_FIELD     = 13
    ERROR_IN_CERT_NOT_AFTER_FIELD      = 14
    ERROR_IN_CRL_LAST_UPDATE_FIELD     = 15
    ERROR_IN_CRL_NEXT_UPDATE_FIELD     = 16
    OUT_OF_MEM                         = 17
    DEPTH_ZERO_SELF_SIGNED_CERT        = 18
    SELF_SIGNED_CERT_IN_CHAIN          = 19
    UNABLE_TO_GET_ISSUER_CERT_LOCALLY  = 20
    UNABLE_TO_VERIFY_LEAF_SIGNATURE    = 21
    CERT_CHAIN_TOO_LONG                = 22
    CERT_REVOKED                       = 23
    INVALID_CA                         = 24
    PATH_LENGTH_EXCEEDED               = 25
    INVALID_PURPOSE                    = 26
    CERT_UNTRUSTED                     = 27
    CERT_REJECTED                      = 28
    SUBJECT_ISSUER_MISMATCH            = 29
    AKID_SKID_MISMATCH                 = 30
    AKID_ISSUER_SERIAL_MISMATCH        = 31
    KEYUSAGE_NO_CERTSIGN               = 32
    APPLICATION_VERIFICATION           = 50

    messages = {
        OK:
            'ok',
        UNABLE_TO_GET_ISSUER_CERT:
            'unable to get issuer certificate',
        UNABLE_TO_GET_CRL:
            'unable to get certificate CRL',
        UNABLE_TO_DECRYPT_CERT_SIGNATURE:
            "unable to decrypt certificate's signature",
        UNABLE_TO_DECRYPT_CRL_SIGNATURE:
            "unable to decrypt CRL's signature",
        UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY:
            'unable to decode issuer public key',
        CERT_SIGNATURE_FAILURE:
            'certificate signature failure',
        CRL_SIGNATURE_FAILURE:
            'CRL signature failure',
        CERT_NOT_YET_VALID:
            'certificate is not yet valid',
        CERT_HAS_EXPIRED:
            'certificate has expired',
        CRL_NOT_YET_VALID:
            'CRL is not yet valid',
        CRL_HAS_EXPIRED:
            'CRL has expired',
        ERROR_IN_CERT_NOT_BEFORE_FIELD:
            "format error in certificate's notBefore field",
        ERROR_IN_CERT_NOT_AFTER_FIELD:
            "format error in certificate's notAfter field",
        ERROR_IN_CRL_LAST_UPDATE_FIELD:
            "format error in CRL's lastUpdate field",
        ERROR_IN_CRL_NEXT_UPDATE_FIELD:
            "format error in CRL's nextUpdate field",
        OUT_OF_MEM:
            'out of memory',
        DEPTH_ZERO_SELF_SIGNED_CERT:
            'self signed certificate',
        SELF_SIGNED_CERT_IN_CHAIN:
            'self signed certificate in certificate chain',
        UNABLE_TO_GET_ISSUER_CERT_LOCALLY:
            'unable to get local issuer certificate',
        UNABLE_TO_VERIFY_LEAF_SIGNATURE:
            'unable to verify the first certificate',
        CERT_CHAIN_TOO_LONG:
            'certificate chain too long',
        CERT_REVOKED:
            'certificate revoked',
        INVALID_CA:
            'invalid CA certificate',
        PATH_LENGTH_EXCEEDED:
            'path length constraint exceeded',
        INVALID_PURPOSE:
            'unsupported certificate purpose',
        CERT_UNTRUSTED:
            'certificate not trusted',
        CERT_REJECTED:
            'certificate rejected',
        SUBJECT_ISSUER_MISMATCH:
            'subject issuer mismatch',
        AKID_SKID_MISMATCH:
            'authority and subject key identifier mismatch',
        AKID_ISSUER_SERIAL_MISMATCH:
            'authority and issuer serial number mismatch',
        KEYUSAGE_NO_CERTSIGN:
            'key usage does not include certificate signing',
        APPLICATION_VERIFICATION:
            'application verification failure',
    }


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self, code):
        """
        @type code: int
        """
        self.code = code
        if code not in Error.messages:
            logger.warning('Unknown error code: %r', code)


    def __str__(self):
        return Error.messages.get(self.code, 'N/A ({!r})'.format(self.code))


    def __repr__(self):
        return u'<ssl.Error {!r} ("{!s}")>'.format(self.code, self)



class ServerContextFactory(t_i_ssl.ContextFactory):
    """
    L{ServerContextFactory} is a factory for server-side SSL context
    objects.

    The code is based on ssl.DefaultOpenSSLContextFactory.

    @ivar _contextFactory: A callable which will be used to create new
        context objects.  This is typically L{SSL.Context}.
    """
    __slots__ = ('private_key', 'certificate',
                 '__extra_trusted_certificate',
                 '__node_uuid',
                 'sslmethod', '_contextFactory')


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self, private_key, certificate, node_uuid, extra_cert,
                 sslmethod=SSL.SSLv23_METHOD, _contextFactory=SSL.Context):
        """
        @param private_key: SSL private key.
        @type private_key: crypto.PKey

        @param certificate: SSL X509 certificate.
        @type certificate: crypto.X509

        @param node_uuid: UUID of the the server entity (Node or Host).
        @type node_uuid: UUID

        @param extra_cert: A function that may return
            the additional certificate considered always trusted.
        @type extra_cert: crypto.X509

        @param sslmethod: The SSL method to use.
        """
        self.private_key = private_key
        self.certificate = certificate
        self.__node_uuid = node_uuid
        self.__trusted_certs = list(TRUSTED_CERTS)
        self.__extra_trusted_certificate = extra_cert
        self.sslmethod = sslmethod
        self._contextFactory = _contextFactory


    @exceptions_logged(logger)
    def __verifyCallback(self, connection, x509, errnum, depth, oknum):
        """
        This function is called by SSL context
        whenever a peer is being validated.
        """
        error = Error(errnum)
        ok = bool(oknum)

        if ok:
            # Additional validations
            assert errnum == Error.OK, repr(errnum)

            if depth == 2:
                ok = validate_hq(x509)
                if not ok:
                    error = 'HQ revalidation failed'
            elif depth == 1:
                ok = validate_node(x509, self.__node_uuid)
                if not ok:
                    error = 'Node revalidation failed'
            elif depth == 0:
                ok = validate_host(x509, node_uuid=self.__node_uuid)
                if not ok:
                    error = 'Host revalidation failed'
            else:
                # Should parse X509 elements to strings early,
                # as they cannot be pickled and passed to the GUI.
                logger.error('Unknown depth %i for certificate %s',
                             depth, str(x509.get_subject()))

        if not ok:
            # After the additional validations, is it still valid?

            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.error(
                'SSL server validation failed for subject %s signed by %s, '
                'for node %s, at depth %r with error: %s',
                str(x509.get_subject()), str(x509.get_issuer()),
                self.__node_uuid, depth, error)

        return ok


    @exceptions_logged(logger)
    def getContext(self):
        ctx = self._contextFactory(self.sslmethod)
        # Disallow SSLv2!
        ctx.set_options(SSL.OP_NO_SSLv2)

        # Add all trusted certificates,..
        _cert_store = ctx.get_cert_store()
        logger.debug('ServerContextFactory: trusted certificates:\n%s',
                     '\n'.join('\t{!r}'.format(c.get_subject())
                                   for c in self.__trusted_certs))
        for cert in self.__trusted_certs:
            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.debug('Adding trusted certificate: %s',
                         str(cert.get_subject()))
            _cert_store.add_cert(cert)

        # ... including any extra ones.
        # When running on a Node, this will be
        # the certificate of the Node itself;
        # when running on a Host, this will be the certificate
        # of the primary Node for this Host.
        _extra_cert = self.__extra_trusted_certificate
        assert _extra_cert is not None
        if _extra_cert is not None:
            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.debug('Adding extra trusted certificate: %s',
                         str(_extra_cert.get_subject()))
            _cert_store.add_cert(_extra_cert)
            #ctx.add_extra_chain_cert(_extra_cert)

        ctx.use_certificate(self.certificate)
        ctx.use_privatekey(self.private_key)
        ctx.set_verify_depth(SSL_VALIDATION_DEPTH)
        ctx.set_verify(SSL.VERIFY_PEER,
                       self.__verifyCallback)

        return ctx


class ClientContextFactory(t_i_ssl.ClientContextFactory):
    """
    L{ClientContextFactory} is a factory for client-side SSL context
    objects.

    The code is based on ssl.ClientContextFactory.

    It doesn't have the valid implementation,
    but should be subclassed with specific classes
    which have the proper C{._verifyCallback()}, C{._get_trusted_certs()}
    and C{._get_extra_chain_certs()} methods implemented.

    @todo: When the twisted.internet.ssl.ClientContextFactory
        becomes a new-style class, change to super().
    """

    isClient = 1

    __slots__ = ('private_key', 'certificate', 'sslmethod', '_contextFactory')


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self, private_key, certificate,
                 sslmethod=SSL.SSLv23_METHOD, _contextFactory=SSL.Context):
        """
        @param private_key: SSL private key of the Host
            (or None if unknown yet).
        @type private_key: NoneType, crypto.PKey

        @param certificate: SSL X509 certificate of the Host
            (or None if unknown yet).
        @type certificate: NoneType, crypto.X509

        @param sslmethod: The SSL method to use.
        """
        self.private_key = private_key
        self.certificate = certificate

        self.sslmethod = sslmethod
        self._contextFactory = _contextFactory


    @exceptions_logged(logger)
    def getContext(self):
        ctx = self._contextFactory(self.sslmethod)
        # See comment in DefaultOpenSSLContextFactory about SSLv2.
        ctx.set_options(SSL.OP_NO_SSLv2)

        # Add trusted certificates.
        _cert_store = ctx.get_cert_store()
        _trusted_certs = self._get_trusted_certs()
        logger.debug(
            'Client context: the following certificates '
            'are trusted by the client:\n%s',
            '\n'.join('\t{!r}/{!r}'.format(_cert.get_subject(),
                                           _cert.get_issuer())
                          for _cert in _trusted_certs))
        for _cert in _trusted_certs:
            _cert_store.add_cert(_cert)

        # Add extra certificates to the chain.
        _extra_certs = self._get_extra_chain_certs()
        if _extra_certs:
            logger.debug(
                'Client context: the following extra certificates '
                'added to the chain:\n%s',
                '\n'.join('\t{!r}/{!r}'.format(_cert.get_subject(),
                                               _cert.get_issuer())
                              for _cert in _extra_certs))
            for _cert in _extra_certs:
                ctx.add_extra_chain_cert(_cert)

        ctx.set_verify_depth(SSL_VALIDATION_DEPTH)
        ctx.set_verify(SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT,
                       self._verifyCallback)

        if self.certificate is not None and\
           self.private_key is not None:
            ctx.use_certificate(self.certificate)
            ctx.use_privatekey(self.private_key)

        return ctx


    @exceptions_logged(logger)
    def _verifyCallback(self, connection, x509, errnum, depth, oknum):
        """
        This function is called by SSL context
        whenever a peer is being validated.

        Implement in every subclass!

        @type connection: SSL.Connection

        @type x509: crypto.X509
        """
        raise NotImplementedError()


    @exceptions_logged(logger)
    def _get_trusted_certs(self):
        """Get the list of trusted certificates.

        Implement in every subclass!
        """
        raise NotImplementedError()


    @exceptions_logged(logger)
    def _get_extra_chain_certs(self):
        """
        Get the list of extra certificates which should be added to the chain.

        Implement in every subclass!
        """
        raise NotImplementedError()



class ClientToNodeContextFactory(ClientContextFactory):
    """
    L{ClientToNodeContextFactory} is a factory for client-side SSL context
    objects for Host-to-Node connections.
    """

    __slots__ = ('__expect_node_uuid', '__trusted_certs')


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self, node_uuid, *args, **kwargs):
        """
        @param node_uuid: The expected UUID of the Node.
        @type node_uuid: UUID
        """
        ClientContextFactory.__init__(self, *args, **kwargs)
        self.__expect_node_uuid = node_uuid
        self.__trusted_certs = list(TRUSTED_CERTS)


    @exceptions_logged(logger)
    def _verifyCallback(self, connection, x509, errnum, depth, oknum):
        """
        @see: _verifyCallback in ClientContextFactory
        """
        ok = bool(oknum)

        error = Error(errnum)

        if ok:
            # Additional validations
            assert errnum == Error.OK, repr(errnum)

            if depth == 1:
                ok = validate_hq(x509)
                if not ok:
                    error = 'HQ revalidation failed'
            elif depth == 0:
                ok = validate_node(x509, self.__expect_node_uuid)
                if not ok:
                    error = 'Node revalidation failed'
            else:
                # Should parse X509 elements to strings early,
                # as they cannot be pickled and passed to the GUI.
                logger.error('Unknown depth %i for certificate %s',
                             depth, str(x509.get_subject()))

        if not ok:
            # After the additional validations, is it still valid?

            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.warning('SSL client H2N validation failed '
                               'for subject %s signed by %s, '
                               'expecting node %s, at level %r with error: %s',
                           str(x509.get_subject()), str(x509.get_issuer()),
                           self.__expect_node_uuid, depth, error)
            logger.debug('Failed x509: serial = %r, PKey = %r',
                         str(x509.get_serial_number()), str(x509.get_pubkey()))
        else:
            logger.debug('Successfully validated SSL: %s at depth %r',
                         str(x509.get_subject()), depth)

        return ok


    @exceptions_logged(logger)
    def _get_trusted_certs(self):
        """
        @see: _get_trusted_certs() in ClientContextFactory
        """
        logger.debug('Calculating trusted certs for '
                     'host-to-node connection...')
        return self.__trusted_certs


    @exceptions_logged(logger)
    def _get_extra_chain_certs(self):
        """
        @see: _get_extra_chain_certs() in ClientContextFactory
        """
        logger.debug('Calculating extra certs for host-to-node connection...')
        return []



class ClientToHostContextFactory(ClientContextFactory):
    """
    L{ClientToHostContextFactory} is a factory for client-side SSL context
    objects for Host-to-Host connections.
    """

    __slots__ = ('__expect_node_uuid', '__expect_host_uuid',
                 '__trusted_certs', '__extra_ssl_cert')


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self, node_uuid, host_uuid, extra_ssl_cert, *args, **kwargs):
        """
        @param node_uuid: The expected UUID of the Node.
        @type node_uuid: UUID

        @param host_uuid: The expected UUID of the Node.
        @type host_uuid: UUID

        @type extra_ssl_cert: crypto.X509
        """
        ClientContextFactory.__init__(self, *args, **kwargs)
        self.__expect_node_uuid = node_uuid
        self.__expect_host_uuid = host_uuid
        self.__trusted_certs = list(TRUSTED_CERTS)
        self.__extra_ssl_cert = extra_ssl_cert


    @exceptions_logged(logger)
    def _verifyCallback(self, connection, x509, errnum, depth, oknum):
        """
        @see: _verifyCallback in ClientContextFactory
        """
        ok = bool(oknum)

        error = Error(errnum)

        if ok:
            # Additional validations
            assert errnum == Error.OK, repr(errnum)

            if depth == 2:
                ok = validate_hq(x509)
                if not ok:
                    error = 'HQ revalidation failed'
            elif depth == 1:
                ok = validate_node(x509,
                                   node_uuid=self.__expect_node_uuid)
                if not ok:
                    error = 'Node revalidation failed'
            elif depth == 0:
                ok = validate_host(x509,
                                   node_uuid=self.__expect_node_uuid,
                                   host_uuid=self.__expect_host_uuid)
                if not ok:
                    error = 'Host revalidation failed'
            else:
                # Should parse X509 elements to strings early,
                # as they cannot be pickled and passed to the GUI.
                logger.error('Unknown depth %i for certificate %s',
                             depth, str(x509.get_subject()))

        if not ok:
            # After the additional validations, is it still valid?

            # Should parse X509 elements to strings early,
            # as they cannot be pickled and passed to the GUI.
            logger.warning(
                'SSL client H2H validation failed '
                'for subject %s signed by %s, '
                'expecting node %s, host %s, at level %r with error: %s',
                str(x509.get_subject()), str(x509.get_issuer()),
                self.__expect_node_uuid, self.__expect_host_uuid, depth, error)
        else:
            logger.debug('Successfully validated SSL: %s at depth %r',
                         str(x509.get_subject()), depth)

        return ok


    @exceptions_logged(logger)
    def _get_trusted_certs(self):
        """
        @see: _get_trusted_certs in ClientContextFactory
        """
        # Copy
        result = self.__trusted_certs

        logger.debug('Calculating trusted certs '
                         'for host-to-host connection...')

        return result


    @exceptions_logged(logger)
    def _get_extra_chain_certs(self):
        """
        @see: _get_extra_chain_certs() in ClientContextFactory
        """
        result = []

        assert self.__extra_ssl_cert is not None
        if self.__extra_ssl_cert is not None:
            result.append(self.__extra_ssl_cert)

        logger.debug('Calculating extra certs for host-to-host connection...')

        return result

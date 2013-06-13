#!/usr/bin/python
"""
Implementation of Credential factory over the SSL data
from the peer certificate, assuming the certificate itself
was validated already.
"""

from __future__ import absolute_import

import logging

from zope.interface import implements

from twisted.cred import credentials
from twisted.web.iweb import ICredentialFactory

from .utils import exceptions_logged



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ISSLCertificate(credentials.ICredentials):
    """
    This credential is used when an X509 certificate is received
    from the other side via the SSL/TLS layer.
    """

    def checkX509Certificate(x509):
        """
        Check the X.509 certificate.
        """
        pass



class SSLCredentials(object):
    """
    Yet Another Simple HTTP Digest authentication scheme.
    """
    implements(ISSLCertificate)


    def __init__(self, cert):
        self.cert = cert


    @exceptions_logged(logger)
    def checkX509Certificate(self, x509):
        """
        @todo: Probably not needed at all.
        """
        pass


class IUsernameDigestHash(credentials.ICredentials):
    """
    This credential is used when a CredentialChecker has access to the hash
    of the username:realm:password as in an Apache .htdigest file.
    """
    def checkHash(digestHash):
        """
        @param digestHash: The hashed username:realm:password to check against.

        @return: C{True} if the credentials represented by this object match
            the given hash, C{False} if they do not, or a L{Deferred} which
            will be called back with one of these values.
        """
        pass


class SSLCredentialFactory(object):
    implements(ICredentialFactory)

    scheme = 'ssl'


    def __init__(self):
        # Nothing to create, actually
        pass


    @exceptions_logged(logger)
    def getChallenge(self, request):
        """
        Generate the challenge for use in the WWW-Authenticate header

        @see: ICredentialFactory

        @param request: The L{IRequest} to which access was denied and for the
                        response to which this challenge is being generated.

        @return: The empty (for this function) dict.
        """
        return {}


    @exceptions_logged(logger)
    def decode(self, response, request):
        """
        Create an L{SSLCredentials} object from the
        given response and request.

        @see: L{ICredentialFactory.decode}
        """
        return SSLCredentials(request.transport.getPeerCertificate())

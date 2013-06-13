#!/usr/bin/python
"""LOGIN message implementation.

LOGIN message contains the information about the username who tries to log in,
as well as Host UUIDs it can potentially already have. Also, for SSL security,
it contains the newly generated SSL Certificate Request.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from types import NoneType
from uuid import UUID

from OpenSSL import crypto

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractMessage
from common.inhabitants import UserGroup

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class LoginMessage(AbstractMessage):
    """LOGIN message.

    @cvar priority: Overrides the C{priority} field from AbstractMessage,
                    meaning the the login message are very important
                    and should take precedence over most of the other messages.
    """

    name = 'LOGIN'
    version = 0

    priority = 90

    __slots__ = ('username', 'host_uuid_candidates', 'cert_request',
                 'ack_result', 'ack_username', 'ack_host_uuid', 'ack_groups',
                 'ack_ssl_cert')


    def __init__(self, *args, **kwargs):
        super(LoginMessage, self).__init__(*args, **kwargs)
        self.revive = False  # AbstractMessage

        self.username = None
        self.host_uuid_candidates = []
        self.cert_request = None

        # Response
        self.ack_result = None
        self.ack_username = None
        self.ack_host_uuid = None
        self.ack_groups = None
        self.ack_ssl_cert = None


    def get_headers(self):
        assert isinstance(self.host_uuid_candidates, col.Iterable), \
               repr(self.host_uuid_candidates)
        # We don't actually need to perform an opposite C{init_from_headers},
        # as these headers are passed on higher levels.

        return {} if self.host_uuid_candidates is None \
                  else {'x-host-uuid-candidates':
                            [u.hex for u in self.host_uuid_candidates]}


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert isinstance(self.cert_request, (crypto.X509Req, NoneType)), \
               repr(self.cert_request)
        assert isinstance(self.username, str), repr(self.username)

        result = {'username': self.username}
        if self.cert_request is not None:
            result['SSL request'] = \
                crypto.dump_certificate_request(crypto.FILETYPE_PEM,
                                                self.cert_request)
        return result


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        self.username = str(body['username'])
        if 'SSL request' in body:
            self.cert_request = \
                crypto.load_certificate_request(crypto.FILETYPE_PEM,
                                                body['SSL request'])


    @bzip2
    @json_dumps
    @contract_epydoc
    def _get_body_ack(self):
        """
        @precondition: self.ack_ssl_cert is None or
                       isinstance(self.ack_ssl_cert, crypto.X509)
        """
        # N2H
        _host_uuid = self.ack_host_uuid
        _result = self.ack_result

        assert _host_uuid is None or \
               isinstance(_host_uuid, UUID)
        assert _result in ('ok', 'dual login', 'fail')

        assert ((_result == 'ok' and _host_uuid is not None) or
                (_result != 'ok' and _host_uuid is None)), \
               (_result, _host_uuid)

        result = {'result': _result}
        if _result == 'ok':
            result.update({
                'username': self.ack_username,
                'uuid': _host_uuid.hex,
                'ack_groups': [gr.to_json()
                                   for gr in self.ack_groups]
            })

        if self.ack_ssl_cert is not None:
            result['SSL certificate'] = \
                crypto.dump_certificate(crypto.FILETYPE_PEM,
                                        self.ack_ssl_cert)

        return result


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H
        assert isinstance(body, dict), repr(body)
        _result = self.ack_result = body['result']

        uuid = None
        if _result == 'ok':
            self.ack_username = str(body['username'])  # forcibly de-unicode
            try:
                uuid = UUID(body['uuid'])
            except:
                logger.error('In LoginMessage, received invalid UUID: %r',
                             body)

        self.ack_host_uuid = uuid
        self.ack_groups = \
            [UserGroup.from_json(gr)()
                 for gr in body['ack_groups']] if 'ack_groups' in body \
                                               else []

        try:
            self.ack_ssl_cert = \
                crypto.load_certificate(crypto.FILETYPE_PEM,
                                        body['SSL certificate'])
        except:
            self.ack_ssl_cert = None

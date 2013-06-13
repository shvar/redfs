#!/usr/bin/python
"""
LOGIN transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging

from contrib.dbc import contract_epydoc

from common import ssl
from common.abstractions import pauses

from protocol import transactions

from host import db
from host.db import HostQueries

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class LoginTransaction_Host(transactions.LoginTransaction,
                            AbstractHostTransaction):
    """LOGIN transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the LOGIN transaction on the Host."""

        __slots__ = ('username', 'host_uuid_candidates', 'ssl_req',
                     # For output
                     'ack_result', 'ack_username', 'ack_host_uuid',
                     'ack_groups', 'ack_ssl_cert')

        name = 'LOGIN'


        @contract_epydoc
        def __init__(self,
                     username, host_uuid_candidates, ssl_req,
                     *args, **kwargs):
            """
            @type username: str
            @type host_uuid_candidates: col.Iterable
            """
            super(LoginTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.username = username
            self.host_uuid_candidates = host_uuid_candidates
            self.ssl_req = ssl_req
            self.ack_result = self.ack_username = self.ack_host_uuid \
                            = self.ack_groups = self.ack_ssl_cert = None


        def __str__(self):
            return (u'username={self.username!r}, '
                     'host_uuid_candidates={self.host_uuid_candidates!r}, '
                     'ssl_req={self.ssl_req!r}'
                         .format(self=self))



    @pauses
    def on_begin(self):

        with self.open_state() as state:
            logger.debug('Querying %r', state.username)
            self.message.username = state.username
            self.message.host_uuid_candidates = state.host_uuid_candidates
            self.message.cert_request = state.ssl_req

        self.manager.post_message(self.message)


    def on_end(self):
        if not self.failure:
            logger.debug('END RECEIVED: %r', self.message_ack)

            _ack = self.message_ack
            _node_uuid = _ack.src.uuid

            _result = _ack.ack_result
            _username = _ack.ack_username
            _host_uuid = _ack.ack_host_uuid
            _user_groups = _ack.ack_groups
            _ssl_cert = _ack.ack_ssl_cert

            if _host_uuid is not None and _ssl_cert is not None:
                # Received the certificate. But is it valid?
                if ssl.validate_host(_ssl_cert,
                                     node_uuid=_node_uuid,
                                     host_uuid=_host_uuid):
                    logger.debug('Received the SSL cert for %r (%r), saving.',
                                 _username, _ssl_cert)

                    with db.RDB() as rdbw:
                        # Note that this most likely written
                        # to the NULL_UUID db.
                        HostQueries.HostSettings.set(
                            HostQueries.HostSettings.SSL_CERTIFICATE,
                            _ssl_cert,
                            rdbw=rdbw)
                        HostQueries.HostUsers.set_my_user_groups(
                            _username, _user_groups, rdbw=rdbw)
                        logger.debug('Written result for Login transaction.')

                else:
                    logger.error('Received the wrong certificate %r '
                                     'signed by %r: '
                                     'from %r, to %r',
                                 _ssl_cert.get_subject(),
                                 _ssl_cert.get_issuer(),
                                 _node_uuid, _host_uuid)
                    _ssl_cert = None  # for future writing to the state

            with self.open_state(for_update=True) as state:
                if state.username != _username:
                    # That's ok, it may be a case mismatch
                    logger.debug('Requested username %r but received %r',
                                 state.username, _username)

                state.ack_result = _result
                state.ack_username = _username
                state.ack_host_uuid = _host_uuid
                state.ack_groups = _user_groups
                state.ack_ssl_cert = _ssl_cert

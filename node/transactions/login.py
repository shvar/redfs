#!/usr/bin/python
"""
LOGIN transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from OpenSSL import crypto

from common import ssl
from common.abstractions import unpauses_incoming
from common.db import Queries

from protocol import transactions

from trusted import db
from trusted.db import TrustedQueries
from trusted.docstore.models.transaction_states.login import \
    LoginTransactionState_Node

from node.settings import get_common

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class LoginTransaction_Node(transactions.LoginTransaction,
                            AbstractNodeTransaction):
    """LOGIN transaction on the Node."""

    __slots__ = ()

    State = LoginTransactionState_Node


    @unpauses_incoming
    def on_begin(self):
        assert isinstance(self.message.username, str), \
               repr(self.message.username)


    def on_end(self):
        # By this time, whatever process to determine the host UUID
        # for the user we use, we've definitely completed it,
        # and the proper host UUID is present in the source message.
        _host = self.message.src
        _message = self.message
        _ack = self.message_ack = self.message.reply()

        if _host is None:
            _ack.ack_result = 'fail'
        elif self.manager.app.known_hosts.is_peer_alive(_host.uuid):
            _ack.ack_result = 'dual login'
        else:
            _ack.ack_result = 'ok'
            _ack.ack_host_uuid = _host.uuid

            with db.RDB() as rdbw:
                user_groups = list(Queries.Inhabitants
                                          .get_groups_for_user(_host.user.name,
                                                               rdbw))

                thost_uuids = \
                    {h.uuid
                         for h in TrustedQueries.HostAtNode
                                                .get_all_trusted_hosts(
                                                     rdbw=rdbw)}

            is_trusted_host = _host.uuid in thost_uuids

            _ack.ack_username = self.message.src.user.name
            _ack.ack_groups = user_groups

            logger.debug('For user %r, %s %r, groups are: %r',
                         _ack.ack_username,
                         'trusted host' if is_trusted_host else 'host',
                         _host.uuid,
                         _ack.ack_groups)

            # If we have an SSL request, create the certificate
            _req = _message.cert_request
            if _req is not None:
                assert isinstance(_req, crypto.X509Req), \
                       repr(_req)

                subj = _req.get_subject()

                if not ssl.validate_host_req(_req):
                    # Need explicit str() here, as subj is non-pickleable!
                    logger.warning('%s probably trying to spoof himself: %r',
                                   _host.uuid, str(subj))
                else:
                    # Force CN to be the host UUID
                    subj.commonName = str(_host.uuid)
                    node_cert = get_common().ssl_cert
                    node_key = get_common().ssl_pkey
                    _key_duration = ssl.OPENSSL_TRUSTED_HOST_KEY_DURATION \
                                        if is_trusted_host \
                                        else ssl.OPENSSL_HOST_KEY_DURATION
                    _ack.ack_ssl_cert = \
                        ssl.createCertificate(
                            _req,
                            node_cert, node_key,
                            notAfter=long(_key_duration.total_seconds()))

        self.manager.post_message(self.message_ack)

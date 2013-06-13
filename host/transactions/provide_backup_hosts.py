#!/usr/bin/python
"""
PROVIDE_BACKUP_HOSTS transaction implementation on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging

from contrib.dbc import contract_epydoc

from common.abstractions import pauses

from protocol import transactions

from ._host import AbstractHostTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class ProvideBackupHostsTransaction_Host(
          transactions.ProvideBackupHostsTransaction,
          AbstractHostTransaction):
    """PROVIDE_BACKUP_HOSTS transaction on the Host."""

    __slots__ = ()



    class State(AbstractHostTransaction.State):
        """The state for the PROVIDE_BACKUP_HOSTS transaction on the Host."""

        __slots__ = ('chunk_count_by_size_code',
                     'ack_result_code', 'ack_hosts_to_use')

        name = 'PROVIDE_BACKUP_HOSTS'


        @contract_epydoc
        def __init__(self, chunk_count_by_size_code, *args, **kwargs):
            """
            @type chunk_count_by_size_code: col.Mapping
            """
            super(ProvideBackupHostsTransaction_Host.State, self) \
                .__init__(*args, **kwargs)
            self.chunk_count_by_size_code = dict(chunk_count_by_size_code)
            self.ack_result_code = None
            self.ack_hosts_to_use = None


        def __str__(self):
            return (
                u'chunk_count_by_size_code={self.chunk_count_by_size_code!r}'
                 '{opt_ack_result_code!s}{opt_ack_hosts_to_use!s}'
                    .format(self=self,
                            opt_ack_result_code=
                                ', ack_result_code={!r}'.format(
                                        self.ack_result_code)
                                    if self.ack_result_code is not None
                                    else '',
                            opt_ack_hosts_to_use=
                                u', ack_hosts_to_use={!r} '.format(
                                        self.ack_hosts_to_use)
                                    if self.ack_hosts_to_use is not None
                                    else ''))


    @pauses
    def on_begin(self):
        with self.open_state() as state:
            self.message.chunks_by_size_code = state.chunk_count_by_size_code

        self.manager.post_message(self.message)


    def on_end(self):
        """
        @todo: Handle NO_MORE_URLS situation and switch to
               "via-node NAT passthrough".
        """
        _msg_ack = self.message_ack
        if _msg_ack is None:
            logger.warning('%r failed', self)
        else:
            with self.open_state(for_update=True) as state:
                state.ack_result_code = _msg_ack.ack_result_code
                state.ack_hosts_to_use = _msg_ack.ack_hosts_to_use

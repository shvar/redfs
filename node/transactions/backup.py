#!/usr/bin/python
"""
BACKUP transaction implementation on the Node.
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from common.abstractions import pauses

from trusted.docstore.models.transaction_states.backup import \
    BackupTransactionState_Node

from protocol import transactions

from ._node import AbstractNodeTransaction



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class BackupTransaction_Node(transactions.BackupTransaction,
                             AbstractNodeTransaction):
    """BACKUP transaction on the Node."""

    __slots__ = ()

    State = BackupTransactionState_Node


    @pauses
    def on_begin(self):
        """Send the backup request to the host."""
        assert self.is_outgoing()

        with self.open_state() as state:
            self.message.dataset_uuid = state.dataset_uuid
        logger.debug('Backing up dataset %r', self.message.dataset_uuid)

        self.manager.post_message(self.message)


    def on_end(self):
        """Receive the backup confirmation from the host."""
        assert self.is_outgoing()
        with self.open_state(for_update=True) as state:
            state.ack_result_code = self.message_ack.ack_result_code



#
# Unit tests
#

if __debug__:
    __test__ = {
        '__repr__':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID
            >>> BackupTransactionState_Node(
            ...     tr_start_time=
            ...         datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...     tr_uuid=
            ...         UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...     tr_src_uuid=
            ...         UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...     tr_dst_uuid=
            ...         UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ...     dataset_uuid=
            ...         UUID('60fb9667-67ae-4e3c-9c86-edac180dd4e6')
            ... )  # doctest:+NORMALIZE_WHITESPACE
            BackupTransactionState_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                dataset_uuid=UUID('60fb9667-67ae-4e3c-9c86-edac180dd4e6'))
            """,
        'docstore':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID
            >>> from common.inhabitants import Host, Node
            >>> from common.transaction_manager_in_memory import \
            ...     TransactionManagerInMemory
            >>> from common.typed_uuids import DatasetUUID, PeerUUID, \
            ...                                MessageUUID
            >>> from protocol import messages
            >>> from node import transactions as node_transactions
            >>> from common.test.test_node_app import TestNodeApp

            >>> # Prepare application and manager.
            >>> test_app = TestNodeApp()
            >>> tr_manager = TransactionManagerInMemory(
            ...     tr_classes=node_transactions.ALL_TRANSACTION_CLASSES,
            ...     app=test_app)

            >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
            >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

            >>> # Let's test the basic message creation.
            >>> msg1 = messages.BackupMessage(
            ...            src=node,
            ...            dst=host,
            ...            uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'))
            >>> messages.BackupMessage \
            ...         ._init_from_body \
            ...         ._without_bunzip2._without_json_loads(
            ...              msg1,
            ...              {'dataset_uuid':
            ...                   '60fb966767ae4e3c9c86edac180dd4e6'})
            >>> msg1  # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
            BackupMessage(src=Node(uuid=PeerUUID('11111111-...aed9')),
                          dst=Host(uuid=PeerUUID('00000000-...c98d')),
                          uuid=MessageUUID('4ac2536a-...-11bd675fdf15'),
                          status_code=0
                          [ dataset_uuid=DatasetUUID('60fb9667-...180dd4e6') ])
            >>> ''.join(msg1.get_body())  # doctest:+ELLIPSIS
            'BZh51AY&SY\xda\xb9,/\x00\x00\x19\x1b...\xc2\x84\x86\xd5\xc9ax'

            >>> # Now create (just create!) the transaction from this message.
            >>> tr_class = tr_manager._ta_class_by_msg_name['BACKUP']
            >>> tr1 = tr_class(
            ...           manager=tr_manager, message=msg1, parent=None,
            ...           start_time=datetime(2012, 9, 27, 15, 33, 43, 605083))

            >>> tr1  # doctest:+ELLIPSIS
            BackupTransaction_Node(... [0/0] IN)

            >>> # Create a state
            >>> st1 = tr_class.State(
            ...           tr_start_time=datetime(2012, 9, 27, 15, 33, 43,
            ...                                  605083),
            ...           tr_uuid=msg1.uuid,
            ...           tr_src_uuid=msg1.src.uuid,
            ...           tr_dst_uuid=msg1.dst.uuid,
            ...           dataset_uuid=msg1.dataset_uuid)
            >>> st1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            BackupTransactionState_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 27, 15, 33, 43, 605083),
                tr_uuid=MessageUUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'),
                tr_src_uuid=PeerUUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'),
                tr_dst_uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d'),
                dataset_uuid=DatasetUUID('60fb9667-...-edac180dd4e6'))

            >>> # Is the state properly serializable?
            >>> st1.to_bson()  # doctest:+NORMALIZE_WHITESPACE
            {'dataset_uuid':
                 DatasetUUID('60fb9667-67ae-4e3c-9c86-edac180dd4e6')}

            >>> # Is the whole transaction properly serializable?
            >>> tr1.to_model(st1)  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            Transaction(_id=None,
                type_='BACKUP',
                uuid=MessageUUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'),
                src=PeerUUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'),
                dst=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d'),
                ts=datetime.datetime(2012, 9, 27, 15, 33, 43, 605083),
                state=Backup...State_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 27, 15, 33, 43, 605083),
                    tr_uuid=MessageUUID('4ac2536a-...-11bd675fdf15'),
                    tr_src_uuid=PeerUUID('11111111-...-8c4ce6a2aed9'),
                    tr_dst_uuid=PeerUUID('00000000-...-a6c8728ac98d'),
                    dataset_uuid=DatasetUUID('60fb9667-...-edac180dd4e6')))
            >>> tr1.to_model(st1).to_bson()  # doctest:+NORMALIZE_WHITESPACE
            {'src': PeerUUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'),
             'state': {'dataset_uuid':
                          DatasetUUID('60fb9667-67ae-4e3c-9c86-edac180dd4e6')},
             'uuid': MessageUUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'),
             'dst': PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d'),
             'type': 'BACKUP',
             'ts': datetime.datetime(2012, 9, 27, 15, 33, 43, 605083)}
            """
    }

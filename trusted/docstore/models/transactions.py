#!/usr/bin/python
"""The FastDB model for the transaction being executed on the node."""

#
# Imports
#

from __future__ import absolute_import
from datetime import datetime
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.typed_uuids import PeerUUID, TransactionUUID

from trusted.ibsonable import IBSONable

from . import transaction_states
from .transaction_states._common import NodeTransactionState
from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class Transaction(IDocStoreTopDocument, IBSONable):
    """A transaction being executed on the node."""

    __slots__ = (
        'type', 'uuid', 'src', 'dst', 'ts', 'state',
        # Optional
        'parent', 'wake_up_ts'
    )

    bson_schema = {
        'type': basestring,
        'uuid': UUID,
        'src': UUID,
        'dst': UUID,
        'ts': datetime,
        'state': dict,
        'parent': (UUID, NoneType),
        'wake_up_ts': (datetime, NoneType),
    }


    def __init__(self,
                 type_, uuid, src, dst, ts, state,
                 parent=None, wake_up_ts=None,
                 *args, **kwargs):
        """Constructor.

        @param type_: the type of the transaction.
        @type type_: basestring

        @param uuid: the UUID of the transaction.
        @type uuid: TransactionUUID

        @param src: the UUID of the transaction originator.
        @type src: PeerUUID

        @param dst: the UUID of the transaction received.
        @type dst: PeerUUID

        @type ts: datetime

        @type state: NodeTransactionState

        @param parent: the UUID of the parent transaction.
        @type parent: TransactionUUID, NoneType

        @param wake_up_ts: the next time when the transaction must be awoken.
        @type wake_up_ts: datetime, NoneType
        """
        super(Transaction, self).__init__(*args, **kwargs)
        self.type = type_
        self.uuid = uuid
        self.src = src
        self.dst = dst
        self.ts = ts
        self.state = state
        # Optional
        self.parent = parent
        self.wake_up_ts = wake_up_ts


    def __str__(self):
        r"""
        >>> st = transaction_states.BackupTransactionState_Node(
        ...          dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'),
        ...          tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...          tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...          tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...          tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

        >>> # No optional arguments
        >>> Transaction(
        ...     _id=None,
        ...     type_='BACKUP',
        ...     uuid=st.tr_uuid,
        ...     src=st.tr_src_uuid,
        ...     dst=st.tr_dst_uuid,
        ...     ts=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     state=st
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Transaction(_id=None,
            type_='BACKUP',
            uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            src=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            dst=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
            state=Backup...State_Node(tr_start_time=....datetime(2012, 9, 26,
                                                                 14, 29, 48,
                                                                 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')))

        >>> # All optional arguments
        >>> Transaction(
        ...     _id=None,
        ...     type_='BACKUP',
        ...     uuid=st.tr_uuid,
        ...     src=st.tr_src_uuid,
        ...     dst=st.tr_dst_uuid,
        ...     ts=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     state=st,
        ...     parent=UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
        ...     wake_up_ts=datetime(2012, 10, 3, 12, 31, 8, 279807)
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Transaction(_id=None,
            type_='BACKUP',
            uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            src=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            dst=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
            state=Backup...State_Node(tr_start_time=....datetime(2012, 9, 26,
                                                                 14, 29, 48,
                                                                 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')),
            parent=UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
            wake_up_ts=datetime.datetime(2012, 10, 3, 12, 31, 8, 279807))
        """
        return (u'{super}, '
                u'type_={self.type!r}, '
                u'uuid={self.uuid!r}, '
                u'src={self.src!r}, '
                u'dst={self.dst!r}, '
                u'ts={self.ts!r}, '
                u'state={self.state!r}'
                u'{opt_parent}'
                u'{opt_wake_up_ts}'.format(
                    super=super(Transaction, self).__str__(),
                    self=self,
                    opt_parent=
                        '' if self.parent is None
                           else ', parent={!r}'.format(self.parent),
                    opt_wake_up_ts=
                        '' if self.wake_up_ts is None
                           else ', wake_up_ts={!r}'.format(self.wake_up_ts)))


    def to_bson(self):
        """
        >>> st = transaction_states.BackupTransactionState_Node(
        ...          dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'),
        ...          tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...          tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...          tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...          tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

        >>> # No optional arguments
        >>> Transaction(
        ...     _id=None,
        ...     type_='BACKUP',
        ...     uuid=st.tr_uuid,
        ...     src=st.tr_src_uuid,
        ...     dst=st.tr_dst_uuid,
        ...     ts=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     state=st
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
         'state': {'dataset_uuid':
                       UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
         'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
         'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
         'type': 'BACKUP',
         'ts': datetime.datetime(2012, 9, 26, 14, 29, 48, 877434)}

        >>> # All optional arguments
        >>> Transaction(
        ...     _id=None,
        ...     type_='BACKUP',
        ...     uuid=st.tr_uuid,
        ...     src=st.tr_src_uuid,
        ...     dst=st.tr_dst_uuid,
        ...     ts=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     state=st,
        ...     parent=UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
        ...     wake_up_ts=datetime(2012, 10, 3, 12, 31, 8, 279807)
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
         'state': {'dataset_uuid':
                       UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
         'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
         'parent': UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
         'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
         'wake_up_ts': datetime.datetime(2012, 10, 3, 12, 31, 8, 279807),
         'type': 'BACKUP',
         'ts': datetime.datetime(2012, 9, 26, 14, 29, 48, 877434)}
        >>>
        """
        cls = self.__class__

        doc = super(Transaction, self).to_bson()

        doc.update({
            'type': self.type,
            'uuid': self.uuid,
            'src': self.src,
            'dst': self.dst,
            'ts': self.ts,
            'state': self.state.to_bson(),
        })

        # Optional fields
        if self.wake_up_ts is not None:
            doc.update({'wake_up_ts': self.wake_up_ts})
        if self.parent is not None:
            doc.update({'parent': self.parent})

        assert cls.validate_schema(doc), repr(doc)
        assert self.state.__class__.validate_schema(doc['state']), repr(doc)

        return doc


    @classmethod
    def state_from_tr_bson(cls, doc):
        r"""
        >>> from common.typed_uuids import DatasetUUID, MessageUUID, PeerUUID

        >>> # No optional arguments
        >>> Transaction.state_from_tr_bson({
        ...     'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     'state': {'dataset_uuid':
        ...                   UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
        ...     'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ...     'type': 'BACKUP',
        ...     'ts': datetime(2012, 9, 26, 14, 29, 48, 877434)
        ... })  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupTransactionState_Node(tr_start_time=....datetime(2012, 9, 26,
                                                               14, 29, 48,
                                                               877434),
            tr_uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            dataset_uuid=DatasetUUID('cf9a54b1-1239-48de-9b25-d7fa927db125'))

        >>> # All optional arguments
        >>> Transaction.state_from_tr_bson({
        ...     'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     'state': {'dataset_uuid':
        ...                   UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
        ...     'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     'parent': UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
        ...     'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ...     'wake_up_ts': datetime(2012, 10, 3, 12, 31, 8, 279807),
        ...     'type': 'BACKUP',
        ...     'ts': datetime(2012, 9, 26, 14, 29, 48, 877434)
        ... })  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupTransactionState_Node(tr_start_time=....datetime(2012, 9, 26,
                                                               14, 29, 48,
                                                               877434),
            tr_uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            dataset_uuid=DatasetUUID('cf9a54b1-1239-48de-9b25-d7fa927db125'))

        @type: NodeTransactionState
        """
        assert cls.validate_schema(doc), repr(doc)

        state_class = \
            transaction_states.TRANSACTION_STATE_CLASS_BY_NAME[doc['type']]

        assert state_class.validate_schema(doc['state']), repr(doc)

        return state_class.from_bson(doc['state'])(
                   tr_start_time=doc['ts'],
                   tr_uuid=TransactionUUID.safe_cast_uuid(doc['uuid']),
                   tr_src_uuid=PeerUUID.safe_cast_uuid(doc['src']),
                   tr_dst_uuid=PeerUUID.safe_cast_uuid(doc['dst']))


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> from common.typed_uuids import DatasetUUID, MessageUUID, PeerUUID

        >>> # No optional arguments
        >>> Transaction.from_bson({
        ...     'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     'state': {'dataset_uuid':
        ...                   UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
        ...     'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ...     'type': 'BACKUP',
        ...     'ts': datetime(2012, 9, 26, 14, 29, 48, 877434)
        ... })() # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Transaction(_id=None,
            type_='BACKUP',
            uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            src=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            dst=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
            state=BackupTransactionState_Node(tr_start_time=....datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                dataset_uuid=DatasetUUID('cf9a54b1-...-d7fa927db125')))

        >>> # All optional arguments
        >>> Transaction.from_bson({
        ...     'src': UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     'state': {'dataset_uuid':
        ...                   UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')},
        ...     'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     'parent': UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
        ...     'dst': UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ...     'wake_up_ts': datetime(2012, 10, 3, 12, 31, 8, 279807),
        ...     'type': 'BACKUP',
        ...     'ts': datetime(2012, 9, 26, 14, 29, 48, 877434)
        ... })() # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Transaction(_id=None,
            type_='BACKUP',
            uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            src=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            dst=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
            state=Backup...State_Node(tr_start_time=....datetime(2012, 9, 26,
                                                                 14, 29, 48,
                                                                 877434),
                tr_uuid=MessageUUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=PeerUUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=PeerUUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                dataset_uuid=DatasetUUID('cf9a54b1-1239-...-d7fa927db125')),
            parent=UUID('a096cf37-dc4f-4d0e-9488-da50443c8c40'),
            wake_up_ts=datetime.datetime(2012, 10, 3, 12, 31, 8, 279807))

        @rtype: Transaction
        """
        assert cls.validate_schema(doc), repr(doc)

        state = cls.state_from_tr_bson(doc)

        return partial(super(Transaction, cls).from_bson(doc),
                       type_=state.tr_type,
                       uuid=TransactionUUID.safe_cast_uuid(state.tr_uuid),
                       src=PeerUUID.safe_cast_uuid(state.tr_src_uuid),
                       dst=PeerUUID.safe_cast_uuid(state.tr_dst_uuid),
                       ts=state.tr_start_time,
                       state=state,
                         # Optional
                       parent=doc.get('parent'),
                       wake_up_ts=doc.get('wake_up_ts'))



#
# Unit tests
#

if __debug__:
    __test__ = {
        'transaction_state_in_docstore':
            r"""
            >>> class SampleTransactionState(NodeTransactionState):
            ...     name = 'SAMPLE'
            ...
            ...     def __init__(self, a, b=None, *args, **kwargs):
            ...         super(SampleTransactionState, self) \
            ...             .__init__(*args, **kwargs)
            ...         self.a = a
            ...         self.b = b
            ...
            ...     def __str__(self):
            ...         return u'a={self.a!r}, b={self.b!r}'.format(self=self)
            ...
            ...     def to_bson(self):
            ...         doc = super(SampleTransactionState, self).to_bson()
            ...         # Mandatory fields
            ...         doc.update({'a': self.a})
            ...         # Optional fields
            ...         if self.b is not None:
            ...             doc.update({'b': self.b})
            ...         return doc
            ...
            ...     @classmethod
            ...     def from_bson(cls, doc):
            ...         assert cls.validate_schema(doc), repr(doc)
            ...
            ...         return partial(super(SampleTransactionState, cls)
            ...                            .from_bson(doc),
            ...                          # Mandatory
            ...                        a=doc['a'],
            ...                          # Optional
            ...                        b=doc.get('b'))

            >>> st1 = SampleTransactionState(
            ...     5,
            ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
            >>> st2 = SampleTransactionState(
            ...     7,
            ...     12,
            ...     tr_start_time=datetime(2012, 9, 26, 14, 31, 58, 550259),
            ...     tr_uuid=UUID('e9bf25cb-5c4b-4181-aa42-6116fe0104bc'),
            ...     tr_src_uuid=UUID('75144c01-8b6e-4538-841f-3b5a69a59014'),
            ...     tr_dst_uuid=UUID('3cc0808c-a24b-48e0-acca-9320ef25039b'))
            >>> st1
            SampleTransactionState(a=5, b=None)
            >>> st2
            SampleTransactionState(a=7, b=12)
            >>> st1.to_bson()
            {'a': 5}
            >>> st2.to_bson()
            {'a': 7, 'b': 12}

            >>> st1new = SampleTransactionState.from_bson({'a': 5})(
            ...     tr_start_time=datetime(2012, 9, 26, 14, 31, 58, 550259),
            ...     tr_uuid=UUID('e9bf25cb-5c4b-4181-aa42-6116fe0104bc'),
            ...     tr_src_uuid=UUID('75144c01-8b6e-4538-841f-3b5a69a59014'),
            ...     tr_dst_uuid=UUID('3cc0808c-a24b-48e0-acca-9320ef25039b')
            ... )
            >>> st1new  # doctest:+NORMALIZE_WHITESPACE
            SampleTransactionState(a=5, b=None)
            >>> (st1new.tr_type, st1new.tr_start_time, st1new.tr_uuid,
            ...  st1new.tr_src_uuid, st1new.tr_dst_uuid
            ... ) # doctest:+NORMALIZE_WHITESPACE
            ('SAMPLE',
             datetime.datetime(2012, 9, 26, 14, 31, 58, 550259),
             UUID('e9bf25cb-5c4b-4181-aa42-6116fe0104bc'),
             UUID('75144c01-8b6e-4538-841f-3b5a69a59014'),
             UUID('3cc0808c-a24b-48e0-acca-9320ef25039b'))

            >>> st2new = SampleTransactionState.from_bson({'a': 7, 'b': 12})(
            ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
            ... )
            >>> st2new  # doctest:+NORMALIZE_WHITESPACE
            SampleTransactionState(a=7, b=12)
            >>> (st2new.tr_type, st2new.tr_start_time, st2new.tr_uuid,
            ...  st2new.tr_src_uuid, st2new.tr_dst_uuid
            ... ) # doctest:+NORMALIZE_WHITESPACE
            ('SAMPLE',
             datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
             UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
             UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
             UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
            """,
}

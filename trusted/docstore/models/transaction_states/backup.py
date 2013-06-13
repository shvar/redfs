#!/usr/bin/python
"""
BACKUP transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import numbers
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

from common.typed_uuids import DatasetUUID

from ._common import NodeTransactionState



#
# Classes
#

class BackupTransactionState_Node(NodeTransactionState):
    """The state for the BACKUP transaction on the Node."""

    __slots__ = ('dataset_uuid', 'ack_result_code')

    name = 'BACKUP'

    bson_schema = {
        'dataset_uuid': UUID,
        # Optional
        'ack_result_code': (numbers.Integral, NoneType),
    }


    @contract_epydoc
    def __init__(self, dataset_uuid, ack_result_code=None, *args, **kwargs):
        r"""Constructor.

        >>> from datetime import datetime
        >>> BackupTransactionState_Node(
        ...     dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'),
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupTransactionState_Node(tr_start_time=....datetime(2012, 9, 26,
                                                               14, 29, 48,
                                                               877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'))

        @type dataset_uuid: UUID
        """
        super(BackupTransactionState_Node, self).__init__(*args, **kwargs)
        self.dataset_uuid = dataset_uuid
        # Results
        self.ack_result_code = ack_result_code

        assert self.is_valid_bsonable(), repr(self)


    def __str__(self):
        return (u'{super}, dataset_uuid={self.dataset_uuid!r}'
                    '{opt_ack_result_code}'.format(
                    super=super(BackupTransactionState_Node, self).__str__(),
                    self=self,
                    opt_ack_result_code=
                        ', ack_result_code={!r}'.format(self.ack_result_code)
                            if self.ack_result_code is not None
                            else ''))


    def to_bson(self):
        r"""
        >>> from datetime import datetime
        >>> st = BackupTransactionState_Node(
        ...          dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'),
        ...          tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...          tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...          tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...          tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
        >>> st.to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'dataset_uuid': UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')}
        """
        cls = self.__class__

        doc = super(BackupTransactionState_Node, self).to_bson()
        # Mandatory
        doc.update({'dataset_uuid': self.dataset_uuid})
        # Optional
        if self.ack_result_code is not None:
            doc.update({'ack_result_code': self.ack_result_code})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> from datetime import datetime
        >>> BackupTransactionState_Node.from_bson({
        ...     'dataset_uuid': UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')
        ... })(tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...    tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...    tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...    tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupTransactionState_Node(tr_start_time=....datetime(2012, 9, 26,
                                                               14, 29, 48,
                                                               877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            dataset_uuid=DatasetUUID('cf9a54b1-1239-48de-9b25-d7fa927db125'))
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(BackupTransactionState_Node, cls).from_bson(doc),
                         # Mandatory
                       dataset_uuid=DatasetUUID.safe_cast_uuid(
                                        doc['dataset_uuid']),
                         # Optional
                       ack_result_code=doc.get('ack_result_code'))



if __debug__:
    __test__ = {
        'iprintable':
            r"""
            >>> from datetime import datetime
            >>> from trusted.docstore import models
            >>> st = BackupTransactionState_Node(
            ...          dataset_uuid=
            ...              UUID('cf9a54b1-1239-48de-9b25-d7fa927db125'),
            ...          tr_start_time=
            ...              datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...          tr_uuid=
            ...              UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...          tr_src_uuid=
            ...              UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...          tr_dst_uuid=
            ...              UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

            >>> models.Transaction(
            ...     _id=None,
            ...     type_=st.name,
            ...     uuid=st.tr_uuid,
            ...     src=st.tr_src_uuid,
            ...     dst=st.tr_dst_uuid,
            ...     ts=st.tr_start_time,
            ...     state=st
            ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            Transaction(_id=None,
                type_='BACKUP',
                uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                src=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                dst=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
                state=Backup...State_Node(tr_start_time=....datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                    tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                    tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                    tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                    dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')))
            >>> models.Transaction(
            ...     type_=st.name,
            ...     uuid=st.tr_uuid,
            ...     src=st.tr_src_uuid,
            ...     dst=st.tr_dst_uuid,
            ...     ts=st.tr_start_time,
            ...     state=st
            ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            Transaction(_id=None,
                type_='BACKUP',
                uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                src=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                dst=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
                state=Backup...State_Node(tr_start_time=....datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                    tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                    tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                    tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                    dataset_uuid=UUID('cf9a54b1-1239-48de-9b25-d7fa927db125')))
            """,
    }

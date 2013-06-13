#!/usr/bin/python
"""
LOGIN transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import

from ._common import NodeTransactionState



#
# Classes
#

class LoginTransactionState_Node(NodeTransactionState):
    """The state for the LOGIN transaction on the Node."""

    __slots__ = ()

    name = 'LOGIN'



if __debug__:
    __test__ = {
        '__repr__':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID
            >>> LoginTransactionState_Node(
            ...     tr_start_time=
            ...         datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...     tr_uuid=
            ...         UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...     tr_src_uuid=
            ...         UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...     tr_dst_uuid=
            ...         UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
            ... )  # doctest:+NORMALIZE_WHITESPACE
            LoginTransactionState_Node(tr_start_time=datetime.datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))
            """,
        'transaction_printability':
            r"""
            >>> from datetime import datetime
            >>> from uuid import UUID
            >>> st = LoginTransactionState_Node(
            ...          tr_start_time=
            ...              datetime(2012, 9, 26, 14, 29, 48, 877434),
            ...          tr_uuid=
            ...              UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            ...          tr_src_uuid=
            ...              UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            ...          tr_dst_uuid=
            ...              UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'))

            >>> from trusted.docstore import models
            >>> models.Transaction(
            ...     type_=st.name,
            ...     uuid=st.tr_uuid,
            ...     src=st.tr_src_uuid,
            ...     dst=st.tr_dst_uuid,
            ...     ts=st.tr_start_time,
            ...     state=st
            ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
            Transaction(_id=None,
                type_='LOGIN',
                uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                src=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                dst=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
                ts=datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
                state=Login...State_Node(tr_start_time=....datetime(2012,
                                                    9, 26, 14, 29, 48, 877434),
                    tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
                    tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
                    tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')))
            """,
    }

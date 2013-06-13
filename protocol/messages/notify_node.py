#!/usr/bin/python
"""NOTIFY_NODE message implementation.

NOTIFY_NODE message sends some Host information to the Node.
"""

#
# Imports
#

from __future__ import absolute_import
from abc import ABCMeta

from contrib.dbc import contract_epydoc, consists_of

from common.abstractions import IJSONable, AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class NotifyNodeMessage(AbstractMessage):
    """NOTIFY_NODE message."""

    name = 'NOTIFY_NODE'
    version = 0

    __slots__ = ('payload', )

    SUPPORTED_TYPES = ('error_logs',)


    class AbstractPayload(IJSONable):
        """
        @cvar TYPE: override in subclasses
        """
        __metaclass__ = ABCMeta

        TYPE = None


    class ErrorLogsPayload(AbstractPayload):
        """
        The payload containing the error logs contents.
        """
        __slots__ = ('logs',)

        TYPE = 'error'


        def __init__(self, logs):
            assert isinstance(logs, dict), repr(logs)
            self.logs = logs


        def to_json(self):
            return self.logs


        @classmethod
        def from_json(cls, data):
            return lambda: NotifyNodeMessage.ErrorLogsPayload(data)


    __ALL_PAYLOAD_TYPE_CLASSES = (ErrorLogsPayload,)
    __PAYLOAD_TYPE_TO_CLASS = {cl.TYPE: cl
                                   for cl in __ALL_PAYLOAD_TYPE_CLASSES}


    def __init__(self, *args, **kwargs):
        super(NotifyNodeMessage, self).__init__(*args, **kwargs)
        self.payload = []


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert consists_of(self.payload, NotifyNodeMessage.AbstractPayload), \
               repr(self.payload)

        return [{'type': item.TYPE, 'data': item.to_json()}
                    for item in self.payload]


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        self.payload = \
            [NotifyNodeMessage.__PAYLOAD_TYPE_TO_CLASS[item['type']]
                              .from_json(item['data'])()
                 for item in body]

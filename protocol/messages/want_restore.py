#!/usr/bin/python
"""WANT_RESTORE message implementation.

WANT_RESTORE message contains a request by the Host to the Node to permit
executing a Restore procedure.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from common.abstractions import AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads
from .restore import RestoreMessage



#
# Classes
#

class WantRestoreMessage(AbstractMessage):
    """WANT_RESTORE message.

    @ivar ack_result_code: present in ACK message, contains the response
        whether the restore was completed properly.
    @type ack_result_code: int, NoneType
    """


    class ResultCodes(AbstractMessage.AbstractResultCodes):
        GENERAL_FAILURE = 0
        OK = 1

        @classmethod
        def _all(cls):
            return frozenset([
                cls.GENERAL_FAILURE,
                cls.OK
            ])

        @classmethod
        def _to_str(cls):
            return {
                cls.GENERAL_FAILURE: 'General failure',
                cls.OK:              'Ok'
            }

        @classmethod
        def _is_good(cls):
            return {
                cls.GENERAL_FAILURE: False,
                cls.OK:              True
            }


        @classmethod
        def from_restore_result_code(cls, restore_result_code):
            """
            Currently conversion is easy, but later it may be more complex.
            """
            assert restore_result_code in RestoreMessage.ResultCodes._all(), \
                   repr(restore_result_code)
            return restore_result_code



    name = 'WANT_RESTORE'
    version = 0

    __slots__ = ('dataset_uuid', 'file_paths',
                 'ack_result_code')


    def __init__(self, *args, **kwargs):
        super(WantRestoreMessage, self).__init__(*args, **kwargs)

        self.dataset_uuid = None
        self.file_paths = []

        self.ack_result_code = None


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert isinstance(self.dataset_uuid, UUID)
        assert isinstance(self.file_paths, list) and \
               all(isinstance(fp, basestring) for fp in self.file_paths)

        return {'dataset': self.dataset_uuid.hex,
                'files': self.file_paths}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        assert 'dataset' in body and 'files' in body, \
               repr(body)
        self.dataset_uuid = UUID(body['dataset'])
        self.file_paths = body['files']


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # N2H
        assert isinstance(self.ack_result_code, int), \
               repr(self.ack_result_code)
        return {'result': self.ack_result_code}


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H
        assert isinstance(body, dict), repr(body)
        self.ack_result_code = body['result']

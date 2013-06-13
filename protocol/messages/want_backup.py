#!/usr/bin/python
"""WANT_BACKUP message implementation.

WANT_BACKUP message contains a request by the Host to the Node to permit
executing a Backup procedure.
"""

#
# Imports
#

from __future__ import absolute_import
from uuid import UUID

from common.abstractions import AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads
from .backup import BackupMessage



#
# Classes
#

class WantBackupMessage(AbstractMessage):
    """WANT_BACKUP message.

    @ivar ack_result_code: present in ACK message, contains the response
        whether the backup was suggested.
    @type ack_result_code: int, NoneType
    """


    class ResultCodes(AbstractMessage.AbstractResultCodes):
        GENERAL_FAILURE = 0
        OK = 1
        NO_SPACE_ON_CLOUD = 2
        USER_IS_SUSPENDED = 3

        @classmethod
        def _all(cls):
            return frozenset([
                cls.GENERAL_FAILURE,
                cls.OK,
                cls.NO_SPACE_ON_CLOUD,
                cls.USER_IS_SUSPENDED,
            ])

        @classmethod
        def _to_str(cls):
            return {
                cls.GENERAL_FAILURE:   'General failure',
                cls.OK:                'Ok',
                cls.NO_SPACE_ON_CLOUD: 'No space left on the cloud',
                cls.USER_IS_SUSPENDED: 'User with this host is suspended',
            }

        @classmethod
        def _is_good(cls):
            return {
                cls.GENERAL_FAILURE:   False,
                cls.OK:                True,
                cls.NO_SPACE_ON_CLOUD: False,
                cls.USER_IS_SUSPENDED: False,
            }


        @classmethod
        def from_backup_result_code(cls, backup_result_code):
            """
            Currently conversion is easy, but later it may be more complex.
            """
            assert backup_result_code in BackupMessage.ResultCodes._all(), \
                   repr(backup_result_code)
            return backup_result_code


    name = 'WANT_BACKUP'
    version = 0

    __slots__ = ('dataset_uuid',
                 'ack_result_code')


    def __init__(self, *args, **kwargs):
        super(WantBackupMessage, self).__init__(*args, **kwargs)

        self.dataset_uuid = None

        self.ack_result_code = None


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        assert isinstance(self.dataset_uuid, UUID), repr(self.dataset_uuid)
        return {'dataset_uuid': self.dataset_uuid.hex}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        assert 'dataset_uuid' in body, repr(body)
        self.dataset_uuid = UUID(body['dataset_uuid'])


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

#!/usr/bin/python
"""RESTORE message implementation.

RESTORE message contains the information how to construct the data
to be restored from the available chunks.
"""

#
# Imports
#

from __future__ import absolute_import
import json
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common.abstract_dataset import AbstractDataset, AbstractBasicDatasetInfo
from common.abstractions import AbstractMessage
from common.chunks import Chunk, File, LocalPhysicalFileState
from common.inhabitants import UserGroup
from common.typed_uuids import TransactionUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class RestoreMessage(AbstractMessage):
    """RESTORE message.

    @ivar dataset: the dataset to be restored; None if only
                   selected files from the dataset are to be restored.
    @type dataset: AbstractDataset, NoneType

    @ivar ugroup: the user group of the dataset.
    @type ugroup: UserGroup

    @ivar wr_uuid: the UUID of the WantRestore transaction which invoked this
                   Restore transaction. None if initiated by the Node.
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


    name = 'RESTORE'
    version = 0

    __slots__ = (  # On request
                 'dataset', 'ugroup', 'sync', 'files', 'wr_uuid',
                   # On response
                 'ack_result_code')


    def __init__(self, *args, **kwargs):
        super(RestoreMessage, self).__init__(*args, **kwargs)

        self.dataset = None
        self.ugroup = None
        self.sync = None
        self.files = {}
        self.wr_uuid = None

        self.ack_result_code = None


    @bzip2
    @json_dumps
    def _get_body(self):
        # N2H
        assert isinstance(self.dataset, (AbstractDataset, NoneType)), \
               repr(self.dataset)
        assert isinstance(self.ugroup, UserGroup), \
               repr(self.ugroup)
        assert isinstance(self.sync, bool), \
               repr(self.sync)

        assert consists_of(self.files.iterkeys(), File), \
               repr(self.files)
        assert consists_of(self.files.itervalues(), list), \
               repr(self.files)
        assert consists_of((bl
                                for v in self.files.itervalues()
                                for bl in v),
                           Chunk.Block), \
               repr(self.files)

        assert isinstance(self.wr_uuid, (TransactionUUID, NoneType)), \
               repr(self.wr_uuid)

        res = {}
        if self.dataset is not None:
            res['dataset'] = self.dataset.to_json()

        if self.ugroup is not None:
            res['ugroup'] = self.ugroup.to_json()

        res['sync'] = 1 if self.sync else 0

        _file_map = {json.dumps(file.to_json()): [bl.to_json(bl.chunk,
                                                             with_file=False)
                                                      for bl in blocks]
                         for file, blocks in self.files.iteritems()}
        res['files'] = _file_map

        if self.wr_uuid is not None:
            res['want_restore'] = self.wr_uuid.hex

        return res


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # N2H
        assert 'files' in body, \
               repr(body)

        if 'dataset' in body:
            self.dataset = \
                AbstractBasicDatasetInfo.from_json(body['dataset'])()

        if 'ugroup' in body:
            self.ugroup = UserGroup.from_json(body['ugroup'])()

        self.sync = bool(int(body['sync']))

        _files_pre = {LocalPhysicalFileState.from_json(
                              json.loads(file_key))(): blocks
                          for file_key, blocks in body['files'].iteritems()}
        self.files = {file: [Chunk.Block.from_json(ser_block, file)()
                                  for ser_block in blocks]
                          for file, blocks in _files_pre.iteritems()}

        if 'want_restore' in body:
            self.wr_uuid = TransactionUUID(body['want_restore'])


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # H2N
        assert isinstance(self.ack_result_code, int), \
               repr(self.ack_result_code)
        return {'result': self.ack_result_code}


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # H2N
        assert isinstance(body, dict), repr(body)
        self.ack_result_code = body['result']

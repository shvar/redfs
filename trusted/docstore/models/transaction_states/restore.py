#!/usr/bin/python
"""
RESTORE transaction state in the FastDB.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import numbers
from datetime import datetime, timedelta
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from common.utils import coalesce

from ._common import NodeTransactionState



#
# Classes
#

class RestoreTransactionState_Node(NodeTransactionState):
    """The state for the RESTORE transaction on the Node.

    @type pending_chunk_uuids: set
    @type pending_host_uuids: list
    """

    __slots__ = ('ds_uuid', 'file_paths_for_basedirs', 'wr_uuid',
                 'success',
                 'num', 'of', 'num_bytes', 'of_bytes',
                 'last_progress_recalc_time',
                 'pending_chunk_uuids', 'pending_host_uuids',
                 # Optional
                 'no_donors_retries',
                 'ack_result_code')

    name = 'RESTORE'

    bson_schema = {
        # Mandatory fields
        'ds_uuid': UUID,
        'file_paths_for_basedirs': lambda f: f is None or
                                             isinstance(f, dict),
        'wr_uuid': (UUID, NoneType),
        # Optional-but-always-existing fields
        'success': bool,
        'num': numbers.Integral,
        'of': numbers.Integral,
        'num_bytes': numbers.Integral,
        'of_bytes': numbers.Integral,
        'last_progress_recalc_time': datetime,
        'pending_chunks': lambda f: isinstance(f, list) and
                                    consists_of(f, UUID),
        'pending_hosts': lambda f: isinstance(f, list) and
                                   consists_of(f, UUID),
        # Optional
        # * no_donors_retries - consider 0 if None;
        #   we don't store it to save space, if/while it is 0,
        #   cause it is so for most of the data.
        'no_donors_retries': (numbers.Integral, NoneType),
        'ack_result_code': (numbers.Integral, NoneType),
    }


    @contract_epydoc
    def __init__(self,
                 ds_uuid,
                 file_paths_for_basedirs=None, wr_uuid=None,
                 success=False, num=0, of=0, num_bytes=0, of_bytes=0,
                 last_progress_recalc_time=None,
                 pending_chunk_uuids=None, pending_host_uuids=None,
                 no_donors_retries=0,
                 ack_result_code=None,
                 *args, **kwargs):
        r"""Constructor.

        >>> # No optional arguments.
        >>> RestoreTransactionState_Node(
        ...     ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Restore...State_Node(tr_start_time=datetime.datetime(2012, 9, 26,
                                                             14, 29, 48,
                                                             877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'))

        >>> # All optional arguments.
        >>> RestoreTransactionState_Node(
        ...     ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     file_paths_for_basedirs={
        ...         '/home/johndoe': ['.zrc', 'bin/f1', 'music/Abba/01.mp3'],
        ...         '/etc': ['passwd', 'init.d/networking'],
        ...     },
        ...     wr_uuid=UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
        ...     success=False,
        ...     num=15, of=17,
        ...     num_bytes=47, of_bytes=123,
        ...     last_progress_recalc_time=datetime(2012, 11, 5, 16, 7, 37,
        ...                                        88896),
        ...     pending_chunk_uuids=
        ...         [UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c'),
        ...          UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
        ...          UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d')],
        ...     pending_host_uuids=
        ...         [UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
        ...          UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
        ...     no_donors_retries=3,
        ...     ack_result_code=42,
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Restore...State_Node(tr_start_time=datetime.datetime(2012, 9, 26,
                                                             14, 29, 48,
                                                             877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
            file_paths_for_basedirs={'/home/johndoe': ['.zrc', 'bin/f1',
                                                       'music/Abba/01.mp3'],
                                     '/etc': ['passwd', 'init.d/networking']},
            wr_uuid=UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
            num=15, of=17,
            num_bytes=47, of_bytes=123,
            last_progress_recalc_time=datetime.datetime(2012, 11, 5,
                                                        16, 7, 37, 88896),
            pending_chunk_uuids=[UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d'),
                                 UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
                                 UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c')],
            pending_host_uuids=[UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
                                UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
            no_donors_retries=3,
            ack_result_code=42)

        @param ds_uuid: dataset UUID.
        @type ds_uuid: UUID

        @param file_paths_for_basedirs: either an mapping from
            the base directories to the (relative) file paths to restore
            (or even maybe to C{None}, if all files from a base directory
            are needed),
            or C{None} if all files from the dataset are needed.
        @type file_paths_for_basedirs: col.Mapping, NoneType
        @type wr_uuid: UUID, NoneType
        @type pending_chunk_uuids: col.Iterable, NoneType
        @type pending_host_uuids: col.Iterable, NoneType
        @param no_donors_retries: how many retries on "no donors found" case
            have already occured.
        @type no_donors_retries: numbers.Integral
        """
        super(RestoreTransactionState_Node, self) \
            .__init__(*args, **kwargs)

        (self.ds_uuid, self.file_paths_for_basedirs, self.wr_uuid) = \
            (ds_uuid, file_paths_for_basedirs, wr_uuid)
        (self.num, self.of) = (num, of)
        (self.num_bytes, self.of_bytes) = (num_bytes, of_bytes)

        self.last_progress_recalc_time = coalesce(last_progress_recalc_time,
                                                  kwargs['tr_start_time'])
        self.success = success
        self.pending_chunk_uuids = set(coalesce(pending_chunk_uuids, []))
        self.pending_host_uuids = coalesce(pending_host_uuids, [])

        self.no_donors_retries = no_donors_retries

        # Results
        self.ack_result_code = ack_result_code

        assert self.is_valid_bsonable(), repr(self)


    @property
    def is_sync(self):
        """
        Whether the restore transaction is intended to sync
        the whole dataset.

        @rtype: bool
        """
        return self.file_paths_for_basedirs == None


    def __str__(self):
        return (u'{super}, '
                u'ds_uuid={self.ds_uuid!r}'
                u'{opt_file_paths_for_basedirs}'
                u'{opt_wr_uuid}'
                u'{opt_success}'
                u'{opt_num}'
                u'{opt_of}'
                u'{opt_num_bytes}'
                u'{opt_of_bytes}'
                u'{opt_last_progress_recalc_time}'
                u'{opt_pending_chunk_uuids}'
                u'{opt_pending_host_uuids}'
                u'{opt_no_donors_retries}'
                u'{opt_ack_result_code}'.format(
                    super=super(RestoreTransactionState_Node, self).__str__(),
                    self=self,
                 opt_file_paths_for_basedirs=
                     '' if self.file_paths_for_basedirs is None
                        else u', file_paths_for_basedirs={!r}'
                                 .format(self.file_paths_for_basedirs),
                 opt_wr_uuid='' if self.wr_uuid is None
                                else u', wr_uuid={!r}'.format(self.wr_uuid),
                 opt_success='' if self.success == False
                                else u', success={!r}'.format(self.success),
                 opt_num='' if self.num == 0
                            else u', num={!r}'.format(self.num),
                 opt_of='' if self.of == 0
                           else u', of={!r}'.format(self.of),
                 opt_num_bytes='' if self.num_bytes == 0
                               else u', num_bytes={!r}'.format(self.num_bytes),
                 opt_of_bytes='' if self.of_bytes == 0
                              else u', of_bytes={!r}'.format(self.of_bytes),
                 opt_last_progress_recalc_time=
                     '' if self.last_progress_recalc_time == self.tr_start_time
                        else u', last_progress_recalc_time={!r}'.format(
                                 self.last_progress_recalc_time),
                 opt_pending_chunk_uuids=
                     '' if not self.pending_chunk_uuids
                        else u', pending_chunk_uuids={!r}'
                                 .format(list(self.pending_chunk_uuids)),
                 opt_pending_host_uuids=
                     '' if not self.pending_host_uuids
                        else u', pending_host_uuids={!r}'
                                 .format(list(self.pending_host_uuids)),
                 opt_no_donors_retries=
                     '' if self.no_donors_retries == 0
                        else u', no_donors_retries={:d}'
                                 .format(self.no_donors_retries),
                 opt_ack_result_code=
                     '' if self.ack_result_code is None
                        else u', ack_result_code={!r}'
                                 .format(self.ack_result_code)))


    def to_bson(self):
        r"""
        >>> # No optional arguments.
        >>> RestoreTransactionState_Node(
        ...     ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'pending_hosts': [],
         'last_progress_recalc_time': datetime.datetime(2012, 9, 26,
                                                        14, 29, 48, 877434),
         'ds_uuid': UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
         'success': False,
         'of': 0,
         'of_bytes': 0,
         'wr_uuid': None,
         'num_bytes': 0,
         'file_paths_for_basedirs': None,
         'num': 0,
         'pending_chunks': []}

        >>> # All optional arguments.
        >>> RestoreTransactionState_Node(
        ...     ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     file_paths_for_basedirs={
        ...         '/home/johndoe': ['.zrc', 'bin/f1', 'music/Abba/01.mp3'],
        ...         '/etc': ['passwd', 'init.d/networking'],
        ...     },
        ...     wr_uuid=UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
        ...     success=True,
        ...     num=15, of=17,
        ...     num_bytes=47, of_bytes=123,
        ...     last_progress_recalc_time=datetime(2012, 11, 5,
        ...                                        16, 9, 13, 166595),
        ...     pending_chunk_uuids=
        ...         [UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c'),
        ...          UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
        ...          UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d')],
        ...     pending_host_uuids=
        ...         [UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
        ...          UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
        ...     no_donors_retries=3,
        ...     ack_result_code=42,
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ... ).to_bson()  # doctest:+NORMALIZE_WHITESPACE
        {'pending_hosts': [UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
                           UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
         'last_progress_recalc_time': datetime.datetime(2012, 11, 5, 16, 9, 13,
                                                        166595),
         'ds_uuid': UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
         'success': True,
         'of': 17,
         'of_bytes': 123,
         'wr_uuid': UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
         'num_bytes': 47,
         'file_paths_for_basedirs': {'/home/johndoe': ['.zrc', 'bin/f1',
                                                       'music/Abba/01.mp3'],
                                     '/etc': ['passwd', 'init.d/networking']},
         'no_donors_retries': 3,
         'num': 15,
         'pending_chunks': [UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d'),
                            UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
                            UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c')],
         'ack_result_code': 42}
        """
        cls = self.__class__

        doc = super(RestoreTransactionState_Node, self).to_bson()

        # Mandatory fields
        doc.update({
            'ds_uuid': self.ds_uuid,
            'file_paths_for_basedirs':
                None if self.file_paths_for_basedirs is None
                     else dict(self.file_paths_for_basedirs),
            'wr_uuid': self.wr_uuid,
        })

        # Optional-but-always-existing fields
        doc.update({
            'success': self.success,
            'num': self.num,
            'of': self.of,
            'num_bytes': self.num_bytes,
            'of_bytes': self.of_bytes,
            'last_progress_recalc_time': self.last_progress_recalc_time,
            'pending_chunks': list(self.pending_chunk_uuids),
            'pending_hosts': list(self.pending_host_uuids),
        })

        # Optional
        if self.no_donors_retries != 0:
            doc.update({'no_donors_retries': self.no_donors_retries})
        if self.ack_result_code is not None:
            doc.update({'ack_result_code': self.ack_result_code})

        assert cls.validate_schema(doc), repr(doc)
        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> tr_start_time = datetime(2012, 9, 26, 14, 29, 48, 877434)
        >>> tr_uuid = UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2')
        >>> tr_src_uuid = UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24')
        >>> tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d')

        >>> # Deserialize with no optional aruments
        >>> RestoreTransactionState_Node.from_bson({'pending_hosts': [],
        ...     'last_progress_recalc_time': datetime(2012, 9, 26, 14, 29, 48,
        ...                                           877434),
        ...     'ds_uuid': UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     'success': False,
        ...     'of': 0,
        ...     'of_bytes': 0,
        ...     'wr_uuid': None,
        ...     'num_bytes': 0,
        ...     'file_paths_for_basedirs': None,
        ...     'num': 0,
        ...     'pending_chunks': []
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Restore...State_Node(tr_start_time=datetime.datetime(2012, 9, 26,
                                                             14, 29, 48,
                                                             877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'))

        >>> # Deserialize with all optional aruments
        >>> RestoreTransactionState_Node.from_bson({'pending_hosts':
        ...         [UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
        ...          UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
        ...     'last_progress_recalc_time': datetime(2012, 11, 5,
        ...                                           16, 9, 13, 166595),
        ...     'ds_uuid': UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     'success': True,
        ...     'of': 17,
        ...     'of_bytes': 123,
        ...     'wr_uuid': UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
        ...     'num_bytes': 47,
        ...     'file_paths_for_basedirs': {'/home/johndoe':
        ...                                     ['.zrc', 'bin/f1',
        ...                                      'music/Abba/01.mp3'],
        ...                                 '/etc':
        ...                                     ['passwd',
        ...                                      'init.d/networking']},
        ...     'num': 15,
        ...     'pending_chunks':
        ...         [UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d'),
        ...          UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
        ...          UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c')],
        ...     'no_donors_retries': 3,
        ...     'ack_result_code': 42
        ... })(tr_start_time=tr_start_time,
        ...    tr_uuid=tr_uuid,
        ...    tr_src_uuid=tr_src_uuid,
        ...    tr_dst_uuid=tr_dst_uuid
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        Restore...State_Node(tr_start_time=datetime.datetime(2012, 9, 26,
                                                             14, 29, 48,
                                                             877434),
            tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
            tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
            tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
            ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
            file_paths_for_basedirs={'/home/johndoe':
                    ['.zrc', 'bin/f1', 'music/Abba/01.mp3'],
                '/etc':
                    ['passwd', 'init.d/networking']},
            wr_uuid=UUID('998d45e6-a5ea-4ffd-a595-982edfa8e3e9'),
            success=True,
            num=15, of=17, num_bytes=47, of_bytes=123,
            last_progress_recalc_time=datetime.datetime(2012, 11, 5,
                                                        16, 9, 13, 166595),
            pending_chunk_uuids=[UUID('6d78a6e6-e223-4233-9d37-3c16d625b00d'),
                                 UUID('a5dc34b3-b7d1-49fc-8b2f-178eb4ea12fd'),
                                 UUID('cb8fb4d2-563d-413f-80f9-9a55c65ea15c')],
            pending_host_uuids=[UUID('59ad73e3-0cad-4402-8811-65ac4736bdc3'),
                                UUID('6812914c-0c3f-411d-9770-25c56473fc5f')],
            no_donors_retries=3,
            ack_result_code=42)
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(RestoreTransactionState_Node, cls)
                           .from_bson(doc),
                       # Mandatory
                       ds_uuid=doc['ds_uuid'],
                       file_paths_for_basedirs=doc['file_paths_for_basedirs'],
                       wr_uuid=doc['wr_uuid'],
                       # Optional-but-always-existing
                       success=doc['success'],
                       num=doc['num'],
                       of=doc['of'],
                       num_bytes=doc['num_bytes'],
                       of_bytes=doc['of_bytes'],
                       last_progress_recalc_time=
                           doc['last_progress_recalc_time'],
                       pending_chunk_uuids=doc['pending_chunks'],
                       pending_host_uuids=doc['pending_hosts'],
                       # Optional
                       no_donors_retries=doc.get('no_donors_retries', 0),
                       ack_result_code=doc.get('ack_result_code'))


    @contract_epydoc
    def get_progress(self, at_moment=None):
        r"""Return the progress information for this restore transaction.

        >>> now = datetime(2012, 9, 27, 16, 31, 49, 997455)
        >>> RestoreTransactionState_Node(
        ...     ds_uuid=UUID('c126ea26-d7e7-4394-a75a-c6f3ef6bc32a'),
        ...     num=15, of=17, num_bytes=16, of_bytes=64,
        ...     tr_start_time=datetime(2012, 9, 26, 14, 29, 48, 877434),
        ...     tr_uuid=UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
        ...     tr_src_uuid=UUID('fa87ebfd-d498-4ba6-9f04-a933e4512b24'),
        ...     tr_dst_uuid=UUID('e6aa4157-ee8a-449e-a2d5-3340a59e717d'),
        ... ).get_progress(now)  # doctest:+NORMALIZE_WHITESPACE
        {'remaining': datetime.timedelta(0, 12496, 149336),
         'num': 15,
         'uuid': UUID('1a82a181-741d-4a64-86e5-77a7dd000ba2'),
         'started': datetime.datetime(2012, 9, 26, 14, 29, 48, 877434),
         'of': 17,
         'of_bytes': 64,
         'num_bytes': 16,
         'elapsed': datetime.timedelta(1, 7321, 120021)}

        @param at_moment: at which moment the progress is measured.
            If C{None}, considered equal to C{datetime.utcnow()}.
        @type at_moment: datetime, NoneType

        @rtype: dict

        @postcondition: isinstance(result['started'], datetime)
        @postcondition: isinstance(result['uuid'], UUID)
        @postcondition: isinstance(result['num'], numbers.Integral)
        @postcondition: isinstance(result['of'], numbers.Integral)
        @postcondition: isinstance(result['num_bytes'], numbers.Integral)
        @postcondition: isinstance(result['of_bytes'], numbers.Integral)
        @postcondition: isinstance(result['elapsed'],
                                   (timedelta, NoneType))
        @postcondition: isinstance(result['remaining'],
                                   (timedelta, NoneType))
        """
        now = at_moment if at_moment is not None else datetime.utcnow()

        num, of = self.num, self.of
        left = of - num

        num_bytes, of_bytes = self.num_bytes, self.of_bytes

        elapsed = now - self.tr_start_time
        remaining = elapsed * left / num if num else None

        result = {
            # datetime
            'started': self.tr_start_time,

            # uuid
            'uuid': self.tr_uuid,

            # int
            'num': num,
            'of': of,

            'num_bytes': num_bytes,
            'of_bytes': of_bytes,

            # timedelta
            'elapsed': elapsed,
            'remaining': remaining,  # may be None
        }
        return result

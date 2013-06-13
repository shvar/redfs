#!/usr/bin/python
"""PROVIDE_BACKUP_HOSTS message implementation.

PROVIDE_BACKUP_HOSTS message from the Host contains the basic information
about the chunks the donor Host wants to upload somewhere;
the response from the Node to the donor Host contains the information about the
acceptor Hosts which can receive these chunks.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from uuid import UUID

from contrib.dbc import contract_epydoc, consists_of

from common import limits
from common.abstractions import IJSONable, IPrintable, AbstractMessage

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Classes
#

class PerHostChunksData(IPrintable, IJSONable):

    __slots__ = ('chunks_by_size', 'urls')


    @contract_epydoc
    def __init__(self, chunks_by_size, urls, *args, **kwargs):
        """
        @type chunks_by_size: col.Mapping
        @type urls: list
        @precondition: consists_of(urls, basestring)
        """
        super(PerHostChunksData, self).__init__(*args, **kwargs)
        self.urls = urls
        self.chunks_by_size = chunks_by_size


    def __str__(self):
        return u'chunks_by_size={!r}, urls={!r})'.format(self.chunks_by_size,
                                                         self.urls)


    def to_json(self):
        return {'urls': self.urls,
                'chunks': self.chunks_by_size}


    @classmethod
    def from_json(cls, body_data):
        assert tuple(sorted(body_data.iterkeys())) == ('chunks', 'urls')

        # But the chunk size keys are strings, while should be ints.
        return lambda: cls({int(k): v
                                for k, v in body_data['chunks'].iteritems()},
                            body_data['urls'])



class ProvideBackupHostsMessage(AbstractMessage):
    """PROVIDE_BACKUP_HOSTS message.

    @ivar chunks_by_size_code: H2N: The mapping from the chunk size code
                               to the total number of such chunks.
    @type chunks_by_size_code: dict

    @ivar ack_hosts_to_use: N2H: The dictionary mapping the host
                                 (which should be used to upload the chunks)
                                 to the dictionary of chunks sizes
                                 and its counts.
    @type ack_hosts_to_use: dict

    @invariant: all(k in limits.SUPPORTED_CHUNK_SIZE_CODES
                        for k in self.chunks_by_size_code.iterkeys())
    @invariant: all(v > 0 for v in self.chunks_by_size_code.itervalues())
    """

    class ResultCodes(AbstractMessage.AbstractResultCodes):
        GENERAL_FAILURE = 0
        OK = 1
        NO_SPACE_ON_CLOUD = 2

        @classmethod
        def _all(cls):
            return frozenset([
                cls.GENERAL_FAILURE,
                cls.OK,
                cls.NO_SPACE_ON_CLOUD,
            ])

        @classmethod
        def _to_str(cls):
            return {
                cls.GENERAL_FAILURE:   'General failure',
                cls.OK:                'Ok',
                cls.NO_SPACE_ON_CLOUD: 'No space left on the cloud',
            }

        @classmethod
        def _is_good(cls):
            return {
                cls.GENERAL_FAILURE:   False,
                cls.OK:                True,
                cls.NO_SPACE_ON_CLOUD: False,
            }


    name = 'PROVIDE_BACKUP_HOSTS'
    version = 0

    __slots__ = ('chunks_by_size_code',
                 'ack_result_code', 'ack_hosts_to_use')


    def __init__(self, *args, **kwargs):
        super(ProvideBackupHostsMessage, self).__init__(*args, **kwargs)
        self.chunks_by_size_code = {}
        self.ack_result_code = ProvideBackupHostsMessage.ResultCodes \
                                                        .GENERAL_FAILURE
        self.ack_hosts_to_use = {}


    @bzip2
    @json_dumps
    def _get_body(self):
        # H2N
        return {'chunks_by_size_code': self.chunks_by_size_code}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # H2N
        # Fix integer keys
        self.chunks_by_size_code = \
            {int(k): v
                 for k, v in body['chunks_by_size_code'].iteritems()}


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        # N2H
        assert isinstance(self.ack_hosts_to_use, dict)
        assert consists_of(self.ack_hosts_to_use.iterkeys(), UUID)
        assert consists_of(self.ack_hosts_to_use.itervalues(),
                           PerHostChunksData)
        assert isinstance(self.ack_result_code, int), self.ack_result_code

        result = {'result': self.ack_result_code}
        if ProvideBackupHostsMessage.ResultCodes.is_good(self.ack_result_code):
            result['hosts'] = {uuid.hex: per_host.to_json()
                                   for uuid, per_host in self.ack_hosts_to_use
                                                             .iteritems()}
        return result


    @bunzip2
    @json_loads
    def _init_from_body_ack(self, body):
        # N2H
        self.ack_result_code = body['result']
        if ProvideBackupHostsMessage.ResultCodes.is_good(self.ack_result_code):
            self.ack_hosts_to_use = \
                {UUID(host_uuid): PerHostChunksData.from_json(host_data)()
                     for host_uuid, host_data in body['hosts'].iteritems()}

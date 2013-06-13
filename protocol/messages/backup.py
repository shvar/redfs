#!/usr/bin/python
"""BACKUP message implementation.

BACKUP message is sent from the Node to the Host to start the backup procedure.

BACKUP transaction runs on the donor host and performs all actions needed
to send the backup chunks to the acceptor hosts/trusted hosts.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col

from contrib.dbc import contract_epydoc

from common.abstractions import AbstractMessage
from common.typed_uuids import DatasetUUID

from ._common import bzip2, bunzip2, json_dumps, json_loads
from .provide_backup_hosts import ProvideBackupHostsMessage



#
# Classes
#

class BackupMessage(AbstractMessage):
    """BACKUP message."""


    class ResultCodes(AbstractMessage.AbstractResultCodes):
        (GENERAL_FAILURE,
         OK,
         NO_SPACE_ON_CLOUD,
         USER_IS_SUSPENDED) = range(4)


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
        def from_provide_backup_host_result_code(cls, pbh_result_code):
            r"""
            >>> BackupMessage.ResultCodes.from_provide_backup_host_result_code(
            ...     ProvideBackupHostsMessage.ResultCodes.NO_SPACE_ON_CLOUD) \
            ...         == BackupMessage.ResultCodes.NO_SPACE_ON_CLOUD
            True

            @note: currently the conversion is easy, but later
                it may be more complex.
            """
            assert pbh_result_code in ProvideBackupHostsMessage.ResultCodes \
                                                               ._all(), \
                   repr(pbh_result_code)
            return pbh_result_code



    name = 'BACKUP'
    version = 0

    __slots__ = ('dataset_uuid',
                 'ack_result_code')


    def __init__(self, *args, **kwargs):
        r"""Constructor.

        >>> from uuid import UUID
        >>> from common.inhabitants import Host, Node
        >>> from common.typed_uuids import PeerUUID, MessageUUID
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> # Smoke test
        >>> BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupMessage(src=Node(uuid=PeerUUID('11111111-...-8c4ce6a2aed9')),
            dst=Host(uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d')),
            uuid=MessageUUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15'),
            status_code=0)
        """
        super(BackupMessage, self).__init__(*args, **kwargs)

        self.dataset_uuid = None
        self.ack_result_code = None


    def __str__(self):
        opt_dataset_uuid = u'dataset_uuid={!r}'.format(self.dataset_uuid) \
                               if self.dataset_uuid is not None \
                               else ''
        opt_ack_result_code = u'ack_result_code={!r}'.format(
                                      self.ack_result_code) \
                                  if self.ack_result_code is not None \
                                  else ''
        opt_strings = [opt_dataset_uuid, opt_ack_result_code]
        opts = ', '.join(filter(None, opt_strings))

        return u'{super}{opts}'.format(
                   super=super(BackupMessage, self).__str__(),
                   opts=' [ {} ]'.format(opts) if opts else '')


    @bzip2
    @json_dumps
    @contract_epydoc
    def _get_body(self):
        """
        >>> from uuid import UUID
        >>> from common.inhabitants import Host, Node
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> msg0 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> BackupMessage._get_body._without_bzip2._without_json_dumps(msg0)
        Traceback (most recent call last):
          ...
        Exception: dataset_uuid is empty!

        >>> msg1 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f')
        ... )
        >>> msg1.dataset_uuid = UUID('dd43b93a-9ee2-469b-a487-a8a278edf64a')
        >>> msg1.ack_result_code = BackupMessage.ResultCodes.NO_SPACE_ON_CLOUD
        >>> BackupMessage._get_body._without_bzip2._without_json_dumps(msg1)
        {'dataset_uuid': 'dd43b93a9ee2469ba487a8a278edf64a'}
        """
        # N2H

        if self.dataset_uuid is not None:
            # Launch backup on its dataset UUID
            return {'dataset_uuid': self.dataset_uuid.hex}

        else:
            raise Exception('dataset_uuid is empty!')


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        r"""
        >>> from uuid import UUID
        >>> from common.inhabitants import Host, Node
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> msg0 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> BackupMessage._init_from_body._without_bunzip2._without_json_loads(
        ...     msg0, {})
        Traceback (most recent call last):
          ...
        Exception: Unparseable backup message body: {}

        >>> msg1 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f'),
        ...     status_code=1
        ... )
        >>> BackupMessage._init_from_body._without_bunzip2._without_json_loads(
        ...     msg1, {'dataset_uuid': '60fb966767ae4e3c9c86edac180dd4e6'})
        >>> msg1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupMessage(src=Node(uuid=PeerUUID('11111111-...-8c4ce6a2aed9')),
            dst=Host(uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d')),
            uuid=MessageUUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f'),
            status_code=1
            [
              dataset_uuid=DatasetUUID('60fb9667-67ae-4e3c-9c86-edac180dd4e6')
            ])
        """
        # N2H
        if 'dataset_uuid' in body:
            self.dataset_uuid = DatasetUUID(body['dataset_uuid'])

        else:
            raise Exception('Unparseable backup message body: {!r}'
                                .format(body))


    @bzip2
    @json_dumps
    def _get_body_ack(self):
        """
        >>> from uuid import UUID
        >>> from common.inhabitants import Host, Node
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> msg0 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> BackupMessage._get_body_ack._without_bzip2._without_json_dumps(
        ...     msg0)
        Traceback (most recent call last):
          ...
        AssertionError: None

        >>> msg1 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f')
        ... )
        >>> msg1.dataset_uuid = UUID('dd43b93a-9ee2-469b-a487-a8a278edf64a')
        >>> msg1.ack_result_code = BackupMessage.ResultCodes.NO_SPACE_ON_CLOUD
        >>> BackupMessage._get_body_ack._without_bzip2._without_json_dumps(
        ...     msg1)
        {'result': 2}

        @precondition: self.ack_result_code is not None
        """
        # H2N
        assert isinstance(self.ack_result_code, int), \
               repr(self.ack_result_code)
        return {'result': self.ack_result_code}


    @bunzip2
    @json_loads
    @contract_epydoc
    def _init_from_body_ack(self, body):
        r"""
        >>> from uuid import UUID
        >>> from common.inhabitants import Host, Node
        >>> host = Host(uuid=UUID('00000000-7606-420c-8a98-a6c8728ac98d'))
        >>> node = Node(uuid=UUID('11111111-79b4-49e0-b72e-8c4ce6a2aed9'))

        >>> msg0 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('4ac2536a-4a1e-4b08-ad4e-11bd675fdf15')
        ... )
        >>> BackupMessage._init_from_body_ack \
        ...              ._without_bunzip2._without_json_loads(
        ...                   msg0, {})
        Traceback (most recent call last):
          ...
        KeyError: 'result'

        >>> msg1 = BackupMessage(
        ...     src=node,
        ...     dst=host,
        ...     uuid=UUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f')
        ... )
        >>> BackupMessage._init_from_body_ack._without_bunzip2 \
        ...              ._without_json_loads(msg1, {'result': 2})
        >>> msg1  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        BackupMessage(src=Node(uuid=PeerUUID('11111111-...-8c4ce6a2aed9')),
            dst=Host(uuid=PeerUUID('00000000-7606-420c-8a98-a6c8728ac98d')),
            uuid=MessageUUID('baebd0f2-fc58-417b-97ee-08a892cd5a8f'),
            status_code=0
            [ ack_result_code=2 ])

        @type body: col.Mapping
        """
        # N2H
        self.ack_result_code = body['result']

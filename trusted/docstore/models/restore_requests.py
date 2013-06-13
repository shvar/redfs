#!/usr/bin/python
"""
The FastDB model for the request to the Node to restore some specific files.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
from datetime import datetime
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from trusted.ibsonable import IBSONable

from .idocstoredocument import IDocStoreTopDocument



#
# Classes
#

class RestoreRequest(IDocStoreTopDocument, IBSONable):
    """A request to the node to restore several files."""

    __slots__ = (
        'executor', 'base_ds_uuid', 'paths', 'ts_exec',
        # Optional
        'ts_start', 'ds_uuid'
    )

    bson_schema = {
        'executor': basestring,
        'base_ds_uuid': UUID,
        'paths': lambda paths: consists_of(paths, basestring),
        'ts_exec': datetime,
        # optional fields
        'ts_start': (datetime, NoneType),
        'ds_uuid': (UUID, NoneType)
    }


    @contract_epydoc
    def __init__(self,
                 executor, base_ds_uuid, paths, ts_exec,
                 ts_start=None, ds_uuid=None,
                 *args, **kwargs):
        """
        @param executor: the name of the user who executed the restore process.

        @param base_ds_uuid: the UUID of the dataset which is used
            as a baseline for the restore.
        @type base_ds_uuid: UUID

        @type paths: col.Iterable

        @type ts_exec: datetime
        @type ts_start: datetime, NoneType

        @param ds_uuid: the UUID of the newly created dataset (or None).
        @type ds_uuid: UUID, NoneType
        """
        super(RestoreRequest, self).__init__(*args, **kwargs)
        self.executor = executor
        self.base_ds_uuid = base_ds_uuid
        self.paths = list(paths)
        assert self.paths
        self.ts_exec = ts_exec
        self.ts_start = ts_start
        self.ds_uuid = ds_uuid


    def __str__(self):
        return u'{super}, executor={self.executor!r}, ' \
                'base_ds_uuid={self.base_ds_uuid!r}, paths={self.paths!r}, ' \
                'ts_exec={self.ts_exec!r}, ts_start={self.ts_start!r}, ' \
                'ds_uuid={self.ds_uuid!r}'.format(
                    super=super(RestoreRequest, self).__str__(),
                    self=self)


    def to_bson(self):
        r"""
        >>> RestoreRequest(
        ...     executor='alpha',
        ...     base_ds_uuid=UUID(int=42),
        ...     paths=['/dev/null', '/usr/bin'],
        ...     ts_exec=datetime(2012, 1, 2, 3, 4, 5, 6)
        ... ).to_bson() # doctest:+NORMALIZE_WHITESPACE
        {'paths': ['/dev/null', '/usr/bin'],
         'base_ds_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
         'ts_exec': datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
         'executor': 'alpha'}

        >>> import bson
        >>> RestoreRequest(
        ...     executor='alpha',
        ...     base_ds_uuid=UUID(int=42),
        ...     paths=['/dev/null', '/usr/bin'],
        ...     ts_exec=datetime(2012, 1, 2, 3, 4, 5, 6),
        ...     ts_start=datetime(2012, 6, 5, 4, 3, 2, 1),
        ...     ds_uuid=UUID(int=84),
        ...     _id=bson.objectid.ObjectId('400b1e67d18ab62e36000000')
        ... ).to_bson() # doctest:+NORMALIZE_WHITESPACE
        {'paths': ['/dev/null', '/usr/bin'],
         'ds_uuid': UUID('00000000-0000-0000-0000-000000000054'),
         'base_ds_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
         'ts_exec': datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
         'executor': 'alpha',
         'ts_start': datetime.datetime(2012, 6, 5, 4, 3, 2, 1),
         '_id': ObjectId('400b1e67d18ab62e36000000')}
        """
        cls = self.__class__

        doc = super(RestoreRequest, self).to_bson()

        doc.update({
            'executor': self.executor,
            'base_ds_uuid': self.base_ds_uuid,
            'paths': self.paths,
            'ts_exec': self.ts_exec,
        })

        # Optional fields
        if self.ts_start is not None:
            doc.update({'ts_start': self.ts_start})
        if self.ds_uuid is not None:
            doc.update({'ds_uuid': self.ds_uuid})

        assert cls.validate_schema(doc), repr(doc)

        return doc


    @classmethod
    def from_bson(cls, doc):
        r"""
        >>> RestoreRequest.from_bson({
        ...     'paths': ['/dev/null', '/usr/bin'],
        ...     'base_ds_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
        ...     'ts_exec': datetime(2012, 1, 2, 3, 4, 5, 6),
        ...     'executor': 'alpha'}
        ... )()  # doctest:+NORMALIZE_WHITESPACE
        RestoreRequest(_id=None,
            executor='alpha',
            base_ds_uuid=UUID('00000000-0000-0000-0000-00000000002a'),
            paths=['/dev/null', '/usr/bin'],
            ts_exec=datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
            ts_start=None, ds_uuid=None)

        >>> import bson
        >>> RestoreRequest.from_bson({
        ...     'paths': ['/dev/null', '/usr/bin'],
        ...     'ds_uuid': UUID('00000000-0000-0000-0000-000000000054'),
        ...     'base_ds_uuid': UUID('00000000-0000-0000-0000-00000000002a'),
        ...     'ts_exec': datetime(2012, 1, 2, 3, 4, 5, 6),
        ...     'executor': 'alpha',
        ...     'ts_start': datetime(2012, 6, 5, 4, 3, 2, 1),
        ...     '_id': bson.objectid.ObjectId('400b1e67d18ab62e36000000')}
        ... )()  # doctest:+NORMALIZE_WHITESPACE
        RestoreRequest(_id=ObjectId('400b1e67d18ab62e36000000'),
            executor='alpha',
            base_ds_uuid=UUID('00000000-0000-0000-0000-00000000002a'),
            paths=['/dev/null', '/usr/bin'],
            ts_exec=datetime.datetime(2012, 1, 2, 3, 4, 5, 6),
            ts_start=datetime.datetime(2012, 6, 5, 4, 3, 2, 1),
            ds_uuid=UUID('00000000-0000-0000-0000-000000000054'))
        """
        assert cls.validate_schema(doc), repr(doc)

        return partial(super(RestoreRequest, cls).from_bson(doc),
                       executor=doc['executor'],
                       base_ds_uuid=doc['base_ds_uuid'],
                       paths=doc['paths'],
                       ts_exec=doc['ts_exec'],
                         # Optional
                       ts_start=doc.get('ts_start'),
                       ds_uuid=doc.get('ds_uuid'))

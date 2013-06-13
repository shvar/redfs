#!/usr/bin/python
"""
Framework to access the document storage database (like MongoDB)
(actually, for now B{only} MongoDB).

@var DS: A docstore-access wrapper factory.
@type DS: DocStoreWrapperFactory
"""

#
# Imports
#

from __future__ import absolute_import
import logging

from contrib.dbc import contract_epydoc

import bson
import gridfs
import pymongo

from . import fdbqueries, bdbqueries
from .abstract_docstorewrapper import AbstractDocStoreWrapper
from .docstorewrapper import (
    DocStoreWrapper, DocStoreWrapperFactory, MongoDBInitializationException)
from .models._common import IndexOptions




#
# Variables/constants
#

logger = logging.getLogger(__name__)

FDB = None
"""
@type FDB: DocStoreWrapperFactory
"""

BDB = None
"""
@type BDB: DocStoreWrapperFactory
"""


#
# Classes
#

class GFSEnabledDocStoreWrapper(DocStoreWrapper):
    """The GridFS wrapper with gfs_*-specific attributes."""

    __slots__ = ('gfs_chunks', 'gfs_messages', 'gfs_uploaded_files')

    def __init__(self, *args, **kwargs):
        super(GFSEnabledDocStoreWrapper, self).__init__(*args, **kwargs)
        self.gfs_chunks = \
            gridfs.GridFS(self._wrapped_db, 'chunks')
        self.gfs_messages = \
            gridfs.GridFS(self._wrapped_db, 'messages')
        self.gfs_uploaded_files = \
            gridfs.GridFS(self._wrapped_db, 'uploaded_files')



class GFSEnabledDocStoreWrapperFactory(DocStoreWrapperFactory):
    """The wrapper factory that enhances each wrapper with C{gfs_*} attributes.
    """

    __slots__ = ()


    def __call__(self, *args, **kwargs):
        """Overrides implementation from C{DocStoreWrapperFactory}.
        """
        return GFSEnabledDocStoreWrapper(self, self._ds)



#
# Functions
#

@contract_epydoc
def init(fast_db_url, big_db_url):
    """Initialize FDB/BDB factory singletons.

    As a useful side effect, creates all (missing) indices.

    @type fast_db_url: basestring
    @type big_db_url: basestring

    @raises MongoDBInitializationException: if cannot complete connection.
    """
    global FDB, BDB
    assert FDB is None and BDB is None, (FDB, BDB)

    try:
        FDB = DocStoreWrapperFactory(fast_db_url)
        BDB = GFSEnabledDocStoreWrapperFactory(big_db_url)
    except MongoDBInitializationException:
        raise

    if not bson.has_c():
        logger.warning('python-bson-ext is not installed, '
                           'performance may be lower than expected!')
    if not pymongo.has_c():
        logger.warning('python-pymongo-ext is not installed, '
                           'performance may be lower than expected!')

    logger.debug('Creating indices on Fast DataBase')
    with FDB() as fdbw:
        __make_indices(fdbqueries.FDB_INDICES_PER_COLLECTION.iteritems(),
                       dsw=fdbw)
    logger.debug('Creating indices on Big DataBase')
    with BDB() as bdbw:
        __make_indices(bdbqueries.GFS_INDICES_PER_COLLECTION.iteritems(),
                       dsw=bdbw)


def __make_indices(items, dsw):
    """Create indices for some set of DocStore collections.

    @type items: col.Iterable
    @param dsw: some docstore wrapper to either FastDB or BigDB
        (i.e. either fdbw or bdbw).
    @type dsw: AbstractDocStoreWrapper
    """
    for col_name, indices in items:
        for index_fields, index_settings in indices.iteritems():
            assert (isinstance(index_fields, (list, tuple)) and
                    not isinstance(index_fields, basestring)), \
                   repr(index_fields)
            index_tuples = [(f, pymongo.ASCENDING) for f in index_fields]

            kwargs = {}
            if IndexOptions.SPARSE in index_settings:
                kwargs['sparse'] = True
            if IndexOptions.UNIQUE in index_settings:
                kwargs['unique'] = True

            logger.verbose('Creating index on %r for %r',
                           col_name, index_fields)

            dsw[col_name].ensure_index(index_tuples, **kwargs)


@contract_epydoc
def uninit():
    global FDB, BDB
    assert FDB is not None and BDB is not None, (FDB, BDB)
    FDB, BDB = None, None

#!/usr/bin/python
"""The singleton to store the C{DataWrapper} and appropriate factory.

@todo: someday this should be used to simplify various
    C{with db.RDB() as rdbw, ds.FDB() as fdbw, ds.BDB() as bdbw:}
"""

#
# Imports
#

from __future__ import absolute_import
from collections import namedtuple



#
# Constants/global variables
#

DW = None



#
# Classes and types
#

DataWrapper = namedtuple('DataWrapper', ('rdbw', 'fdbw', 'bdbw'))
"""
A tuple of accessors to:
  * RDB (rdbw - Relational DataBase Wrapper),
  * FDB (fdbw - Fast DataBase Wrapper), and
  * BDB (bdbw - Big DataBase Wrapper).
"""



class DataWrapperFactory(object):

    __slots__ = ('__rdbw_factory', '__fdbw_factory', '__bdbw_factory')


    def __init__(self, rdbw_factory, fdbw_factory, bdbw_factory):
        """Constructor.

        @type rdbw_factory: db.DatabaseWrapperFactory
        @type fdbw_factory: ds.DocStoreWrapperFactory
        @type bdbw_factory: ds.DocStoreWrapperFactory
        """
        self.__rdbw_factory = rdbw_factory
        self.__fdbw_factory = fdbw_factory
        self.__bdbw_factory = bdbw_factory

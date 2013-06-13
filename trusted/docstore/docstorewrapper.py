#!/usr/bin/python
"""
Framework to access the document storage database (like MongoDB)
(actually, for now B{only} MongoDB).
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
from time import sleep

from twisted.internet.defer import Deferred

import pymongo

from contrib.dbc import contract_epydoc

from .abstract_docstorewrapper import AbstractDocStoreWrapper



#
# Variables/constants
#

logger = logging.getLogger(__name__)



#
# Classes
#

class DocStoreWrapper(AbstractDocStoreWrapper):
    """
    Wraps the C{pymongo.database.Database} class to provide auto-reconnect
    in case of problems.
    """
    __slots__ = ('__factory', '_wrapped_db')


    def __init__(self, factory, db, *args, **kwargs):
        self.__factory = factory
        self._wrapped_db = db


    @classmethod
    @contract_epydoc
    def retried_sync(cls, cb, *args, **kwargs):
        """Execute a PyMongo callable probably retrying it on AutoReconnect.

        Synchronous function that executes an underlying callable
        and, in case if it raised C{pymongo.errors.AutoReconnect} exception,
        retries it several times.

        The function is synchronous and may block the caller thread
        for up to 32 seconds.

        Currently up to five retries (6 attempts total) are performed,
        with the delays increasing twice with each retry;
        i.e. 1 s, 2 s, 4 s, 8 s, 16 s

        @type cb: col.Callable
        """
        max_tries = 5

        for i in xrange(max_tries + 1):
            try:
                result = cb(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                if i != max_tries:
                    delay = 2 ** i
                    logger.warning('Reconnect error during %r(*%r, **%r), '
                                       'retrying in %i seconds',
                                   cb, args, kwargs, delay)
                    sleep(delay)
                else:
                    break
            else:
                return result

        # No retry succeeded
        raise Exception('All retries failed for {!r}(*{!r}, **{!r}), giving up'
                            .format(cb, args, kwargs))


    @classmethod
    @contract_epydoc
    def retried_async(cls, cb, *args, **kwargs):
        """Execute a PyMongo callable probably retrying it on AutoReconnect.

        Asynchronous function that executes an underlying callable
        and, in case if it raised C{pymongo.errors.AutoReconnect} exception,
        retries it several times.

        The function is asynchronous and last for up to 32 seconds.

        Currently up to five retries (6 attempts total) are performed,
        with the delays increasing twice with each retry;
        i.e. 1 s, 2 s, 4 s, 8 s, 16 s

        @type cb: col.Callable

        @rtype: Deferred
        """
        raise


    def __is_own_attr(self, name):
        classname = DocStoreWrapper.__name__

        if name in DocStoreWrapper.__slots__:
            # Is private and accessed from inside the class.
            # Or maybe it is not private at all.
            return True
        elif (name.startswith('_' + classname) and
              name[len(classname) + 1:] in DocStoreWrapper.__slots__):
            # Is private but accessed from outside the class
            return True
        else:
            return False


    def __getitem__(self, name):
        return self._wrapped_db[name]


    def __getattr__(self, name):
        if self.__is_own_attr(name):
            return getattr(self, name)
        else:
            return getattr(self._wrapped_db, name)


    def __setattr__(self, name, value):
        if self.__is_own_attr(name):
            return object.__setattr__(self, name, value)
        else:
            return setattr(self._wrapped_db, name, value)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is exc_val is exc_tb is None:
            pass
        else:
            logger.exception('Leaving %r: (%r, %r, %r)',
                             self, exc_type, exc_val, exc_tb)



class MongoDBInitializationException(Exception):
    __slots__ = ()



class DocStoreWrapperFactory(object):
    """
    A factory that is capable of creating the docstore wrappers
    as soon as it is initialized.
    """
    __slots__ = ('__connection', '_ds')


    def __init__(self, host, *args, **kwargs):
        """Constructor.

        @param host: the MongoDB-style URL for connection.

        @raises MongoDBInitializationException: if cannot parse the url
            or connect to it for some reason.
        """
        if not host.startswith('mongodb://'):
            raise MongoDBInitializationException(
                      '{!r} must start with mongodb://'.format(host))
        else:
            # Cut out 'mongodb://' - which is 10 symbols long btw
            url_noproto = host[10:]
            slash_idx = url_noproto.rfind('/')
            if slash_idx == -1 or slash_idx == len(url_noproto):
                raise MongoDBInitializationException(
                          '{!r} must contain URL and DB parts'.format(host))
            else:
                url, db = url_noproto[:slash_idx], url_noproto[slash_idx + 1:]
                try:
                    if pymongo.version_tuple >= (2, 2):
                        # auto_start_request is available since 2.2 only.
                        kwargs.update(auto_start_request=True)

                    self.__connection = \
                        pymongo.connection.Connection(url,
                                                      *args,
                                                      max_pool_size=64,
                                                      journal=True,
                                                      **kwargs)

                except Exception as e:
                    raise MongoDBInitializationException(
                              'Cannot connect to MongoDB at {!r}: {!s}'
                                  .format(url, e))
                else:
                    self._ds = self.__connection[db]


    def __call__(self):
        """
        @rtype: DocStoreWrapper
        """
        return DocStoreWrapper(self, self._ds)

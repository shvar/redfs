#!/usr/bin/python
"""
The module containing the configurable cache of the host settings.
"""
import collections as col, logging
from collections import namedtuple
from datetime import datetime, timedelta

from contrib.dbc import contract_epydoc



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class HostSettingsCache(object):
    """
    The cache to store the host settings.

    @ivar __all: whether all settings were ever cached.
    @type __all: bool
    """

    Item = namedtuple('Item', ('value', 'last_update_time'))

    __CacheRecord = namedtuple('__CacheRecord',
                               ('value_item', 'cached_time'))


    __slots__ = ('__setting_names',
                 '__cache_period', '__records', '__all')


    @contract_epydoc
    def __init__(self, setting_names, cache_period):
        """
        @type setting_names: col.Iterable
        @type cache_period: timedelta
        """
        self.__setting_names = frozenset(setting_names)
        self.__cache_period = cache_period
        self.__records = {}
        self.__all = False


    @contract_epydoc
    def __setitem__(self, name, value):
        """
        Given a setting name, update the cached value of the setting.

        @param name: the name of the setting.

        @param value: the value of the setting (together with its
                      last update time).
        @type value: HostSettingsCache.Item
        """
        assert name in self.__setting_names, (name, self.__setting_names)
        cls = self.__class__

        cached_time = datetime.utcnow()

        self.__records[name] = cls.__CacheRecord(value, cached_time)
        logger.debug('Cache set: %r=%r', name, value)


    @contract_epydoc
    def __getitem__(self, name):
        """
        Given a setting name, get the cached value of the setting.

        @rtype: HostSettingsCache.Item

        @raises KeyError: if the setting value is not in cache (because either
                          it was not ever set or it has been expired).
        """
        assert name in self.__setting_names, (name, self.__setting_names)
        try:
            record = self.__records[name]
        except KeyError:
            # Reason 1 of 2 why the setting may be missing
            logger.debug('Cache miss: %r', name)
            raise

        now = datetime.utcnow()
        if now - record.cached_time > self.__cache_period:
            # Reason 2 of 2 why the setting may be missing
            logger.debug('Cache expired: %r', name)
            raise KeyError(name)
        else:
            logger.debug('Cache hit: %r = %r', name, record.value_item)
            return record.value_item


    @contract_epydoc
    def get_all(self):
        """
        Get all cached settings, but only if they are both present and none of
        them is expired.

        @returns: a dictionary mapping the setting names
                  to the C{HostSettingsCache.Item} objects.
        @rtype: col.Mapping

        @raises KeyError: if at least one of the settings cannot be get
                          from the cache safely.
        """
        # If some setting cache time is older than oldest_time,
        # it is considered expired.
        oldest_time = datetime.utcnow() - self.__cache_period

        if not self.__all:
            # It is likely that all settings were not read yet.
            raise KeyError('Not all settings are probably cached.')
        else:
            # If the settings have ever been read, the records were
            # initialized. They may be expired, but the records even for the
            # expired records are stored.

            # Creating a separate dictionary for the result.
            result = {}
            for name, record in self.__records.iteritems():
                if record.cached_time < oldest_time:
                    raise KeyError('{!r} expired'.format(name))
                else:
                    result[name] = record.value_item
            return result


    def set_all(self, values_map):
        for name, item in values_map.iteritems():
            self[name] = item

        self.__all = True

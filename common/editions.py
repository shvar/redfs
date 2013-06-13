#!/usr/bin/python
"""
The edition-specific features of the system, common for all components.
"""

#
# Imports
#

from __future__ import absolute_import

from .abstractions import IPrintable


class FeatureSet(IPrintable):

    __slots__ = ('codename', 'short_name', 'full_name', 'per_group_encryption',
                 'web_initiated_restore', 'p2p_storage')


    def __init__(self, codename, short_name, full_name, per_group_encryption,
                       web_initiated_restore, p2p_storage):
        self.codename = codename
        self.short_name = short_name
        self.full_name = full_name
        self.per_group_encryption = per_group_encryption
        self.web_initiated_restore = web_initiated_restore
        self.p2p_storage = p2p_storage


    def __str__(self):
        return (u'codename={self.codename!r}, '
                 'short_name={self.short_name!r}, '
                 'full_name={self.full_name!r}, '
                 'per_group_encryption={self.per_group_encryption!r}, '
                 'web_initiated_restore={self.web_initiated_restore!r}, '
                 'p2p_storage={self.p2p_storage!r}'
                     .format(self=self))


    @property
    def _init_kwargs(self):
        return {
            'codename': self.codename,
            'short_name': self.short_name,
            'full_name': self.full_name,
            'per_group_encryption': self.per_group_encryption,
            'web_initiated_restore': self.web_initiated_restore,
            'p2p_storage': self.p2p_storage,
        }



EDITIONS = {
    'community':
        FeatureSet(
            codename='community',
            short_name='FreeBrie',
            full_name='FreeBrie Community Edition',
            per_group_encryption=True,
            web_initiated_restore=False,
            p2p_storage=True),
    'personal':
        FeatureSet(
            codename='personal',
            short_name='KeepTheCopy',
            full_name='KeepTheCopy',
            per_group_encryption=False,
            web_initiated_restore=False,
            p2p_storage=False),
    'superfolder':
        FeatureSet(
            codename='superfolder',
            short_name='SuperFolder',
            full_name='SuperFolder',
            per_group_encryption=False,
            web_initiated_restore=False,
            p2p_storage=False),
}

# The key should match the codename for now;
assert (k == fs.codename for k, fs in EDITIONS.iteritems()), repr(EDITIONS)
# the short name must be unique;
assert len(EDITIONS.keys()) == \
           len({fs.short_name for fs in EDITIONS.itervalues()}), \
       repr(EDITIONS)
# and the lowercased short name must be unique too.
assert len(EDITIONS.keys()) == \
           len({fs.short_name.lower() for fs in EDITIONS.itervalues()}), \
       repr(EDITIONS)

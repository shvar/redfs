#!/usr/bin/python
"""
Abstract interface for the classes containing the whole dataset.
"""

# Imports
from __future__ import absolute_import
import numbers
from abc import ABCMeta, abstractmethod
from datetime import datetime
from functools import partial
from types import NoneType

from contrib.dbc import contract_epydoc

from .abstractions import IJSONable, IPrintable
from .itertools_ex import ilen
from .typed_uuids import DatasetUUID, UserGroupUUID
from .utils import strftime, strptime



#
# Classes
#

class AbstractDataset(IPrintable, IJSONable):
    """
    The data set for a single backup; contains the base directories
    and per-directory file mappings.

    @ivar name: Dataset name.
    @type name: basestring

    @ivar uuid: UUID for this dataset.
    @type uuid: DatasetUUID

    @ivar ugroup_uuid: user group UUID for this dataset.
    @type ugroup_uuid: UserGroupUUID

    @ivar sync: a flag to display whether the dataset is sync-created.
    @type sync: bool

    @ivar directories: the mapping from the directory path (absolute)
        to the tuple containing two items,
            1. the list of the small files, and
            2. the list of the large files.
        Example:
        {u'/home/honeyman/.sync-123': (
            [LocalPhysicalFileState(
                 "...rd_shortcuts.pdf"
                 in /home/honeyman/.sync-123,
                 000000...616810: 254137 bytes,
                 time_changed=datetime.datetime(2012, 11, 5,12, 12, 42)),
             LocalPhysicalFileState(
                 "all.md5"
                 in /home/honeyman/.sync-123,
                 000000...544bc4: 458 bytes,
                 time_changed=datetime.datetime(2012, 9, 19, 16, 4, 49)),
            [LocalPhysicalFileState(
                 "...xi_album_mix.mp3"
                 in /home/honeyman/.sync-123, 000000...503561:
                 13829879 bytes,
                 time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41)),
             LocalPhysicalFileState(
                 "...low_screen-2.mp3"
                 in /home/honeyman/.sync-123,
                 000000...feff1c:
                 12232704 bytes,
                 time_changed=datetime.datetime(2012, 11, 5, 12, 12, 41))])}

    @type directories: dict
    """
    __slots__ = ('name', 'uuid', 'sync', 'ugroup_uuid',
                 'time_started', 'time_completed')

    __metaclass__ = ABCMeta


    @contract_epydoc
    def __init__(self, name, sync, uuid, ugroup_uuid,
                 time_started=None, time_completed=None):
        """Constructor.

        @param name: The dataset name.
        @type name: basestring

        @param sync: whether the dataset is sync-related.
        @type sync: bool

        @param uuid: dataset UUID.
        @type uuid: DatasetUUID

        @param ugroup_uuid: The UUID of the User Group.
        @type ugroup_uuid: UserGroupUUID

        @type time_started: datetime, NoneType
        @type time_completed: datetime, NoneType
        """
        self.name = name
        self.sync = sync

        self.uuid = uuid
        self.ugroup_uuid = ugroup_uuid

        self.time_started = time_started if time_started is not None \
                                         else datetime.utcnow()

        self.time_completed = time_completed


    def __str__(self):
        return u'name={self.name!r}, sync={self.sync!r}, uuid={self.uuid!r}, '\
               u'ugroup_uuid={self.ugroup_uuid!r}, '\
               u'time_started={self.time_started!r}'\
               u'{opt_time_completed}'.format(
                   self=self,
                   opt_time_completed='' if self.time_completed is None
                                         else u', time_completed={!r}'.format(
                                                  self.time_completed))


    @property
    def completed(self):
        """
        @return: Whether the dataset upload was completed.
        @rtype: bool
        """
        return self.time_completed is not None


    def to_json(self):
        """
        @precondition: isinstance(self.uuid, DatasetUUID)
        @rtype: dict
        """
        res = super(AbstractDataset, self).to_json()
        res.update({
            'name': self.name,
            'uuid': self.uuid.hex,
            'ugroup_uuid': self.ugroup_uuid.hex,
            'sync': 1 if self.sync else 0,
            'time_started': strftime(self.time_started)
        })
        if self.time_completed is not None:
            res['time_completed'] = strftime(self.time_completed)

        return res


    @classmethod
    def from_json(cls, data):
        """
        For directories, the dataset puts all the files
        to the list of small files only.
        """
        return partial(
            super(AbstractDataset, cls).from_json(data),
            name=data['name'],
            sync=bool(data['sync']),
            uuid=DatasetUUID(data['uuid']),
            ugroup_uuid=UserGroupUUID(data['ugroup_uuid']),
            time_started=strptime(data['time_started']),
            time_completed=None if 'time_completed' not in data
                                else strptime(data['time_completed'])
        )


    def files_count(self):
        """
        @todo: Test it and remove this todo line.

        @returns: The number of all the files in the dataset.
        @rtype: numbers.Integral
        """
        return sum(len(small_files) + len(large_files)
                       for (small_files, large_files)
                           in self.directories.itervalues())



class AbstractBasicDatasetInfo(AbstractDataset):
    """
    An abstract dataset structure which contains just the general information
    about the dataset.
    """
    pass



class AbstractDatasetWithChunks(AbstractDataset):

    __metaclass__ = ABCMeta


    @abstractmethod
    def chunks(self):
        """
        @returns: The iterator over the chunks in this dataset.
        @rtype: iterator
        """
        pass


    def chunks_count(self):
        """
        @returns: The number of all the chunks in the dataset.
        @rtype: numbers.Integral
        """
        return ilen(self.chunks())


    def size(self):
        """
        @return: The total size of all the chunks.
        @rtype: numbers.Integral
        """
        return sum(c.size() for c in self.chunks())

#!/usr/bin/python
"""
The abstract interface that every notify manager MUST support.
"""

#
# Imports
#

from __future__ import absolute_import
from abc import ABCMeta, abstractmethod



#
# Classes
#

class AbstractFSNotifyManager(object):
    """
    The abstract class (or, even more correctly, the interface, but the name
    like INotifyManager would be misleading here) that must be supported by
    every FSnotify manager.

    FSnotify manager can produce events about chenges on fs, and this events
    can be produced with slight delay after trigger event occured.
    """

    __metaclass__ = ABCMeta

    __slots__ = ()


    @abstractmethod
    def watch(self, path, cb):
        """
        This method must contain the implementation of notifymanager-specific
        adding of a directory to watchlist.

        @param path: Path to file or directory to watch.

        @param cb: Callback to handle events from FSNotifyManager.
        """
        pass



def __on_fs_event_received(event):
    """This is a sample of callback function to be passed to C{watch()}.

    @param event: the FS event that has occured.
    @type event: fs_notify.events.Event
    """
    pass

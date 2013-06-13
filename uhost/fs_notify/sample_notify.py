#!/usr/bin/python
"""A dummy FS notification watcher with the minimal interface."""

#
# Imports
#

from __future__ import absolute_import

from .abstract_fsnotify_manager import AbstractFSNotifyManager



#
# Class
#

class DummyFSNotifyManager(AbstractFSNotifyManager):

    def watch(self, path, cb):
        """Add a directory to the watch list.

        @param path: the path to the directory to watch.
        @type path: basestring

        @param cb: a callback to call when an FS even in a directory C{path}
            happens.
        @type cb: col.Callable
        """
        pass

#!/usr/bin/python
"""
The interface for a typical DocStore (FastDB/BigDB) wrapper.
"""

#
# Imports
#

from __future__ import absolute_import

from abc import ABCMeta, abstractmethod



#
# Classes
#

class AbstractDocStoreWrapper(object):
    """The interface for a typical DocStore (FastDB/BigDB) wrapper."""

    __metaclass__ = ABCMeta

    @abstractmethod  # must become @abstractclassmethod
    def retried_sync(cls, cb, *args, **kwargs):
        pass

    @abstractmethod  # must become @abstractclassmethod
    def retried_async(cls, cb, *args, **kwargs):
        pass

    @abstractmethod
    def __getattr__(self, name):
        pass

    @abstractmethod
    def __setattr__(self, name, value):
        pass

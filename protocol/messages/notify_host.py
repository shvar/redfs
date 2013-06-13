#!/usr/bin/python
"""NOTIFY_HOST message implementation.

NOTIFY_HOST message is unused at the moment. Generally, it should send some
information to the Host.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import AbstractMessage



#
# Classes
#

class NotifyHostMessage(AbstractMessage):
    """NOTIFY_HOST message."""

    name = 'NOTIFY_HOST'
    version = 0

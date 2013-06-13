#!/usr/bin/python
"""READY_FOR_CHUNKS message implementation.

Not used at the moment.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import AbstractMessage



#
# Classes
#

class ReadyForChunksMessage(AbstractMessage):
    """READY_FOR_CHUNKS message."""

    name = 'READY_FOR_CHUNKS'
    version = 0

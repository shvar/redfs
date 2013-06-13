#!/usr/bin/python
"""LOGOUT message implementation.

LOGOUT message logs a person/host out from the system and marks its host
as not available.
"""

#
# Imports
#

from __future__ import absolute_import

from common.abstractions import AbstractMessage



#
# Classes
#

class LogoutMessage(AbstractMessage):
    """LOGOUT message.

    @cvar priority: Overrides the C{priority} field from AbstractMessage,
                    meaning the the logout message are very important
                    and should take precedence over most of the other messages
                    (but login messages are still a bit more important).
    """

    name = 'LOGOUT'
    version = 0

    priority = 85

    __slots__ = ()


    def __init__(self, *args, **kwargs):
        super(LogoutMessage, self).__init__(*args, **kwargs)
        self.revive = False

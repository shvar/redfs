#!/usr/bin/python
"""UPDATE_CONFIGURATION message implementation.

UPDATE_CONFIGURATION message contains a new settings which should be applied
on the Host.
"""

#
# Imports
#

from __future__ import absolute_import
import base64
import logging
import collections as col

from common.abstractions import AbstractMessage
from common.utils import strftime, strptime

from ._common import bzip2, bunzip2, json_dumps, json_loads



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Classes
#

class UpdateConfigurationMessage(AbstractMessage):
    """UPDATE_CONFIGURATION message."""

    name = 'UPDATE_CONFIGURATION'
    version = 0

    __slots__ = ('settings', )


    def __init__(self, *args, **kwargs):
        super(UpdateConfigurationMessage, self).__init__(*args, **kwargs)
        self.settings = {}


    @bzip2
    @json_dumps
    def _get_body(self):
        # N2H
        assert isinstance(self.settings, col.Mapping), repr(self.settings)

        logger.debug('Sending updated settings to the host %s: %r',
                     self.dst.uuid, self.settings)

        # The value may be binary, so use base64 encoding
        return {'settings': {name: (base64.b64encode(value), strftime(time))
                                 for name, (value, time)
                                     in self.settings.iteritems()}}


    @bunzip2
    @json_loads
    def _init_from_body(self, body):
        # N2H
        assert isinstance(body, dict), repr(body)
        self.settings = {name: (base64.b64decode(value), strptime(time))
                             for name, (value, time)
                                 in body['settings'].iteritems()}

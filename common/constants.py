#!/usr/bin/python
"""Various constants used throughout the system.

This module is designed to be as high in the subordination pyramid as possible,
thus the types defined here should NOT contain any significant
implementation dependent on the other implementation modules
(except probably the serializing/deserializing code);
instead, any other modules will import this module.
"""

#
# Imports
#

from __future__ import absolute_import

from .utils import strptime



#
# Constants
#

CURRENT_DB_VERSION = strptime('2013-04-05 18:10:00.000000')

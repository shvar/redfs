#!/usr/bin/python
"""
Common authentication-related code.
"""

#
# Imports
#

from __future__ import absolute_import
from collections import namedtuple



#
# Classes
#

AuthAvatar = namedtuple('AuthAvatar',
                        ('username', 'host_uuid_candidates', 'do_create'))

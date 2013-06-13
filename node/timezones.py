#!/usr/bin/python
"""
Timezone-processing code.
"""

#
# Imports
#

import logging
from datetime import datetime

import pytz



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Variables/constants
#

UTC = pytz.utc



#
# Functions
#

def is_naive_dt(dt):
    """
    Whether the datetime is naive.

    @rtype: bool

    >>> is_naive_dt(datetime.utcnow())
    True

    >>> is_naive_dt(UTC.localize(datetime.utcnow()))
    False
    """
    assert isinstance(dt, datetime), repr(dt)
    return dt.tzinfo is None


def utcnow():
    """
    @return: The utcnow()-like time, but with proper timezone.
    @rtype: datetime

    >>> is_naive_dt(utcnow())
    False
    """
    return UTC.localize(datetime.utcnow())

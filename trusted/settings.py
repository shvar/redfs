#!/usr/bin/python
"""Settings common for all Trusted components."""

#
# Imports
#

from __future__ import absolute_import
import os

from common import version
from common.utils import coalesce



#
# Functions
#

def get_log_directory(__debug_override=None):
    r"""
    >>> # Mostly smoke tests
    >>> get_log_directory(__debug_override=True)
    u'.'
    >>> get_log_directory(__debug_override=False)
    u'/var/log/freebrie'

    @return: the directory where the logs should be stored.
    @rtype: basestring
    """
    debug = coalesce(__debug_override, __debug__)

    if debug:
        return u'.'
    else:
        return os.path.join(u'/var/log', version.project_name)

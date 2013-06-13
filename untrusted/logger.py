#!/usr/bin/python
"""Untrusted-side-specific logging utilities."""

#
# Imports
#

from __future__ import absolute_import
import logging



#
# Classes
#

class UXEventsOnlyFilter(logging.Filter):
    """
    This filter accepts only the records which name starts
    with the word "ux.".
    """

    def filter(self, record):
        return record.name.startswith('ux.')


class IgnoreUXEventFilter(logging.Filter):
    """
    This filter ignores all the records which name starts
    with the word "ux.".
    """

    def filter(self, record):
        return not record.name.startswith('ux.')


class StatusEventsOnlyFilter(logging.Filter):
    """
    This filter accepts only the records which name starts
    with the word "status.".
    """

    def filter(self, record):
        return record.name.startswith('status.')


class IgnoreStatusEventFilter(logging.Filter):
    """
    This filter ignores all the records which name starts
    with the word "status.", they are the ones used for the host feedback
    to the GUI.
    """

    def filter(self, record):
        return not record.name.startswith('status.')


class UXAndStatusEventsOnlyFilter(logging.Filter):
    """
    This filter accepts only the records which name starts
    with the word "status." or "ux.".
    """

    def filter(self, record):
        return record.name.startswith('ux.') \
               or record.name.startswith('status.')

#!/usr/bin/python
"""Backup system version.

@var version_info: Version tuple, in a format similar to sys.version_info
@type version_info: tuple

@var version: Version string, in a format similar to sys.version
@type version: str

@var project_internal_codename: the project code name used internally and never
    expected to change. Used and verified in various protocol fields,
    for SSL keys, etc.
@type project_internal_codename: str

@var project_name: the short project name, used in the filenames, etc.
@type project_name: str

@var version_string_template: A template string (containing
    a single %s placeholder for the inhabitant type, be that a host, a node,
    or a server) to describe the current inhabitant.
@type version_string_template: str
"""

from __future__ import absolute_import

version_info = (0, 2, 7, 'beta', 1)

version = '{major}.{minor}.{micro}{opt_level}' \
              .format(major=version_info[0],
                      minor=version_info[1],
                      micro=version_info[2],
                      opt_level=' ' + version_info[3]
                          if version_info[3] != 'final'
                          else '')

project_internal_codename = 'Calathi'
project_name = 'freebrie'

project_title = 'FreeBrie'

version_string_template = project_title + ' {} ' + version

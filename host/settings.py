#!/usr/bin/python
"""
Working with host-specific settings.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import json
import logging
import numbers
import operator
import os
import re
import sys
import traceback
from datetime import datetime
from functools import partial
from itertools import ifilter, imap, chain
from types import GeneratorType, NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from common import (version, logger as common_logger,
                    settings as common_settings, path_ex)
from common.datatypes import AutoUpdateSettings, LogReportingSettings
from common.db import Queries
from common.inhabitants import Host, Node, UserGroup
from common.itertools_ex import ilen
from common.path_ex import decode_posix_path, encode_posix_path
from common.typed_uuids import UserGroupUUID
from common.utils import coalesce, NULL_UUID, duration_logged

from . import db, editions as host_editions
from .db import HostQueries
from .migration import HostMigrationPlan
from .system import get_if_addresses, get_olson_tz_name



#
# Module variables/constants and logging
#

logger = logging.getLogger(__name__)

try:
    from host_magic import SQLALCHEMY_ECHO
except ImportError:
    SQLALCHEMY_ECHO = False

HOST_VERSION_STRING = version.version_string_template.format('Host')

LOG_ROTATE_MAX_BYTES = None
LOG_ROTATE_MAX_BACKUPS = None

TEST_ALL_GROUPS_SAME_SYNC_DIR = False
"""Whether every group-specific contents get synced to the same directory.

@note: setting to C{True} is dangerous even while testing.

@todo: ticket:246 - as soon as multiple groups are supported, this will become
    outdated!
"""

MY_UUID = None  # DO NOT USE ANYMORE!
"""
This is a global variable marking what is the UUID of the current host;
but this is a legacy code, and should be changed to local variables/attributes
whenever possible.
Do not use it anymore. Use the C{host_app.host.uuid} if needed
(but pay attention for C{None}s).
"""


# The implementation in actual CLI/GUI module should initialize it
# to the real value.
# This should be a template that accepts the single named argument, "edition".
_CONF_FILENAME_TEMPLATE = None

# The implementation in actual CLI/GUI module should initialize it
# to the function that gets the edition.
get_edition = None



#
# Classes
#

class NoHostDbException(Exception):
    pass



class DBInitializationIssue(Exception):
    __slots__ = ('base_exc', 'msg')


    def __init__(self, base_exc, msg):
        self.base_exc = base_exc
        self.msg = msg



#
# Functions
#

# * General host settings *

def _get_dir_formatting_args(feature_set):
    r"""For a feature set, get all the arguments used in formatting
        the directories.

    >>> host_editions.EDITIONS['superfolder'].dflt_user_conf_dir_tmpl.format(
    ...     **_get_dir_formatting_args(host_editions.EDITIONS['superfolder']))
    u'~/.superfolder'

    >>> host_editions.EDITIONS['superfolder'].dflt_group_sync_dir_tmpl.format(
    ...     group_name='Group 1',
    ...     **_get_dir_formatting_args(host_editions.EDITIONS['superfolder']))
    u'~/SuperFolder/Group 1'

    >>> _get_dir_formatting_args(host_editions.EDITIONS['community'])
    {'short_name_lc': 'freebrie', 'short_name': 'FreeBrie'}
    >>> _get_dir_formatting_args(host_editions.EDITIONS['personal'])
    {'short_name_lc': 'keepthecopy', 'short_name': 'KeepTheCopy'}

    @type feature_set: host_editions.FeatureSet
    @rtype: dict
    """
    return {'short_name': feature_set.short_name,
            'short_name_lc': feature_set.short_name.lower()}


def get_user_config_dir(__debug_override=None,
                        __sys_platform_override=None,
                        __edition_override=None):
    r"""
    For a given host UUID, return the appropriate configuration directory path.

    >>> home = os.path.expanduser('~')

    >>> # Debug mode
    >>> get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='community') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='personal') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='superfolder') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='community') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='personal') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='superfolder') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='community') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='personal') == \
    ... get_user_config_dir(__debug_override=True,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='superfolder')
    ... u'.'
    True

    >>> # Release mode
    >>> get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='community') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='community') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='community') == \
    ...     '{}/.freebrie'.format(home)
    True
    >>> get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='personal') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='personal') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='personal') == \
    ...     '{}/.keepthecopy'.format(home)
    True
    >>> get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='superfolder') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='linux2',
    ...                     __edition_override='superfolder') == \
    ... get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='win32',
    ...                     __edition_override='superfolder') == \
    ...     '{}/.superfolder'.format(home)
    True

    >>> # Unsupported edition
    >>> get_user_config_dir(__debug_override=False,
    ...                     __sys_platform_override='darwin',
    ...                     __edition_override='asdfg')
    Traceback (most recent call last):
      ...
    ValueError: Unknown edition 'asdfg'

    @raises ValueError: when the passed edition is not supported.

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    debug = coalesce(__debug_override, __debug__)
    sys_platform = coalesce(__sys_platform_override, sys.platform)

    assert __edition_override is not None or callable(get_edition), \
           'Import thost.settings or uhost.settings before host.settings!'

    # Coalesce doesn't cut here, we shouldn't even launch get_edition()
    # in some cases.
    edition = get_edition() if __edition_override is None \
                            else __edition_override

    try:
        host_feature_set = host_editions.EDITIONS[edition]
    except KeyError:
        raise ValueError('Unknown edition {!r}'.format(edition))
    else:  # such edition is indeed supported
        if not debug:
            return path_ex.expanduser(
                       host_feature_set.dflt_user_conf_dir_tmpl.format(
                           **_get_dir_formatting_args(host_feature_set)))
        else:
            return u'.'


@contract_epydoc
def _get_db_file_path(uuid,
                      __debug_override=None,
                      __sys_platform_override=None,
                      __edition_override=None):
    r"""For a given host UUID, return the appropriate database file path.

    >>> home = os.path.expanduser('~')

    >>> _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='darwin',
    ...                   __edition_override='community') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='darwin',
    ...                   __edition_override='personal') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='linux2',
    ...                   __edition_override='community') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='linux2',
    ...                   __edition_override='personal') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='win32',
    ...                   __edition_override='community') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=True,
    ...                   __sys_platform_override='win32',
    ...                   __edition_override='personal') == \
    ...     u'./host-00000000-1111-0000-0000-000000000001.db'
    True

    >>> _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='darwin',
    ...                   __edition_override='community') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='linux2',
    ...                   __edition_override='community') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='win32',
    ...                   __edition_override='community') == \
    ...     '{}/.freebrie/host-00000000-1111-0000-0000-000000000001.db' \
    ...         .format(home)
    True

    >>> _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='darwin',
    ...                   __edition_override='personal') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='linux2',
    ...                   __edition_override='personal') == \
    ... _get_db_file_path(UUID('00000000-1111-0000-0000-000000000001'),
    ...                   __debug_override=False,
    ...                   __sys_platform_override='win32',
    ...                   __edition_override='personal') == \
    ...     '{}/.keepthecopy/host-00000000-1111-0000-0000-000000000001.db' \
    ...         .format(home)
    True

    @type uuid: UUID

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    return os.path.join(get_user_config_dir(
                            __debug_override=__debug_override,
                            __sys_platform_override=__sys_platform_override,
                            __edition_override=__edition_override),
                        'host-{}.db'.format(uuid))


def get_db_url(uuid,
               __debug_override=None,
               __sys_platform_override=None,
               __edition_override=None):
    r"""Given a host UUID, return the SQLAlchemy-compatible URL.

    >>> home = os.path.expanduser('~')
    >>> uuid = UUID('00000000-1111-0000-0000-000000000001')

    >>> # Debug mode
    >>> get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='darwin',
    ...                 __edition_override='community') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='darwin',
    ...                 __edition_override='personal') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='linux2',
    ...                 __edition_override='community') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='linux2',
    ...                 __edition_override='personal') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='win32',
    ...                 __edition_override='community') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=True,
    ...                 __sys_platform_override='win32',
    ...                 __edition_override='personal') == \
    ... 'sqlite:///./host-00000000-1111-0000-0000-000000000001.db'
    True

    >>> # Release mode
    >>> get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='darwin',
    ...                 __edition_override='community') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='linux2',
    ...                 __edition_override='community') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='win32',
    ...                 __edition_override='community') == \
    ...     'sqlite:///{}/.freebrie/host-{}.db'.format(home, uuid)
    True
    >>> get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='darwin',
    ...                 __edition_override='personal') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='linux2',
    ...                 __edition_override='personal') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='win32',
    ...                 __edition_override='personal') == \
    ...     'sqlite:///{}/.keepthecopy/host-{}.db'.format(home, uuid)
    True
    >>> get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='darwin',
    ...                 __edition_override='superfolder') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='linux2',
    ...                 __edition_override='superfolder') == \
    ... get_db_url(UUID('00000000-1111-0000-0000-000000000001'),
    ...                 __debug_override=False,
    ...                 __sys_platform_override='win32',
    ...                 __edition_override='superfolder') == \
    ...     'sqlite:///{}/.superfolder/host-{}.db'.format(home, uuid)
    True

    @type uuid: UUID
    @rtype: basestring
    """
    db_file_path = _get_db_file_path(
                       uuid,
                       __debug_override=__debug_override,
                       __sys_platform_override=__sys_platform_override,
                       __edition_override=__edition_override)
    return u'sqlite:///{}'.format(db_file_path)


@contract_epydoc
def _get_default_chunk_dir(host_uuid,
                           __debug_override=None,
                           __sys_platform_override=None,
                           __edition_override=None):
    r"""
    For a given host UUID, returh the appropriate default path to the directory
    used to store the chunk files.

    >>> home = os.path.expanduser('~')

    >>> _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='darwin',
    ...                        __edition_override='community') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='darwin',
    ...                        __edition_override='personal') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='linux2',
    ...                        __edition_override='community') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='linux2',
    ...                        __edition_override='personal') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='win32',
    ...                        __edition_override='community') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=True,
    ...                        __sys_platform_override='win32',
    ...                        __edition_override='personal') == \
    ...     './.host-00000000-1111-0000-0000-000000000001'
    True

    >>> _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='darwin',
    ...                        __edition_override='community') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='linux2',
    ...                        __edition_override='community') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='win32',
    ...                        __edition_override='community') == \
    ...     '{}/.freebrie/chunks'.format(home)
    True

    >>> _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='darwin',
    ...                        __edition_override='personal') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='linux2',
    ...                        __edition_override='personal') == \
    ... _get_default_chunk_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                        __debug_override=False,
    ...                        __sys_platform_override='win32',
    ...                        __edition_override='personal') == \
    ...     '{}/.keepthecopy/chunks'.format(home)
    True

    @type host_uuid: UUID

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    debug = coalesce(__debug_override, __debug__)
    sys_platform = coalesce(__sys_platform_override, sys.platform)

    return os.path.join(get_user_config_dir(
                            __debug_override=__debug_override,
                            __sys_platform_override=__sys_platform_override,
                            __edition_override=__edition_override),
                        'chunks' if not debug
                                 else '.host-{}'.format(host_uuid))


@contract_epydoc
def _get_default_sync_dir_setting_template(__debug_override=None,
                                           __sys_platform_override=None,
                                           __edition_override=None):
    r"""
    Get the template for the appropriate default path to the directory
    used for syncing.

    The '~' meaning the home dir is not expanded.

    The template may have a single '{}' substitution to contain the host UUID.

    >>> _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='darwin',
    ...     __edition_override='community') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='linux2',
    ...     __edition_override='community') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='win32',
    ...     __edition_override='community') == \
    ... u'./.sync-{}'
    True

    >>> _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='darwin',
    ...     __edition_override='personal') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='linux2',
    ...     __edition_override='personal') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=True,
    ...     __sys_platform_override='win32',
    ...     __edition_override='personal') == \
    ... u'./.sync-{}'
    True

    >>> _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='darwin',
    ...     __edition_override='community') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='linux2',
    ...     __edition_override='community') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='win32',
    ...     __edition_override='community') == \
    ... u'~/FreeBrie/My files'
    True

    >>> _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='darwin',
    ...     __edition_override='personal') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='linux2',
    ...     __edition_override='personal') == \
    ... _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='win32',
    ...     __edition_override='personal') == \
    ... u'~/KeepTheCopy'
    True

    >>> _get_default_sync_dir_setting_template(
    ...     __debug_override=False,
    ...     __sys_platform_override='darwin',
    ...     __edition_override='asdfg')
    Traceback (most recent call last):
      ...
    ValueError: Unknown edition 'asdfg'

    @raises ValueError: when the passed edition is not supported.

    @rtype: basestring
    """
    debug = coalesce(__debug_override, __debug__)
    sys_platform = coalesce(__sys_platform_override, sys.platform)

    assert __edition_override is not None or callable(get_edition), \
           'Import thost.setttings or uhost.settings before host.settings!'
    # Coalesce doesn't cut here, we shouldn't even launch get_edition()
    # in some cases.
    edition = get_edition() if __edition_override is None \
                            else __edition_override

    try:
        host_feature_set = host_editions.EDITIONS[edition]
    except KeyError:
        raise ValueError('Unknown edition {!r}'.format(edition))
    else:  # such edition is indeed supported
        if not debug:
            return host_feature_set.dflt_user_sync_dir_tmpl.format(
                       **_get_dir_formatting_args(host_feature_set))
        else:
            # debug mode; or on Linux (in any mode) - may need to be fixed
            # closer to the release.
            return os.path.join(
                       get_user_config_dir(
                           __debug_override=__debug_override,
                           __sys_platform_override=__sys_platform_override,
                           __edition_override=__edition_override),
                       '.sync-{}'
                           if debug
                           else host_feature_set.short_name)


@contract_epydoc
def get_default_sync_dir(host_uuid,
                         __debug_override=None,
                         __sys_platform_override=None,
                         __edition_override=None):
    r"""
    For a given host UUID, get the appropriate default path to the directory
    used for syncing.

    >>> home = os.path.expanduser('~')

    >>> # Debug mode; any platform, any edition
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='community') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='community') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='community') == \
    ... u'./.sync-00000000-1111-0000-0000-000000000001'
    True
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='personal') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='personal') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='personal') == \
    ... u'./.sync-00000000-1111-0000-0000-000000000001'
    True
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='superfolder') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='superfolder') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=True,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='superfolder') == \
    ... u'./.sync-00000000-1111-0000-0000-000000000001'
    True

    >>> # Release mode
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='community') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='community') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='community') == \
    ...     '{}/FreeBrie/My files'.format(home)
    True
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='personal') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='personal') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='personal') == \
    ...     '{}/KeepTheCopy'.format(home)
    True
    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='superfolder') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='linux2',
    ...                      __edition_override='superfolder') == \
    ... get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='win32',
    ...                      __edition_override='superfolder') == \
    ...     '{}/SuperFolder'.format(home)
    True

    >>> get_default_sync_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                      __debug_override=False,
    ...                      __sys_platform_override='darwin',
    ...                      __edition_override='asdfg')
    Traceback (most recent call last):
      ...
    ValueError: Unknown edition 'asdfg'

    @raises ValueError: when the passed edition is not supported.

    @type host_uuid: UUID

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    template_noexpanduser = \
        _get_default_sync_dir_setting_template(
            __debug_override=__debug_override,
            __sys_platform_override=__sys_platform_override,
            __edition_override=__edition_override)
    return path_ex.expanduser(template_noexpanduser).format(host_uuid)


def get_default_restore_dir(host_uuid,
                            __debug_override=None,
                            __sys_platform_override=None,
                            __edition_override=None):
    r"""
    For a given host UUID, returh the appropriate default path to the directory
    used for web restores.

    >>> home = os.path.expanduser('~')

    >>> get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='darwin',
    ...                         __edition_override='community') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='linux2',
    ...                         __edition_override='community') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='win32',
    ...                         __edition_override='community') == \
    ... u'./.restore-00000000-1111-0000-0000-000000000001'
    True

    >>> get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='darwin',
    ...                         __edition_override='personal') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='linux2',
    ...                         __edition_override='personal') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=True,
    ...                         __sys_platform_override='win32',
    ...                         __edition_override='personal') == \
    ... u'./.restore-00000000-1111-0000-0000-000000000001'
    True

    >>> get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='darwin',
    ...                         __edition_override='community') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='linux2',
    ...                         __edition_override='community') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='win32',
    ...                         __edition_override='community') == \
    ...     '{}/FreeBrie/Restored files'.format(home)
    True

    >>> get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='darwin',
    ...                         __edition_override='personal') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='linux2',
    ...                         __edition_override='personal') == \
    ... get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='win32',
    ...                         __edition_override='personal') == \
    ...     '{}/KeepTheCopy/Restored files'.format(home)
    True

    >>> get_default_restore_dir(UUID('00000000-1111-0000-0000-000000000001'),
    ...                         __debug_override=False,
    ...                         __sys_platform_override='darwin',
    ...                         __edition_override='asdfg')
    Traceback (most recent call last):
      ...
    ValueError: Unknown edition 'asdfg'

    @raises ValueError: when the passed edition is not supported.

    @type host_uuid: UUID

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    debug = coalesce(__debug_override, __debug__)
    sys_platform = coalesce(__sys_platform_override, sys.platform)

    assert __edition_override is not None or callable(get_edition), \
           'Import thost.setttings or uhost.settings before host.settings!'
    # Coalesce doesn't cut here, we shouldn't even launch get_edition()
    # in some cases.
    edition = get_edition() if __edition_override is None \
                            else __edition_override

    try:
        host_feature_set = host_editions.EDITIONS[edition]
    except KeyError:
        raise ValueError('Unknown edition {!r}'.format(edition))
    else:  # such edition is indeed supported
        if not debug:
            return path_ex.expanduser(
                       host_feature_set.dflt_restore_dir_tmpl.format(
                           **_get_dir_formatting_args(host_feature_set)))
        else:
            # debug mode; or on Linux (in any mode) - may need to be fixed
            # closer to the release.
            return os.path.join(
                       get_user_config_dir(
                           __debug_override=__debug_override,
                           __sys_platform_override=__sys_platform_override,
                           __edition_override=__edition_override),
                       '.restore-{}'.format(host_uuid)
                           if debug
                           else host_feature_set.short_name)


@contract_epydoc
def _get_ugroup_default_sync_dir(host_uuid, user_group,
                                 __debug_override=None,
                                 __sys_platform_override=None,
                                 __edition_override=None,
                                 __all_groups_same_sync_dir_override=None):
    r"""
    For a given host UUID, and a user group UUID (), return the appropriate
    default path to the directory used for syncing.

    >>> host_uuid = UUID('00000000-1111-0000-0000-0000-00000001')
    >>> ug_uuid = UserGroupUUID('00000000-bbbb-0000-0000-000000000001')
    >>> user_group = UserGroup(uuid=ug_uuid,
    ...                        name='Some User Group',
    ...                        private=False,
    ...                        enc_key='01234567012345670123456701234567')
    >>> home = os.path.expanduser('~')

    >>> # Test __debug__ = True, __all_groups_same_sync_dir_override=False
    >>> # any os, any edition.
    >>> _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='darwin',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=False) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='darwin',
    ...     __edition_override='personal',
    ...     __all_groups_same_sync_dir_override=False) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='linux2',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=False) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='win32',
    ...     __edition_override='superfolder',
    ...     __all_groups_same_sync_dir_override=False) == \
    ...     './.sync-{}_{}-Some User Group'.format(host_uuid, ug_uuid)
    True

    >>> # Test __debug__ = True, __all_groups_same_sync_dir_override=True;
    >>> # any os, any edition.
    >>> _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='linux2',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=True) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='win32',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=True) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='darwin',
    ...     __edition_override='superfolder',
    ...     __all_groups_same_sync_dir_override=True) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=True, __sys_platform_override='darwin',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=True) == \
    ...     './.sync-{}'.format(host_uuid)
    True

    >>> # Test __debug__ = False, __all_groups_same_sync_dir_override=False;
    >>> # for example, for community edition.
    >>> _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=False, __sys_platform_override='darwin',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=False) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=False, __sys_platform_override='linux2',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=False) == \
    ... _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=False, __sys_platform_override='win32',
    ...     __edition_override='community',
    ...     __all_groups_same_sync_dir_override=False) == \
    ...     '{}/FreeBrie/Shared/Some User Group'.format(home)
    True

    >>> # Test invalid edition
    >>> _get_ugroup_default_sync_dir(
    ...     host_uuid, user_group,
    ...     __debug_override=False, __sys_platform_override='darwin',
    ...     __edition_override='asdfg',
    ...     __all_groups_same_sync_dir_override=False)
    Traceback (most recent call last):
      ...
    ValueError: Unknown edition 'asdfg'

    @raises ValueError: when the passed edition is not supported.

    @type host_uuid: UUID
    @type user_group: UserGroup

    @returns: a forced-Unicode (to be sure for the OS-specific functions) path.
    @rtype: unicode
    """
    debug = coalesce(__debug_override, __debug__)
    sys_platform = coalesce(__sys_platform_override, sys.platform)

    edition = get_edition() if __edition_override is None \
                            else __edition_override

    all_groups_same_dir = coalesce(__all_groups_same_sync_dir_override,
                                   TEST_ALL_GROUPS_SAME_SYNC_DIR,
                                   False)

    try:
        host_feature_set = host_editions.EDITIONS[edition]
    except KeyError:
        raise ValueError('Unknown edition {!r}'.format(edition))
    else:  # such edition is indeed supported
        if not debug:
            return path_ex.expanduser(
                       host_feature_set.dflt_group_sync_dir_tmpl.format(
                           group_name=user_group.name,
                           **_get_dir_formatting_args(host_feature_set)))
        else:
            _template = \
                u'.sync-{host_uuid}' \
                    if all_groups_same_dir \
                    else '.sync-{host_uuid}_{ugroup_uuid}-{ugroup_name}'
            return os.path.join(
                       get_user_config_dir(
                           __debug_override=__debug_override,
                           __sys_platform_override=__sys_platform_override,
                           __edition_override=__edition_override),
                       _template.format(host_uuid=host_uuid,
                                        ugroup_uuid=user_group.uuid,
                                        ugroup_name=user_group.name)
                           if debug
                           else version.project_title)


# * .conf-file reading *

@contract_epydoc
def get_conf_setting(section, option, default=None):
    """
    Given the section name, option name and the default value,
    return the appropriate setting from the "host.conf" file
    (or the default value if unavailable).

    @rtype: basestring, NoneType
    """
    parser = common_settings.get_conf_parser(
                 _CONF_FILENAME_TEMPLATE.format(edition=get_edition()))

    try:
        return parser.get(section, option)
    except:
        return default


# * Host preparation *

@contract_epydoc
def _set_my_uuid(uuid):
    """Choose the current UUID to use.

    @type uuid: UUID
    """
    global MY_UUID

    assert MY_UUID is None or MY_UUID == NULL_UUID, repr(MY_UUID)
    MY_UUID = uuid


def _unset_my_uuid():
    """Unmark the currently used UUID."""
    global MY_UUID

    assert isinstance(MY_UUID, UUID), repr(MY_UUID)
    MY_UUID = None


@contract_epydoc
def configure_logging(my_uuid):
    """Configure logging for the Host.

    @param my_uuid: the UUID of the host
    @type my_uuid: UUID
    """
    try:
        # pylint:disable=F0401
        from host_magic import MIN_STDOUT_LOG_LEVEL as min_stdout_log_level
        # pylint:enable=F0401
    except ImportError:
        min_stdout_log_level = None
    try:
        # pylint:disable=F0401
        from host_magic import MIN_FILE_LOG_LEVEL as min_file_log_level
        # pylint:enable=F0401
    except ImportError:
        min_file_log_level = None

    log_dir = get_user_config_dir()

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    common_logger.configure_logging(
        dir_path=log_dir,
        prefix='host',
        postfix=my_uuid,
        log_level_stdout_override=min_stdout_log_level,
        log_level_file_override=min_file_log_level,
        debug_to_stdout=True,
        limit=LOG_ROTATE_MAX_BYTES,
        backup_count=LOG_ROTATE_MAX_BACKUPS)


@contract_epydoc
def _proceed_with_host_uuid(my_uuid):
    """
    Given the host UUID, run all the preparations which are needed
    to access logging, do the database access, etc.

    @param my_uuid: Host UUID
    @type my_uuid: UUID

    @raises NoHostDbException: If the database file is not initialized.
                               If occurs, use __init_host_db().
    """
    # Initialize settings
    logger.debug('Setting up uuid')
    _set_my_uuid(my_uuid)
    logger.debug('Config logging')
    configure_logging(my_uuid=my_uuid)

    logger.debug('Getting RDB url')
    rdb_url = get_db_url(my_uuid)
    logger.debug('RDB init')
    db.init(rdb_url, SQLALCHEMY_ECHO)

    if not db.RDB.is_schema_initialized:
        logger.debug('RDB is not initialized')
        # Need to create the database if the file is missing or 0 bytes long.
        raise NoHostDbException()
    else:
        logger.debug('Migrate if needed')
        __migrate_if_needed()
    # else:
    #     # During each execution, make sure that some settings
    #     # are initialized.
    #     if not HostQueries.HostSettings.get(Queries.Settings.TIMEZONE):
    #         with db.RDB() as rdbw:
    #             HostQueries.HostSettings.set(Queries.Settings.TIMEZONE,
    #                                          get_olson_tz_name(),
    #                                          rdbw=rdbw)


def __migrate_if_needed():
    """Migrate the database(s) to the most up-to-date version."""
    mig = HostMigrationPlan(rdbw_factory=db.RDB)
    mig.auto_upgrade()


@contract_epydoc
def proceed_with_host_uuid_ex(my_uuid, group_uuid,
                              username, digest,
                              listen_port,
                              node_uuid, node_urls,
                              do_reconfigure_host, do_create_db):
    """
    Expanded/extended version of _proceed_with_host_uuid(),
    which also deals with the database file recreation
    if it was unavailable on the first attempt.

    @param my_uuid: the UUID of the host.
    @type my_uuid: UUID

    @param group_uuid: the UUID of the user group.
    @type group_uuid: UUID

    @param listen_port: (optional) the exact TCP port we want to listen.
    @type listen_port: numbers.Integral, NoneType

    @raises NoHostDbException: If the database was absent,
                               and do_create_db is False.
    @raises DBInitializationIssue: If some issue occured
                                   during the DB initialization.
    """
    logger.debug('proceed_with_host_uuid_ex starts...')

    try:
        _proceed_with_host_uuid(my_uuid)
        logger.debug('_proceed_with_host_uuid(%r) completed', my_uuid)

        # For NULL_UUID, always recreate the database,
        # so raise NoHostDbException() forcefully.
        if my_uuid == NULL_UUID:
            logger.debug('Got NULL_UUID')
            raise NoHostDbException()
        else:
            if do_reconfigure_host:
                logger.debug('Reconfiguring host to %r', my_uuid)
                HostQueries.MyHost.reconfigure_my_host(my_uuid,
                                                       username,
                                                       digest)

            # During each execution, make sure that some settings
            # are initialized.
            with db.RDB() as rdbw:
                if not HostQueries.HostSettings.get(Queries.Settings.TIMEZONE):
                    tzname = get_olson_tz_name()
                    logger.debug('Setting Timezone to %r', tzname)
                    HostQueries.HostSettings.set(Queries.Settings.TIMEZONE,
                                                 tzname,
                                                 rdbw=rdbw)
                logger.debug('Setting Port to %r', listen_port)
                if listen_port is not None:
                    HostQueries.HostSettings.set(
                        Queries.Settings.PORT_TO_LISTEN, listen_port,
                        rdbw=rdbw)

    except NoHostDbException as e:
        logger.debug('proceed_with_host_uuid_ex failed with %r', e)
        # Probably we should create the DB.
        if do_create_db:
            try:
                __init_host_db(my_uuid,
                               group_uuid,
                               username,
                               digest,
                               listen_port,
                               node_uuid,
                               node_urls)
            except Exception as e:
                raise DBInitializationIssue(e, traceback.format_exc())

    logger.debug('proceed_with_host_uuid_ex ends.')


@contract_epydoc
def unproceed_with_host_uuid():
    """
    Do the actions opposite to _proceed_with_host_uuid():
    disable the current host UUID and deinitialize it to default state.
    """
    _unset_my_uuid()
    db.uninit()


@contract_epydoc
def __init_host_db(my_uuid, group_uuid,
                   init_username, init_digest,
                   init_listen_port, node_uuid, node_urls):
    """Initialize the database.

    If this is the database for null UUID, it will be cleaned completely
    before each initialization.

    @type my_uuid: UUID
    @type group_uuid: UUID
    @init_username: str
    @type init_listen_port: int
    @type node_uuid: UUID
    @precondition: consists_of(node_urls, str)
    """
    db_file_path = _get_db_file_path(my_uuid)

    if my_uuid == NULL_UUID:
        logger.debug('Wiping NULL UUID host db: %s', db_file_path)
        HostQueries.wipe_host_db(db_file_path)

    HostQueries.create_host_db_schema()

    HostQueries.MyHost.configure_my_host(my_uuid, group_uuid, init_username,
                                         init_digest, init_listen_port)

    # Find the directory to store chunks; create if missing
    chunk_dir = os.path.abspath(_get_default_chunk_dir(my_uuid))
    if not os.path.exists(chunk_dir):
        os.makedirs(chunk_dir)

    # Find the default directory to synchronize; create if missing
    sync_dir = os.path.abspath(get_default_sync_dir(my_uuid))
    if not os.path.exists(sync_dir):
        os.makedirs(sync_dir)

    if get_feature_set().web_initiated_restore:
        restore_dir = os.path.abspath(get_default_restore_dir(my_uuid))
        if not os.path.exists(restore_dir):
            os.makedirs(restore_dir)

    # Now write all the settings
    with db.RDB() as rdbw:
        _QS = Queries.Settings
        for k, v in {_QS.CHUNK_STORAGE_PATH: chunk_dir,
                     _QS.NODE_UUID:          node_uuid,
                     _QS.NODE_URLS:          node_urls,
                     _QS.PATHS_TO_BACKUP:    {sync_dir: {}},
                    }.iteritems():
            HostQueries.HostSettings.set(k, v, rdbw=rdbw)


# * Database settings readers/writers *

@contract_epydoc
def get_my_listen_port():
    """
    @returns: the port which is listened to on by this host.
    @rtype: int
    """
    return HostQueries.HostSettings.get(Queries.Settings.PORT_TO_LISTEN)


@contract_epydoc
def get_chunk_storage_path():
    """
    @returns: the path where the chunks should be stored
    @rtype: basestring
    """
    return HostQueries.HostSettings.get(Queries.Settings.CHUNK_STORAGE_PATH)


@contract_epydoc
def get_my_announced_urls():
    """
    Get the list of announced URLs, including both autodetected ones,
    and the ones explicitly mentioned in the DB.

    @returns: The list of URLs the host announces as listening to.
    @rtype: col.Iterable
    """
    my_port = get_my_listen_port()

    db_urls_str = HostQueries.HostSettings.get_my_urls()
    explicit_urls = set(json.loads(db_urls_str))

    # Detect my IPs
    _ifaces = get_if_addresses()
    if False and __debug__:
        # TODO: maybe return back when the better validation is available
        ifaces = _ifaces
    else:
        # On release versions, do not report the localhost addresses
        ifaces = {k: v
                      for k, v in _ifaces.iteritems()
                      if __debug__ or
                         not v.startswith('127.')}  # for Debug, it's ok
                                                    # to have localhost
    implicit_urls = {'https://{}:{:d}'.format(v, my_port)
                         for v in ifaces.itervalues()}

    return chain(explicit_urls,
                 implicit_urls - explicit_urls)


@contract_epydoc
def get_my_primary_node():
    """
    Get the node which is the primary for this host.

    @rtype: Node
    """
    return Node(uuid=HostQueries.HostSettings.get(Queries.Settings.NODE_UUID),
                urls=HostQueries.HostSettings.get(Queries.Settings.NODE_URLS))


@contract_epydoc
def get_all_settings_newer_than(ref_time):
    """
    @param ref_time: Only the settings newer than this value will be returned.
    @type ref_time: datetime

    @returns: The dictionary mapping all the setting names to the tuples
              of (value, last_update_time)
              for all the settings which have never been updated
              since "ref_time".
    @rtype: dict
    """
    with db.RDB() as rdbw:
        result = {name: (value, lu_time)
                      for name, value, lu_time
                          in HostQueries.HostSettings.get_all(rdbw=rdbw)
                      if lu_time > ref_time}

        return result


@contract_epydoc
def get_selected_paths():
    """Return the currently selected paths (files or directories).

    Note the paths are assumed to be in local path formatting
    (not necessary POSIX).

    @rtype: dict
    """
    return {os.path.abspath(path_ex.expanduser(k)): v
                for k, v in HostQueries.HostSettings
                                       .get(Queries.Settings.PATHS_TO_BACKUP)
                                       .iteritems()}


@contract_epydoc
def store_selected_paths_to_backup(paths):
    """
    Given the information aboth the selected directories/files, store it
    in the DB as "paths to backup" setting.

    Note the paths are assumed to be in local path formatting
    (not necessary POSIX).

    @type paths: dict
    """
    with db.RDB() as rdbw:
        HostQueries.HostSettings.set(Queries.Settings.PATHS_TO_BACKUP, paths,
                                     rdbw=rdbw)


@contract_epydoc
def _get_db_log_reporting_settings():
    """
    @rtype: LogReportingSettings
    """
    return HostQueries.HostSettings.get(Queries.Settings.LOG_SENDING_METHOD)


@contract_epydoc
def get_auto_update_settings():
    """Get the settings of auto update.

    @rtype: AutoUpdateSettings
    """
    return HostQueries.HostSettings.get(Queries.Settings.AUTO_UPDATE)


RE_UUID_FROM_DB_NAME = re.compile(
    r'^host-({hex}{w1}-{hex}{w2}-{hex}{w3}-{hex}{w4}-{hex}{w5})\.db$'.format(
        hex=r'(?:[\da-fA-F])',
        w1='{8}',
        w2='{4}',
        w3='{4}',
        w4='{4}',
        w5='{12}'))


@contract_epydoc
def get_available_db_uuids():
    """Get the host UUIDs of all available databases.

    @return: the list of all the UUIDs for all the databases
             available locally for this host.
    @rtype: list
    @postcondition: consists_of(result, UUID)
    """
    global RE_UUID_FROM_DB_NAME

    try:
        directory_listing = os.listdir(get_user_config_dir())
    except:
        return []
    else:
        # Get the available databases - but this may include
        # the NULL UUID as well
        pre_filter = \
            [UUID(mo.group(1))
                 for mo in sorted(ifilter(bool,
                                          imap(RE_UUID_FROM_DB_NAME.match,
                                               directory_listing)))]
        return pre_filter


def get_edition_short_name(__edition_override=None):
    r"""Returns edition short name.

    For example, for a FreeBrie Community Edition, it returns "FreeBrie".

    >>> get_edition_short_name(__edition_override='community')
    'FreeBrie'
    >>> get_edition_short_name(__edition_override='personal')
    'KeepTheCopy'
    >>> get_edition_short_name(__edition_override='superfolder')
    'SuperFolder'

    @rtype: basestring
    """
    return get_feature_set(__edition_override=__edition_override).short_name


def get_edition_full_name(__edition_override=None):
    r"""Returns edition full name.

    For example, for a FreeBrie Community Edition, it returns
    "FreeBrie Community Edition".

    >>> get_edition_full_name(__edition_override='community')
    'FreeBrie Community Edition'
    >>> get_edition_full_name(__edition_override='personal')
    'KeepTheCopy'
    >>> get_edition_full_name(__edition_override='superfolder')
    'SuperFolder'

    @rtype: basestring
    """
    return get_feature_set(__edition_override=__edition_override).full_name


def get_feature_set(__edition_override=None):
    r"""Get the host-specific feature set.

    >>> get_feature_set(
    ...     __edition_override='community')  # doctest:+NORMALIZE_WHITESPACE
    HostFeatureSet(codename='community',
        short_name='FreeBrie',
        full_name='FreeBrie Community Edition',
        per_group_encryption=True,
        web_initiated_restore=False,
        p2p_storage=True,
        blinking_icon=False,
        dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
        dflt_user_sync_dir_tmpl=u'~/{short_name}/My files',
        dflt_group_sync_dir_tmpl=u'~/{short_name}/Shared/{group_name}',
        dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
        first_launch_wizard=True)

    >>> get_feature_set(
    ...     __edition_override='personal')  # doctest:+NORMALIZE_WHITESPACE
    HostFeatureSet(codename='personal',
                   short_name='KeepTheCopy',
                   full_name='KeepTheCopy',
                   per_group_encryption=False,
                   web_initiated_restore=False,
                   p2p_storage=False,
                   blinking_icon=False,
                   dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
                   dflt_user_sync_dir_tmpl=u'~/{short_name}',
                   dflt_group_sync_dir_tmpl=u'~/{short_name}/{group_name}',
                   dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
                   first_launch_wizard=False)

    >>> get_feature_set(
    ...     __edition_override='superfolder')  # doctest:+NORMALIZE_WHITESPACE
    HostFeatureSet(codename='superfolder',
                   short_name='SuperFolder',
                   full_name='SuperFolder',
                   per_group_encryption=False,
                   web_initiated_restore=False,
                   p2p_storage=False,
                   blinking_icon=True,
                   dflt_user_conf_dir_tmpl=u'~/.{short_name_lc}',
                   dflt_user_sync_dir_tmpl=u'~/{short_name}',
                   dflt_group_sync_dir_tmpl=u'~/{short_name}/{group_name}',
                   dflt_restore_dir_tmpl=u'~/{short_name}/Restored files',
                   first_launch_wizard=False)

    @returns: the feature set for the host, considering the node has
        the edition as defined in the "common" section of the conf-file.
    @rtype: host_editions.HostFeatureSet
    """
    # Don't do coalesce() here!
    # get_edition() may not work in all the test cases where the following code
    # works fine.
    edition = __edition_override if __edition_override is not None \
                                 else get_edition()
    return host_editions.EDITIONS[edition]

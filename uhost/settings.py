#/usr/bin/python
"""The settings specific for any Untrusted Host.

@note: on import, it sets up some variables in C{host_settings}
    with the actual implementations.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import os
import re
import sys
from functools import partial
from types import NoneType
from uuid import UUID

from contrib.dbc import consists_of, contract_epydoc

from common import editions, settings as common_settings
from common.datatypes import (LoginPasswordTuple,
                              SMTPLogReportingSettings, LogReportingSettings)
from common.settings import str_to_bool
from common.system import get_install_file_ext
from common.utils import memoized

import host.settings as host_settings



#
# Constants
#

logger = logging.getLogger(__name__)


#
# Functions
#

# * .conf-file readers *

@contract_epydoc
def get_default_node_settings():
    """
    Return the tuple of default node settings for this host
    (from the "host.conf" file).

    @raises common_settings.UnreadableConfFileException:
        if the .conf-file could not be read at all.
    @returns: a tuple of node UUID and node URLs, or C{None}
              if they cannot be read.
    @rtype: tuple, NoneType
    @postcondition: result is None or (len(result) == 2 and
                                       isinstance(result[0], UUID) and
                                       isinstance(result[1], list) and
                                       consists_of(result[1], str)) # result
    """
    try:
        return (UUID(host_settings.get_conf_setting('default-node', 'uuid')),
                host_settings.get_conf_setting('default-node', 'urls').split())
    except common_settings.UnreadableConfFileException:
        raise
    except:
        return None


_RE_PARSE_ARGV0_NAME = re.compile(r'^'
                                  r'(?P<binary>[^\._]+)'
                                  r'[\._]'
                                  r'(?P<edition>[^\._]+)'
                                  r'(?:\..*)?'
                                  r'$')


def __binary_name_to_edition(base_argv0, platform):
    r"""Given the name of the binary file, try to guess the edition.

    @param base_argv0: the basename name of the binary file.
    @param platform:
    @precondition: platform in ('darwin', 'linux2', 'linux3', 'win32')

    >>> # Darwin

    >>> __binary_name_to_edition('FreeBrie Community Edition', 'darwin')
    'community'
    >>> __binary_name_to_edition('KeepTheCopy', 'darwin')
    'personal'
    >>> __binary_name_to_edition('SuperFolder', 'darwin')
    'superfolder'
    >>> __binary_name_to_edition('Someother Edition', 'darwin')
    Traceback (most recent call last):
       ...
    NotImplementedError: Unsupported binary name "Someother Edition" for darwin

    >>> # Linux

    >>> __binary_name_to_edition('chost.community', 'linux2')
    'community'
    >>> __binary_name_to_edition('chost_community.py', 'linux2')
    'community'
    >>> __binary_name_to_edition('chostgui2.community', 'linux3')
    'community'

    >>> __binary_name_to_edition('chost.personal', 'linux3')
    'personal'
    >>> __binary_name_to_edition('chost_personal.py', 'linux2')
    'personal'
    >>> __binary_name_to_edition('chostgui2.keepthecopy.py', 'linux2')
    'personal'
    >>> __binary_name_to_edition('chostgui2_personal', 'linux2')
    'personal'

    >>> __binary_name_to_edition('chost.superfolder', 'linux3')
    'superfolder'
    >>> __binary_name_to_edition('chost_superfolder.py', 'linux2')
    'superfolder'
    >>> __binary_name_to_edition('chostgui2.superfolder.py', 'linux2')
    'superfolder'
    >>> __binary_name_to_edition('chostgui2_superfolder', 'linux2')
    'superfolder'

    >>> __binary_name_to_edition('chost.py', 'linux2')
    Traceback (most recent call last):
       ...
    NotImplementedError: Unsupported binary name "chost.py" for linux2

    >>> # Windows

    >>> __binary_name_to_edition('chost.community.exe', 'win32')
    'community'
    >>> __binary_name_to_edition('chostgui2_community.exe', 'win32')
    'community'

    >>> __binary_name_to_edition('chost.personal.exe', 'win32')
    'personal'
    >>> __binary_name_to_edition('chostgui2_personal.exe', 'win32')
    'personal'
    >>> __binary_name_to_edition('chostgui2.keepthecopy.exe', 'win32')
    'personal'

    >>> __binary_name_to_edition('chost.superfolder.exe', 'win32')
    'superfolder'
    >>> __binary_name_to_edition('chostgui2_superfolder.exe', 'win32')
    'superfolder'
    >>> __binary_name_to_edition('chostgui2.superfolder.exe', 'win32')
    'superfolder'

    >>> __binary_name_to_edition('chostgui2.exe', 'win32')
    Traceback (most recent call last):
       ...
    NotImplementedError: Unsupported binary name "chostgui2.exe" for win32

    >>> # wrong platform

    >>> __binary_name_to_edition('chostgui2.app', 'os2')
    Traceback (most recent call last):
       ...
    NotImplementedError: Unsupported platform "os2"
    """
    if platform == 'darwin':
        # base_argv0 contains the app name, which generally must be among
        # C{editions.EDITIONS} values.
        for e in editions.EDITIONS:
            if editions.EDITIONS[e].full_name == base_argv0:
                return e

        raise NotImplementedError(u'Unsupported binary name "{}" for {}'
                                      .format(base_argv0, platform))

    elif platform.startswith('linux') or platform == 'win32':
        # The executable should contain the edition name after the host type.
        gdict = _RE_PARSE_ARGV0_NAME.match(base_argv0).groupdict()
        try:
            edition_candidate = gdict['edition']  # may fail: let it fail
            # Try to find among the EDITION keys
            if edition_candidate in editions.EDITIONS:
                # Ok, do nothing
                pass
            else:
                # Otherwise, let's assume the lowercased version
                # of the short name is used for guessing.
                keys_by_shortname_lc = \
                    {fs.short_name.lower(): k
                         for k, fs in editions.EDITIONS.iteritems()}
                edition_candidate = keys_by_shortname_lc[edition_candidate]

            # Final check
            if edition_candidate not in editions.EDITIONS:
                raise Exception(u'Unknown candidate {!r}'.format(
                                    edition_candidate))
        except:
            raise NotImplementedError(u'Unsupported binary name "{}" for {}'
                                          .format(base_argv0, platform))
        else:
            return edition_candidate

    else:
        raise NotImplementedError(u'Unsupported platform "{}"'
                                      .format(platform))


@memoized
@contract_epydoc
def detect_edition():
    """Perform some magic to detect the actual client edition.

    @returns: client edition
    """
    return __binary_name_to_edition(os.path.basename(sys.argv[0]),
                                    sys.platform)


@contract_epydoc
def get_website_setting():
    """Returns the "website" setting from "urls" section of "host.conf" file.

    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'website')

@contract_epydoc
def get_cpanel_page_setting():
    """
    Returns the "cpanel-page" setting from "urls" section of "host.conf" file.

    @returns: client edition
    """
    return host_settings.get_conf_setting('urls', 'cpanel-page')

@contract_epydoc
def get_facebook_page_setting():
    """
    Returns the "facebook-page" setting from "urls" section of
    "host.conf" file.

    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'facebook-page')


@contract_epydoc
def get_support_page_setting():
    """
    Returns the "support-page" setting from "urls" section of "host.conf" file.

    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'support-page')


@contract_epydoc
def get_upgrade_page_url_setting():
    """Return the upgrade page URL from the "host.conf" file.

    @returns: upgrade page URL, or None if it is unavailable.
    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'upgrade-page')


@contract_epydoc
def __get_upgrade_file_template_setting():
    """Return the upgrade file template from the "host.conf" file.

    @returns: upgrade file template, or None if it is unavailable.
    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'upgrade-file-template')


@contract_epydoc
def get_upgrade_file_url(build):
    """
    Based upon the upgrade file URL template from the "host.conf" file,
    return the URL of the upgrade file, appropriate for the system
    it is running on, for a given build.

    @returns: upgrade file url, or None if it is unavailable.
    @rtype: basestring, NoneType
    """
    assert build is not None

    tmpl = __get_upgrade_file_template_setting()

    if tmpl is None:
        return tmpl
    else:
        return tmpl.format(build=build,
                           extension=get_install_file_ext())


@contract_epydoc
def get_create_account_page_setting():
    """
    Return the "create account" URL from the "host.conf" file.

    @returns: "create account" URL, or None if it is unavailable.
    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'create-account-page')


@contract_epydoc
def get_restore_password_page_setting():
    """
    Return the "create account" URL from the "host.conf" file.

    @returns: "create account" URL, or None if it is unavailable.
    @rtype: basestring, NoneType
    """
    return host_settings.get_conf_setting('urls', 'restore-password-page')


@contract_epydoc
def _get_default_log_reporting_settings():
    """
    Return the default log reporting settings from the "host.conf" file.

    @returns: default log reporting settings.
    @rtype: LogReportingSettings
    """
    _get = partial(host_settings.get_conf_setting, 'default-log-reporting')

    _smtp = SMTPLogReportingSettings(
                recipient=_get('smtp-recipient', ''),
                server=_get('smtp-server', ''),
                starttls=str_to_bool(_get('smtp-starttls', 'No')),
                loginpassword=LoginPasswordTuple(_get('smtp-login'),
                                                 _get('smtp-password')))

    return LogReportingSettings(
            method=LogReportingSettings.Methods.or_default(_get('method')),
            smtp_settings=_smtp)


@contract_epydoc
def get_log_reporting_settings():
    """
    Return the log reporting settings from the database (if present),
    otherwise from the defaults.

    @rtype: LogReportingSettings
    """
    try:
        settings = host_settings._get_db_log_reporting_settings()
    except Exception:
        settings = False
    if not settings:
        settings = _get_default_log_reporting_settings()
    return settings



#
# Variables/constrants
#

host_settings._CONF_FILENAME_TEMPLATE = 'host.{edition}.conf'
host_settings.get_edition = detect_edition

host_settings.LOG_ROTATE_MAX_BYTES = 10 * 1024 * 1024
host_settings.LOG_ROTATE_MAX_BACKUPS = 5

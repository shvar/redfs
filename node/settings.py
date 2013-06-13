#!/usr/bin/python
"""
Working with node-specific settings.
"""

#
# Imports
#

from __future__ import absolute_import
import logging
import os
import signal
import sys
import threading
from ConfigParser import NoSectionError, NoOptionError, \
                         Error as ConfigParserError
from collections import namedtuple
from datetime import timedelta
from types import NoneType
from uuid import UUID

from OpenSSL import crypto

from contrib.dbc import contract_epydoc, consists_of

from common import (editions, version, logger as common_logger,
                    settings as common_settings)
from common.utils import exceptions_logged
from common.build_version import get_build_timestamp

from trusted.settings import get_log_directory


#
# Constants
#

logger = logging.getLogger(__name__)

NODE_VERSION_STRING = version.version_string_template.format('Node')

LOG_ROTATE_MAX_BYTES = 1024 * 1024 * 1024
LOG_ROTATE_MAX_BACKUPS = None

# initialized below by ConfSettings.reread_from_conf_file()
__CONF_SETTINGS = None

__old_sigusr1_handler = None



#
# Classes
#

class ConfSettings(object):
    """
    The class to conveniently access all the node settings.
    """

    __slots__ = ('edition', 'ssl_cert', 'ssl_pkey',
                 'build_version', 'required_version', 'recommended_version',
                 'rel_db_url', 'fast_db_url', 'big_db_url')


    def __init__(self, edition, ssl_cert, ssl_pkey,
                       build_version, required_version, recommended_version,
                       rel_db_url, fast_db_url, big_db_url):
        self.edition = edition
        self.ssl_cert = ssl_cert
        self.ssl_pkey = ssl_pkey
        self.build_version = build_version
        self.required_version = required_version
        self.recommended_version = recommended_version
        self.rel_db_url = rel_db_url
        self.fast_db_url = fast_db_url
        self.big_db_url = big_db_url


    @classmethod
    def reread_from_conf_file(cls):
        """
        Reread the conf-file and return a new instance of C{ConfSettings}
        with the settings as defined in the node.conf file.

        This includes three build versions (version timestamps):
        the node build version, the required version and the recommended version.
        The first one is calculated from the code,
        the latter two are defined in the node.conf file.
        If they are absent, they are returned equal to the node own version.

        @note: each call to this function perform complete parsing of the
        Mercurial repository, and of the config file if applicable.

        @returns: the tuple containing all the node-specific settings.

        @rtype: ConfSettingsTuple

        @raises Exception: if something important cannot be determined
                           or failed to be parsed.
        """
        parser = common_settings.get_conf_parser('node.conf',
                                                 relative_path='.')
        _conf_dir = common_settings.conf_dir()

        try:
            with open(os.path.join(_conf_dir,
                                   parser.get('common', 'ssl-cert')),
                      'r') as fh:
                _cert = crypto.load_certificate(crypto.FILETYPE_PEM,
                                                fh.read())
        except Exception:
            #raise Exception("Cannot read ssl-cert setting: %s" % e)
            _cert = None

        try:
            with open(os.path.join(_conf_dir,
                                   parser.get('common', 'ssl-pkey')),
                      'r') as fh:
                _pkey = crypto.load_privatekey(crypto.FILETYPE_PEM,
                                               fh.read())
        except Exception:
            #raise Exception("Cannot read ssl-pkey setting: %s" % e)
            _pkey = None

        _build = get_build_timestamp()

        _edition = _parser_get_raw(parser,
                                   'common', 'edition')
        # Read raw values from the conf file.
        _req = _parser_get_raw(parser,
                               'common', 'required-version',
                               _build)
        _rec = _parser_get_raw(parser,
                               'common', 'recommended-version',
                               _build)
        _rel_db_url = _parser_get_raw(parser,
                                     'common', 'rel-db-url')
        _fast_db_url = _parser_get_raw(parser,
                                       'common', 'fast-db-url')
        _big_db_url = _parser_get_raw(parser,
                                      'common', 'big-db-url')

        return cls(edition=_edition,
                   ssl_cert=_cert, ssl_pkey=_pkey,
                   build_version=_build,
                   required_version=_req, recommended_version=_rec,
                   rel_db_url=_rel_db_url,
                   fast_db_url=_fast_db_url,
                   big_db_url=_big_db_url)


    @property
    def build_versions(self):
        """
        @returns: the tuple the consists the build version,
            the required version and the recommended version.
        """
        return (self.build_version,
                self.required_version,
                self.recommended_version)



#
# Functions
#

def _parser_get_raw(parser, section, option, default=None):
    """
    Given a section name and an option name, return the raw value from
    the settings. If undefined, return the default value.
    """
    try:
        return parser.get(section, option)
    except (NoSectionError, NoOptionError):
        return default


NodeConfSettings = namedtuple('NodeConfSettings',
                              ('port', 'uuid', 'control_sockets'))


@contract_epydoc
def get_my_nodes_settings():
    """
    Return the mapping from node port to the node settings
    (which include the port as well, btw).
    This mapping is taken from the node.conf file exclusively.

    @rtype: dict
    @postcondition: consists_of(result.iterkeys(), int)
    @postcondition: consists_of(result.itervalues(), NodeConfSettings)
    """
    parser = common_settings.get_conf_parser('node.conf', relative_path='.')
    node_sections = [s for s in parser.sections() if s.startswith('node-')]

    result = {}
    # Should do that iteratively, to generate proper error logs
    for sect in node_sections:
        # Read and validate all options for this node
        try:
            _port = parser.getint(sect, 'port')
        except:
            logger.critical('Cannot read port for section %s', sect)
            continue

        try:
            _uuid = UUID(parser.get(sect, 'uuid'))
        except:
            logger.critical('Cannot read uuid for section %s', sect)
            continue

        try:
            _control_sockets = parser.get(sect, 'control-sockets').split()
        except ConfigParserError:
            logger.critical('Cannot read control-sockets for section %s', sect)
            continue

        # Seems all required options are present, add the node.
        assert _port not in result
        result[_port] = NodeConfSettings(port=_port,
                                         uuid=_uuid,
                                         control_sockets=_control_sockets)

    return result


def configure_logging(postfix):
    """Configure logging for the Node.

    @param postfix: the postfix for the log files.
    @type postfix: str
    """
    try:
        # pylint:disable=F0401
        from node_magic import MIN_STDOUT_LOG_LEVEL as min_stdout_log_level
        # pylint:enable=F0401
    except ImportError:
        min_stdout_log_level = None

    try:
        # pylint:disable=F0401
        from node_magic import MIN_FILE_LOG_LEVEL as min_file_log_level
        # pylint:enable=F0401
    except ImportError:
        min_file_log_level = None

    common_logger.configure_logging(
        dir_path=get_log_directory(),
        prefix='node',
        postfix=postfix,
        log_level_stdout_override=min_stdout_log_level,
        log_level_file_override=min_file_log_level,
        debug_to_stdout=True,
        limit=LOG_ROTATE_MAX_BYTES,
        backup_count=LOG_ROTATE_MAX_BACKUPS)


@exceptions_logged(logger)
def sigusr1_handler(sig, frame):
    """
    Unix signal handler - reread the configuration file
    """
    global __old_sigusr1_handler, __CONF_SETTINGS

    assert sig in (signal.SIGUSR1,), \
           'Unknown signal {!r}'.format(sig)

    logger.debug('Got signal %r, rereading settings', sig)
    __CONF_SETTINGS = ConfSettings.reread_from_conf_file()

    if callable(__old_sigusr1_handler):
        __old_sigusr1_handler(sig, frame)


def get_common():
    """
    @returns: the (already) cached settings of the node, "common" section.
    @rtype: ConfSettings
    """
    global __CONF_SETTINGS
    assert __CONF_SETTINGS is not None
    return __CONF_SETTINGS


def get_feature_set():
    """
    @returns: the feature set for the node, considering the node has
        the edition as defined in the "common" section of the conf-file.
    @rtype: editions.FeatureSet
    """
    global __CONF_SETTINGS
    assert __CONF_SETTINGS is not None
    return editions.EDITIONS[__CONF_SETTINGS.edition]


def init():
    global __CONF_SETTINGS, __old_sigusr1_handler

    # Emulate SIGUSR1, to read and cache the configuration.
    try:
        __CONF_SETTINGS = ConfSettings.reread_from_conf_file()
        sigusr1_handler(signal.SIGUSR1, None)
    except Exception as e:
        print(e)
        sys.exit(-1)

    if not threading.current_thread().daemon:
        # This is the working thread, so bind the signal handlers
        __old_sigusr1_handler = signal.signal(signal.SIGUSR1, sigusr1_handler)



#
# Main
#

# TODO: this is most likely wrong, and the whole init() should be called
# on the first run, rather than on import. But leave it as is for now.
if not 'unittest' in sys.modules:
    init()  # initialize the settings even on an import

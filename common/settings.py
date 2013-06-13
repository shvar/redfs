#!/usr/bin/python
"""
Common code to work with settings.
"""

#
# Imports
#

from __future__ import absolute_import

import sys
import os
import ConfigParser

if sys.platform == 'win32':
    from win32com.shell import shellcon, shell  # pylint:disable=F0401

from contrib.dbc import contract_epydoc

from . import version
from .abstractions import IPrintable
from .utils import memoized


#
# Module variables/constants
#

HTTP_AUTH_REALM_NODE = '{0} {1}'.format(version.project_internal_codename,
                                        'node')
HTTP_AUTH_REALM_HOST = '{0} {1}'.format(version.project_internal_codename,
                                        'host')



#
# Classes
#

class UnreadableConfFileException(Exception, IPrintable):
    __slots__ = ('path',)

    def __init__(self, path, *args, **kwargs):
        super(UnreadableConfFileException, self).__init__(*args, **kwargs)
        self.path = path


    def __str__(self):
        return u'Cannot read .conf file from {!r}!'.format(self.path)



#
# Functions
#

def conf_dir():
    """
    On Windows, it will be something like
      "All Users/Application Data/.freebrie" but with the environment
    variables expanded.
    On Linux in non-debug mode, it will be "/etc/freebrie".
    On Linux in debug mode, it will be "etc".
    On OSX it will be "etc".
    """

    _platform = sys.platform
    if __debug__ or _platform == 'darwin' or in_dev_env():
        return 'etc'
    else:
        if _platform == 'win32':
            return os.path.join(
                       shell.SHGetFolderPath(0, shellcon.CSIDL_COMMON_APPDATA,
                                             0, 0),
                       u'.{}'.format(version.project_name))
        elif _platform.startswith('linux'):
            return os.path.join('/etc', version.project_name)
        else:
            raise NotImplementedError('')


@memoized
def in_dev_env():
    in_repo = None
    try:
        import mercurial
    except ImportError:
        in_repo = False
    else:
        repo_path = os.path.abspath(os.path.join(__file__, '../../..'))
        try:
            _ui = mercurial.ui.ui()
            _repo = mercurial.hg.repository(_ui, repo_path)
        except mercurial.error.RepoError:
            in_repo = False
        else:
            in_repo = True

    return all([in_repo])



@contract_epydoc
def get_conf_parser(filename,
                    relative_path='',
                    defaults=None,
                    parser_cls=ConfigParser.RawConfigParser):
    """
    Given a filename of the .conf file, returns a parser on it.

    @param parser_cls: when some specific treatment of the returned values
        is needed (such as, postprocessing, or type conversion), one can
        provide the custom derivative of C{ConfigParser.RawConfigParser}
        to use for configuration files parsing.
    @precondition: issubclass(parser_cls, ConfigParser.RawConfigParser)
        # parser_cls
    """
    if defaults is None:
        defaults = {}
    parser = parser_cls(defaults)
    try:
        path = os.path.join(relative_path, conf_dir(), filename)
        parser.readfp(open(path))
    except:
        raise UnreadableConfFileException(path)

    return parser


_BOOL_TO_STR_MAP = {True:  'True',
                    False: 'False'}


@contract_epydoc
def bool_to_str(bool_value):
    """
    Convert a boolean value to string, to use in the setting purposes.

    @type bool_value: bool
    @rtype: str
    """
    return _BOOL_TO_STR_MAP[bool(bool_value)]


_STR_TO_BOOL_MAP = {'yes': True,
                    'no':  False}


@contract_epydoc
def str_to_bool(str_value):
    """
    Convert a string value to bool, to use in the setting purposes.

    The string MUST be a variation of "Yes" or "No",
    otherwise a ValueError will be raised.

    @type str_value: basestring
    @rtype: bool
    @raises ValueError: if the C{str_value} argument does not
                        contain a proper boolean-meaning value.
    """
    try:
        return _STR_TO_BOOL_MAP[str_value.lower()]
    except KeyError:
        raise ValueError(str_value)

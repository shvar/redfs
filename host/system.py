#!/usr/bin/python
"""
Various low-level system-specific hacks, needed on Host only.
"""

from __future__ import absolute_import
import collections as col
import logging
import os
import platform
import sys
import time
import warnings

if sys.platform == 'win32':
    from xml.dom.minidom import parseString  # __convert_tz_name_win_to_olson

    # for __get_if_addresses_win32
    from ctypes import POINTER, Structure, byref, sizeof, windll
    from ctypes import c_char, c_ubyte, c_uint, c_ulong

    # for get_olson_tz_name
    from win32timezone import TimeZoneInfo  # pylint:disable=F0401

    # for _win32_current_app_path
    import win32con
    import win32api
    import win32security
    import ntsecuritycon

elif sys.platform.startswith('linux'):
    # for __get_if_addresses_linux
    import array
    import fcntl
    import socket
    import struct

import netifaces

from common import editions
from common.utils import on_exception
from common.settings import in_dev_env

# The redundant warning is generated by pkg_resource in case
# if twisted is imported after netifaces
warnings.filterwarnings('ignore',
                        r'Module \S+ was already imported from \S+, '
                            'but \S+ is being added to sys.path')



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Functions
#

def __get_if_addresses_netifaces():
    """
    Get the IP addresses of all the available interfaces.
    Returns dictionaries mapping the interface name to the string
    of its IP address.
    Requires python-netifaces module to be installed.

    @rtype: dict

    >>> # Just validate the function being called.
    >>> bool(get_if_addresses()) or True
    True
    """
    # Due to a bug in netifaces.ifaddresses(), it may raise ValueError
    # on some values which were returned by netifaces.interfaces().
    # Let's be safe.

    # Instead of raising ValueError, this wrapped function returns
    # an empty dictionary.
    ifaddresses_wrapped = on_exception(ValueError, {})(netifaces.ifaddresses)

    # Note the values may be empty dictionaries!
    iface_groups = {i: ifaddresses_wrapped(i)
                        for i in netifaces.interfaces()}

    return {i: al['addr']
                for i, gr in iface_groups.iteritems()
                if netifaces.AF_INET in gr
                for al in gr[netifaces.AF_INET]}


def __get_if_addresses_linux():
    """
    Idea:
    http://code.activestate.com/recipes/
        439093-get-names-of-all-up-network-interfaces-linux-only/

    @note: Alternate version: http://coderstalk.blogspot.com/2011/04/
                                  python-code-to-get-ip-address-from.html
           (/proc/net/dev)
    """
    SIOCGIFCONF = 0x8912
    MAXBYTES = 8096

    arch = platform.architecture()[0]

    # I really don't know what to call these right now
    if arch == '32bit':
        var1 = 32
        var2 = 32
    elif arch == '64bit':
        var1 = 16
        var2 = 40
    else:
        raise OSError('Unknown architecture: {}'.format(arch))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    names = array.array('B', '\0' * MAXBYTES)
    outbytes = struct.unpack('iL', fcntl.ioctl(
        sock.fileno(),
        SIOCGIFCONF,
        struct.pack('iL', MAXBYTES, names.buffer_info()[0])
        ))[0]

    namestr = names.tostring()
    return {namestr[i:i + var1].split('\0', 1)[0]:
              socket.inet_ntoa(namestr[i + 20:i + 24])
                for i in xrange(0, outbytes, var2)}


def __get_if_addresses_win32():
    """
    Idea:
    http://stackoverflow.com/questions/166506/
        finding-local-ip-addresses-using-pythons-stdlib

    @todo: Support multiple IPs on the same interface!
    """
    MAX_ADAPTER_DESCRIPTION_LENGTH = 128
    MAX_ADAPTER_NAME_LENGTH = 256
    MAX_ADAPTER_ADDRESS_LENGTH = 8

    class IP_ADDR_STRING(Structure):
        pass

    LP_IP_ADDR_STRING = POINTER(IP_ADDR_STRING)
    IP_ADDR_STRING._fields_ = [
        ('next', LP_IP_ADDR_STRING),
        ('ipAddress', c_char * 16),
        ('ipMask', c_char * 16),
        ('context', c_ulong)
    ]

    class IP_ADAPTER_INFO (Structure):
        pass

    LP_IP_ADAPTER_INFO = POINTER(IP_ADAPTER_INFO)
    IP_ADAPTER_INFO._fields_ = [
        ('next', LP_IP_ADAPTER_INFO),
        ('comboIndex', c_ulong),
        ('adapterName', c_char * (MAX_ADAPTER_NAME_LENGTH + 4)),
        ('description', c_char * (MAX_ADAPTER_DESCRIPTION_LENGTH + 4)),
        ('addressLength', c_uint),
        ('address', c_ubyte * MAX_ADAPTER_ADDRESS_LENGTH),
        ('index', c_ulong),
        ('type', c_uint),
        ('dhcpEnabled', c_uint),
        ('currentIpAddress', LP_IP_ADDR_STRING),
        ('ipAddressList', IP_ADDR_STRING),
        ('gatewayList', IP_ADDR_STRING),
        ('dhcpServer', IP_ADDR_STRING),
        ('haveWins', c_uint),
        ('primaryWinsServer', IP_ADDR_STRING),
        ('secondaryWinsServer', IP_ADDR_STRING),
        ('leaseObtained', c_ulong),
        ('leaseExpires', c_ulong)
    ]

    GetAdaptersInfo = windll.iphlpapi.GetAdaptersInfo
    GetAdaptersInfo.restype = c_ulong
    GetAdaptersInfo.argtypes = [LP_IP_ADAPTER_INFO, POINTER(c_ulong)]
    adapterList = (IP_ADAPTER_INFO * 10)()
    buflen = c_ulong(sizeof(adapterList))
    rc = GetAdaptersInfo(byref(adapterList[0]), byref(buflen))

    result = {}
    if rc == 0:
        for a in adapterList:
            adNode = a.ipAddressList
            while True:
                ipAddr = adNode.ipAddress
                if ipAddr:
                    result['adapterName'] = ipAddr
                adNode = adNode.next
                if not adNode:
                    break
    return result

#if sys.platform == "win32":
#    get_if_addresses = __get_if_addresses_win32
#elif sys.platform.startswith("linux"):
#    get_if_addresses = __get_if_addresses_linux
#else:
#    raise NotImplementedError(sys.platform)


def get_if_addresses():
    """
    >>> ifs = get_if_addresses()
    >>> isinstance(ifs, col.Mapping) and bool(ifs)
    True
    """
    return __get_if_addresses_netifaces()


__olson_tz_name = None


if sys.platform == 'win32':
    __win32timezone_to_en = None

    __windows_to_olson_tz_name_mapping = None

    def __convert_tz_name_win_to_olson(windows_tz_name):
        """
        Given a Windows-used timezone name (like 'Russian Standard Time'),
        convert it to the Olson timezone name (like 'Europe/Moscow').

        The official mapping from
        http://unicode.org/repos/cldr/trunk/common/supplemental/
            windowsZones.xml
        is used.

        @returns: Either the OLSON timezone name, or None
                  (if cannot be converted).
        """
        global __windows_to_olson_tz_name_mapping

        assert sys.platform == 'win32', repr(sys.platform)

        try:
            if __windows_to_olson_tz_name_mapping is None:
                if __debug__ or in_dev_env():
                    xml_dir_path = os.path.join('contrib', 'windowsZones')
                else:
                    xml_dir_path = 'libs'
                with open(os.path.join(xml_dir_path, 'windowsZones.xml'),
                          'rb') as fh:
                    dom = parseString(fh.read())

                __windows_to_olson_tz_name_mapping = \
                    {e.getAttribute('other'): e.getAttribute('type')
                         for e in dom.getElementsByTagName('mapZone')}

            assert isinstance(__windows_to_olson_tz_name_mapping, dict), \
                   repr(__windows_to_olson_tz_name_mapping)

            return __windows_to_olson_tz_name_mapping.get(windows_tz_name)

        except Exception as e:
            logger.exception('Cannot convert the timezone %r',
                             windows_tz_name)
            return None


def __force_get_olson_tz_name():
    """
    Retrieve the name of the local timezone in a format
    used by the OLSON timezone database (eg. like "Europe/Moscow").

    >>> tzname = __force_get_olson_tz_name()
    >>> import pytz
    >>> pytz.timezone(tzname) is not None
    True

    @rtype: basestring
    """
    try:
        if sys.platform == 'win32':
            global __win32timezone_to_en

            if __win32timezone_to_en is None:
                __win32timezone_to_en = \
                    dict(TimeZoneInfo._get_indexed_time_zone_keys('Std'))

            # Get timezone name, probably in local language
            win32tz_name = TimeZoneInfo.local().timeZoneName
            # Get the timezone name in English language,
            # like "Russian Standard Time"
            win32timezone_name_en = __win32timezone_to_en.get(win32tz_name,
                                                              win32tz_name)
            olson_tz = __convert_tz_name_win_to_olson(win32timezone_name_en)
            if olson_tz is None:
                raise Exception('Can not retrieve olson tz name')
            else:
                return olson_tz

        elif sys.platform.startswith('linux'):
            with open('/etc/timezone', 'r') as fh:
                return fh.read().strip()

        elif sys.platform == 'darwin':
            return os.readlink('/etc/localtime').strip('/usr/share/zoneinfo/')

        else:
            raise NotImplementedError(sys.platform)

    except NotImplementedError as e:
        raise

    except Exception as e:
        logger.exception('Cannot get the local timezone, using default')
        return ''


def get_olson_tz_name():
    """
    Retrieve the name of the local timezone in a format
    used by the OLSON timezone database (eg. like "Europe/Moscow").

    >>> tzname = get_olson_tz_name() # doctest:+ELLIPSIS
    >>> import pytz
    >>> pytz.timezone(tzname) is not None
    True

    @note: The value is cached, so each consequent call will be faster
           but will return the same value.

    @rtype: basestring
    """
    global __olson_tz_name

    if __olson_tz_name is None:
        __olson_tz_name = __force_get_olson_tz_name()

    return __olson_tz_name


class NotInAppError(Exception):
    pass


def _darwin_current_app_path(edition=None):
    assert edition is None or edition in editions.EDITIONS, \
           repr(edition)

    logger.debug("What's the current_app_path(%r) for darwin?", edition)

    result = None
    path_list = os.path.abspath(__file__).split(os.path.sep)
    logger.debug('__file__ = %r', __file__)
    if path_list[1].lower() == 'applications':
        result = os.path.join(os.path.sep, *path_list[:3])
    else:
        for i, part in enumerate(path_list):
            if part.endswith('.app') and \
               path_list[i + 1].lower() == 'contents':
                result = os.path.join(os.path.sep, *path_list[:i + 1])
                break

    if result is not None:
        logger.debug('current_app_path() = %r', result)
        return result
    else:
        raise NotInAppError(path_list)


def _win32_current_app_path(edition):
    assert edition in editions.EDITIONS, repr(edition)
    edition_full_name = editions.EDITIONS[edition].full_name

    try:
        fbkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE,
                                      'SOFTWARE\\FreeBrie\\{}'
                                          .format(edition_full_name))
        instdir = str(win32api.RegQueryValueEx(fbkey, 'Install_Dir')[0])
    except Exception:
        logger.exception('application install path not in win32 register')
        instdir = ''
    finally:
        win32api.RegCloseKey(fbkey)

    logger.debug('current_app_path() = %r', instdir)
    return instdir


if sys.platform == 'darwin':
    current_app_path = _darwin_current_app_path
elif sys.platform == 'win32':
    current_app_path = _win32_current_app_path
else:
    # Not implemented
    current_app_path = lambda edition: None


def __test_can_create(path):
    try:
        f = open(path, 'wb')
        f.close()
    except Exception:
        return False
    os.remove(path)
    return True


def can_self_update(edition):
    """Test that we can update our app from current user.

    @param edition: product edition.
    """
    assert edition in editions.EDITIONS, repr(edition)

    if sys.platform.startswith('linux'):
        return False
    elif sys.platform.startswith('win'):
        return True
    # test that we can write to application path and it's parent path
    app_path = current_app_path(edition)
    install_path = os.path.split(app_path)[0]
    tstfile = '.writetest{}'.format(int(time.time()))

    path1 = os.path.join(app_path, tstfile)
    path2 = os.path.join(install_path, tstfile)
    can_create_path1, can_create_path2 = (__test_can_create(path1),
                                          __test_can_create(path2))

    logger.debug('Can self update? %s write to %r, %s write to %r',
                 'Can' if can_create_path1 else "Can't",
                 path1,
                 'Can' if can_create_path2 else "Can't",
                 path2)

    return can_create_path1 and can_create_path2



if sys.platform == 'win32':
    from contrib.ntfsutils import junction
    _create_link_to_dir = junction.create
    _is_link_to_dir = junction.isjunction
    _read_link_to_dir = junction.readlink
    _remove_link_to_dir = junction.unlink
else:
    _create_link_to_dir = os.symlink
    _is_link_to_dir = os.path.islink
    _read_link_to_dir = os.readlink
    _remove_link_to_dir = os.unlink


def create_link_to_dir(source, destination):
    """
    Create a link to the directory, generally similarly to
    C{ln -s source destination}

    Symlink on Unix, junction point on Windows.

    @type source: basestring
    @type destination: basestring
    """
    if not os.path.isdir(source):
        raise OSError('Invalid argument: %r', source)
    return _create_link_to_dir(source, destination)


def is_link_to_dir(path):
    """
    Check whether some path is in fact a link (symlink or junction point)
    to the directory rather than the actual directory.

    May raise an exception, in case if, say, the path does not exist.

    @type path: basestring
    """
    return _is_link_to_dir(path) if os.path.isdir(path) else False


def read_link_to_dir(path):
    """
    Take a link to the directory (a symlink, or junction poin)
    and return the destination of the link.

    @type path: basestring

    @raises OSError: if C{path} is actually not a link to the directory.
    """
    if not is_link_to_dir(path):
        raise OSError('Invalid argument: %r', path)
    return _read_link_to_dir(path)


def remove_link_to_dir(path):
    if not is_link_to_dir(path):
        raise OSError('Invalid argument: %r', path)
    return _remove_link_to_dir(path)


def __win32_add_acces_denied_ace(path, ace):
    user, _ignore, _ignore = \
        win32security.LookupAccountName("", win32api.GetUserName())
    sd = win32security.GetFileSecurity(
        path, win32security.DACL_SECURITY_INFORMATION)
    dacl = sd.GetSecurityDescriptorDacl()
    dacl.AddAccessDeniedAce(
        win32security.ACL_REVISION,
        ntsecuritycon.DELETE | ntsecuritycon.FILE_DELETE_CHILD,
        user)
    sd.SetSecurityDescriptorDacl(1, dacl, 0)
    win32security.SetFileSecurity(
        path, win32security.DACL_SECURITY_INFORMATION, sd)


def __win32_deny_path_deletion(path):
    __win32_add_acces_denied_ace(
        path, ntsecuritycon.DELETE)


def __win32_deny_childs_deletion(path):
    __win32_add_acces_denied_ace(
        path, ntsecuritycon.FILE_DELETE_CHILD)


def deny_path_deletion(path):
    if sys.platform != 'win32':
        raise NotImplementedError('this function implemented only for win32')
    else:
        parent_path = os.path.dirname(path)
        __win32_deny_path_deletion(path)
        # to prevent user from deleting path,
        # it's required to prevent childs deletion for parent folder
        __win32_deny_childs_deletion(parent_path)


if __name__ == '__main__':
    print(get_if_addresses())

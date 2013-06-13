#!/usr/bin/python
"""
Common logging subsystem initialization code.
"""

#
# Imports
#

from __future__ import absolute_import, print_function
import errno
import glob
import logging
import logging.handlers
import numbers
import os
import re
import sys
import traceback
from collections import OrderedDict
from datetime import timedelta
from functools import partial
from itertools import chain
from stat import ST_DEV, ST_INO, ST_MTIME

try:
    import colorama
except ImportError:
    colorama = None
else:
    colorama.init()

from .build_version import get_build_timestamp
from .utils import coalesce



#
# Module variables/constants
#

LOG_FILE_PATHS = []
ERROR_LOG_FILE_PATHS = []



#
# Classes
#

class WatchedRotatingFileHandler(logging.FileHandler):
    """
    Handler for logging to a set of files, which switches from one file
    to the next when the current file reaches a certain size.

    This handler watches the file
    to see if it has changed while in use. This can happen because of
    usage of programs such as newsyslog and logrotate which perform
    log file rotation. This handler, intended for use under Unix,
    watches the file to see if it has changed since the last emit.
    (A file has changed if its device or inode have changed.)
    If it has changed, the old file stream is closed, and the file
    opened to get a new stream.

    This handler is not appropriate for use under Windows, because
    under Windows open files cannot be moved or renamed - logging
    opens the files with exclusive locks - and so there is no need
    for such a handler. Furthermore, ST_INO is not supported under
    Windows; stat always returns zero for this value.

    This handler based on WatchedFileHandler and RotatingFileHandler
    """

    COMPRESSED_LOG_EXTENSIONS = ['zip', 'gz', 'bz2', 'xz', '7z']

    _LOG_EXTENSIONS_PARSING_RE_STR = \
        '(?:{})'.format('|'.join(COMPRESSED_LOG_EXTENSIONS))

    LOG_SUFFIX_PARSING_RE = \
        re.compile(r'^\.(?P<num>\d+)(?:\.(?P<ext>{exts:s}))?$'.format(
                       exts=_LOG_EXTENSIONS_PARSING_RE_STR))


    def __init__(self, filename, mode='a', maxBytes=0,
                 backupCount=None, encoding=None, delay=0):
        """Open the specified file and use it as the stream for logging.

        By default, the file grows indefinitely. You can specify particular
        values of maxBytes and backupCount to allow the file to rollover at
        a predetermined size.

        Rollover occurs whenever the current log file is nearly maxBytes in
        length. If backupCount is >= 1, the system will successively create
        new files with the same pathname as the base file, but with extensions
        ".1", ".2" etc. appended to it. For example, with a backupCount of 5
        and a base file name of C{app.log}, you would get C{app.log},
        C{app.log.1}, C{app.log.2}, ... through to C{app.log.5}.

        The file being written to is always C{app.log} - when it gets
        filled up, it is closed and renamed to C{app.log.1}, and
        if files C{app.log.1}, C{app.log.2} etc. exist,
        then they are renamed to C{app.log.2}, C{app.log.3} etc. respectively.

        If C{maxBytes} is zero, rollover never occurs.

        @note: latest changes cause not only the log files be rotated, but
            the compressed versions of them as well,
            similarly to the "real" logrotate.
            That is, the files C{app.log.5}, C{app.log.6.bz2}, C{app.log.7.xz}
            will get renamed to C{app.log.6}, C{app.log.7.bz2}, C{app.log.8.xz}
            accordingly.
            The supported extensions are stored
            in C{cls.COMPRESSED_LOG_EXTENSIONS}.
            Also note that the compressed version of the logs are B{never}
            removed automatically.

        @param backupCount: How many backups of log would be stored.
            Makes sense only if C{maxBytes} is not zero.
            if C{None} - unlimited
            if 0 - No backups (when log file reaches limit, it is erased).
        @type backupCount: NoneType, int
        """
        # If rotation/rollover is wanted, it doesn't make sense to use another
        # mode. If for example 'w' were specified, then if there were multiple
        # runs of the calling application, the logs from previous runs would be
        # lost if the 'w' is respected, because the log file would be truncated
        # on each run.
        if maxBytes > 0:
            mode = 'a'
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)

        self.mode = mode
        self.encoding = encoding
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self.dev, self.ino = -1, -1
        self._statstream()


    def _statstream(self):
        if self.stream:
            sres = os.fstat(self.stream.fileno())
            self.dev, self.ino = sres[ST_DEV], sres[ST_INO]


    def __do_rollover(self):
        """Do a rollover, as described in C{__init__()}."""
        cls = self.__class__

        if self.stream:
            self.stream.close()
            self.stream = None

        cls._do_rollover(base_filename=self.baseFilename,
                         backup_count=self.backupCount)

        self.mode = 'w'
        self.stream = self._open()


    @classmethod
    def _do_rollover(cls, base_filename, backup_count):
        r"""Perform rollover of the log (+compressed log) files.

        >>> # Advanced unittests are available in C{__test__} variable.

        @type base_filename: basestring
        @type backup_count: NoneType, numbers.Integral
        """
        base_filename_len = len(base_filename)

        # Q: What looks like a log, but is not a log?
        suspects = cls._glob_all_suspect_logs(base_filename)
        mos = (cls.LOG_SUFFIX_PARSING_RE.match(path[base_filename_len:])
                   for path in suspects)
        # A: ... a file which is like <base_filename>.233.abc,
        # abc is not a supported compressor extension!

        # Groupdict (+base_filename) is enough to get everything
        # about the log file names.
        groupdicts = (mo.groupdict()
                          for mo in mos
                          if mo is not None)

        # Now let's find group all groupdicts by their number; note
        # that theoretically there may be multiple files with the same
        # number
        mos_by_number = {}
        for gd in groupdicts:
            mos_by_number.setdefault(int(gd['num']), []).append(gd)

        if mos_by_number:  # the numbered logs exist and need renaming!
            # Iterate over numbered logs.
            for i in sorted(mos_by_number.iterkeys(), reverse=True):
                for mo in mos_by_number[i]:
                    ext = mo['ext']
                    ext_str = '.' + ext if ext is not None else ''
                    src = u'{:s}.{:d}{:s}'.format(
                              base_filename, i, ext_str)
                    dst = u'{:s}.{:d}{:s}'.format(
                              base_filename, i + 1, ext_str)
                    # EAFP!
                    try:
                        os.remove(dst)
                    except:
                        pass

                    try:
                        # After logrotating, the number of the log (i + 1)
                        # is either still lower than the backup count,
                        # or... or backup count is unlimited.
                        if backup_count is None or i + 1 <= backup_count:
                            os.rename(src, dst)
                        else:
                            os.remove(src)
                    except:
                        # Ignore occasional errors
                        pass

        # Now change the non-numbered base log to .1
        dst = base_filename + '.1'
        # EAFP!
        try:
            os.remove(dst)
        except:
            pass

        try:
            if backup_count is None or 1 <= backup_count:
                os.rename(base_filename, dst)
            else:
                os.remove(base_filename)
        except:
            # Ignore occasional errors
            pass


    @classmethod
    def _find_number_of_backups(cls, base_filename):
        r"""Analyze the FS and find the maximum number of logs available now.

        >>> f = WatchedRotatingFileHandler._find_number_of_backups
        >>> base_filename = '/v/l/app.log'

        >>> import mock
        >>> # Simulate the directory that has both the valid log-looking files
        >>> # (like app.log.1, app.log.42.bz2) and invalid ones
        >>> # (like app.log.117abc.de, app.log.12.abc).

        >>> # default_dir_contents1 - non-compressed logs are higher
        >>> default_dir_contents1 = frozenset([
        ...     '/v/l/1.txt', '/v/l/123', '/v/l/app.log',
        ...     '/v/l/app.log.1', '/v/l/app.log.2',
        ...     '/v/l/app.log.40', '/v/l/app.log.84',
        ...     '/v/l/app.log.117abc.de', '/v/l/app.log.122.abc',
        ...     '/v/l/app.log.42.bz2', '/v/l/app.log.43.bz2'
        ... ])
        >>> # default_dir_contents2 - compressed logs are higher
        >>> default_dir_contents2 = frozenset([
        ...     '/v/l/1.txt', '/v/l/123', '/v/l/app.log',
        ...     '/v/l/app.log.1', '/v/l/app.log.2',
        ...     '/v/l/app.log.40', '/v/l/app.log.41',
        ...     '/v/l/app.log.117abc.de', '/v/l/app.log.122.abc',
        ...     '/v/l/app.log.42.bz2', '/v/l/app.log.43.bz2'
        ... ])

        >>> def new_glob(pattern):
        ...     from fnmatch import fnmatch
        ...     print(u'glob.glob({!r})'.format(pattern))
        ...     return [f for f in dir_contents if fnmatch(f, pattern)]

        >>> # Test 1: non-compressed logs are higher.
        >>> dir_contents = set(default_dir_contents1)
        >>> with mock.patch('glob.glob', new_callable=lambda: new_glob):
        ...     f(base_filename)
        glob.glob('/v/l/app.log.[0-9]*')
        glob.glob('/v/l/app.log.[0-9]*.*')
        85

        >>> # Test 2: compressed logs are higher.
        >>> dir_contents = set(default_dir_contents2)
        >>> with mock.patch('glob.glob', new_callable=lambda: new_glob):
        ...     f(base_filename)
        glob.glob('/v/l/app.log.[0-9]*')
        glob.glob('/v/l/app.log.[0-9]*.*')
        44
        """
        max_num = 0
        try:
            base_filename_len = len(base_filename)

            # We pre-validate the log files using glob, but then
            # we validate them properly using regex.
            for backup_path in cls._glob_all_suspect_logs(base_filename):
                assert backup_path.startswith(base_filename), \
                       (backup_path, base_filename)
                suffix = backup_path[base_filename_len:]

                mo = cls.LOG_SUFFIX_PARSING_RE.match(suffix)
                if mo is not None:
                    groups = mo.groupdict()
                    backup_num = int(groups['num'])

                    max_num = max(max_num, backup_num)
        except:
            try:
                print('Critical problem with rotating!')
                traceback.print_exc()
            except:
                # Too bad, we can't even print out the problem
                pass

        return max_num + 1


    @classmethod
    def _glob_all_suspect_logs(cls, base_filename):
        r"""Return all filenames which look suspiciously like logs.

        Though, in fact, they may B{not} be logs, and their filenames require
        additional processing with the regex.

        >>> f = WatchedRotatingFileHandler._glob_all_suspect_logs
        >>> base_filename = '/v/l/app.log'

        >>> # Simulate the directory that has both the valid log-looking files
        >>> # (like app.log.1, app.log.42.bz2) and invalid ones
        >>> # (like app.log.117abc.de, app.log.12.abc).

        >>> import mock

        >>> dir_contents = frozenset([
        ...     '/v/l/1.txt', '/v/l/123', '/v/l/app.log',
        ...     '/v/l/app.log.1', '/v/l/app.log.2',
        ...     '/v/l/app.log.40', '/v/l/app.log.84',
        ...     '/v/l/app.log.117abc.de', '/v/l/app.log.122.abc',
        ...     '/v/l/app.log.4.gz', '/v/l/app.log.5.gz',
        ...     '/v/l/app.log.42.bz2', '/v/l/app.log.43.bz2'
        ... ])

        >>> def new_glob(pattern):
        ...     from fnmatch import fnmatch
        ...     print(u'glob.glob({!r})'.format(pattern))
        ...     return [f for f in dir_contents if fnmatch(f, pattern)]

        >>> with mock.patch('glob.glob', new_callable=lambda: new_glob):
        ...     list(f(base_filename))  # doctest:+NORMALIZE_WHITESPACE
        glob.glob('/v/l/app.log.[0-9]*')
        glob.glob('/v/l/app.log.[0-9]*.*')
        ['/v/l/app.log.4.gz', '/v/l/app.log.40', '/v/l/app.log.2',
         '/v/l/app.log.1', '/v/l/app.log.122.abc', '/v/l/app.log.117abc.de',
         '/v/l/app.log.43.bz2', '/v/l/app.log.42.bz2', '/v/l/app.log.84',
         '/v/l/app.log.5.gz']

        @rtype: col.Iterable
        """
        # There should not be dupes! So we have to frozenset'it.
        return frozenset(glob.glob(base_filename + '.[0-9]*') +
                         glob.glob(base_filename + '.[0-9]*.*'))


    def __should_rollover(self, record):
        """Determine if rollover should occur.

        Basically, see if the supplied record would cause the file to exceed
        the size limit we have.
        """
        if self.stream is None:  # delay was set...
            self.stream = self._open()
        if self.maxBytes > 0:  # are we rolling over?
            msg = '%s\n' % self.format(record)
            self.stream.seek(0, 2) # due to non-posix-compliant Windows feature
            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1
        return 0


    def emit(self, record):
        """Emit a record.

        First check if the underlying file has changed, and if it
        has, close the old stream and reopen the file to get the
        current stream.

        Output the record to the file, catering for rollover as described
        in C{do_rollover()}.
        """

        # Reduce the chance of race conditions by stat'ing by path only
        # once and then fstat'ing our new fd if we opened a new log stream.
        # See issue #14632: Thanks to John Mulligan for the problem report
        # and patch.
        try:

            try:
                # stat the file by path, checking for existence
                sres = os.stat(self.baseFilename)
            except OSError as err:
                if err.errno == errno.ENOENT:
                    sres = None
                else:
                    raise
            # compare file system stat with that of our stream file handle
            if (not sres or
                sres[ST_DEV] != self.dev or
                sres[ST_INO] != self.ino):
                # Second check here...
                if self.stream is not None:
                    # we have an open file handle, clean it up
                    self.stream.flush()
                    self.stream.close()
                    # open a new file handle and get new stat info from that fd
                    self.stream = self._open()
                    self._statstream()

            if self.__should_rollover(record):
                self.__do_rollover()

            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)



#
# Functions
#

__old_stdout_handler = None
__old_file_handler = None
__old_file_error_handler = None



def add_verbose_level():
    """Create a new logger level, 'VERBOSE'."""
    logging.VERBOSE = 9
    logging.addLevelName(logging.VERBOSE, 'VERBOSE')
    logging.Logger.verbose = \
        lambda self, *args: self.log(logging.VERBOSE, *args)



class ColoredFormatter(logging.Formatter):
    """
    The formatter that adds colors to the error levels.
    """

    def __init__(self, fmt, datefmt=None):
        """Constructor."""
        assert colorama is not None

        fmt = fmt.replace(
                  '%(levelname)s',
                  '%(_color_levelname)s%(levelname)s%(_color_reset)s')

        fmt = fmt.replace(
                  '%(name)s',
                  '%(_color_name)s%(name)s%(_color_reset)s')

        fmt = fmt.replace(
                  '%(process)d',
                  '%(_color_process)s%(process)d%(_color_reset)s')

        fmt = fmt.replace(
                  '%(thread)x',
                  '%(_color_thread)s%(thread)x%(_color_reset)s')

        super(ColoredFormatter, self).__init__(fmt, datefmt)

        fore = colorama.Fore
        back = colorama.Back
        style = colorama.Style

        self._colors = OrderedDict([
            (logging.CRITICAL, fore.RED + style.BRIGHT),
            (logging.ERROR, fore.RED + style.NORMAL),
            (logging.WARNING, fore.YELLOW + style.BRIGHT),
            (logging.INFO, fore.GREEN + style.BRIGHT),
            (logging.DEBUG, fore.WHITE + style.BRIGHT),
            (logging.VERBOSE, fore.WHITE + style.DIM)
        ])


    def _calculate_color_for_level(self, levelno):
        """
        Given a levelno, calculate the color which should be used
        for this level.
        """
        for k, v in self._colors.iteritems():
            if levelno >= k:
                return v

        # Default value
        return self._colors[logging.VERBOSE]


    def format(self, record):
        """Overrides the implementation from C{logging.Formatter}."""
        fore = colorama.Fore
        back = colorama.Back
        style = colorama.Style

        record._color_levelname = self._calculate_color_for_level(
                                      record.levelno)
        record._color_name = fore.CYAN + style.DIM
        record._color_process = fore.BLUE + style.BRIGHT
        record._color_thread = fore.BLUE + style.BRIGHT
        record._color_reset = colorama.Style.RESET_ALL

        return super(ColoredFormatter, self).format(record)


def preferred_file_handler_class(limit=None, backup_count=None,
                                 __sys_platform_override=None):
    r"""Get a preferred class to be used for file log handling.

    >>> # Smoke test 1
    >>> preferred_file_handler_class(__sys_platform_override='linux2') \
    ... # doctest:+ELLIPSIS
    <functools.partial object at 0x...>

    >>> # Smoke test 2
    >>> preferred_file_handler_class(limit=12345,
    ...                              __sys_platform_override='linux2') \
    ... # doctest:+ELLIPSIS
    <functools.partial object at 0x...>

    >>> # Smoke test 3
    >>> preferred_file_handler_class(limit=123456789L,
    ...                              __sys_platform_override='linux2') \
    ... # doctest:+ELLIPSIS
    <functools.partial object at 0x...>

    >>> # Error combination
    >>> preferred_file_handler_class(limit=12345, backup_count=None,
    ...                              __sys_platform_override='win32')
    Traceback (most recent call last):
      ...
    ValueError: RotatingFileHandler can not create unlimited backups!

    @param limit: the limit of log size (in bytes).
        See comment in C{configure_logging} for more details.
    @type limit: NoneType, numbers.Integral

    @param backup_count: how many backups of log would be stored.
        See comment in C{configure_logging} for more details.
    @type backup_count: NoneType, numbers.Integral

    @raises ValueError: if the combination of OS and C{limit}/C{backup_count}
        is not supported.

    @rtype: logging.FileHandler
    """
    encoding = None

    sys_platform = coalesce(__sys_platform_override, sys.platform)

    if limit is None:
        if sys_platform == 'win32':
            file_handler_class = \
                partial(logging.FileHandler,
                        delay=True, encoding=encoding)
        else:
            file_handler_class = \
                partial(logging.handlers.WatchedFileHandler,
                        delay=True, encoding=encoding)
    elif isinstance(limit, numbers.Integral):
        if sys_platform == 'win32':
            if backup_count is None:
                raise ValueError('RotatingFileHandler can not '
                                 'create unlimited backups!')
            file_handler_class = \
                partial(logging.handlers.RotatingFileHandler,
                        delay=True, encoding=encoding,
                        maxBytes=limit,
                        backupCount=backup_count)
        else:
            file_handler_class = \
                partial(WatchedRotatingFileHandler,
                        delay=True, encoding=encoding,
                        maxBytes=limit,
                        backupCount=backup_count)
    else:
        assert False, repr(limit)

    return file_handler_class


def configure_logging(dir_path, prefix, postfix=None,
                      log_level_stdout_override=None,
                      log_level_file_override=None,
                      debug_to_stdout=False,
                      limit=None, backup_count=None):
    """Initialize the whole logging subsystem.

    @param dir_path: The directory path for the log files.
    @type dir_path: str

    @param prefix: The prefix for the log files.
    @type prefix: str

    @param postfix: The postfix for the log files.
    @type postfix: str

    @param limit: the limit of log size (in bytes).
        Affects the File Handler used for the log:
        if isinstance(limit, numbers.Integral) and platform == 'win32':
            RotatingFileHandler
        if isinstance(limit, numbers.Integral) and platform != 'win32':
            WatchedRotatingFileHandler
        if isinstance(limit, NoneType) and platform == 'win32':
            logging.FileHandler
        if isinstance(limit, NoneType) and platform != 'win32':
            logging.WatchedFileHandler
    @type limit: NoneType, numbers.Integral

    @param backup_count: how many backups of log would be stored.
        Makes sense only if C{limit} is not None.
                         if None - unlimited
                         if 0 - No backups (when log file reaches limit,
                                            it is erased).
    @type backup_count: NoneType, numbers.Integral

    @raises ValueError: from C{preferred_file_handler_class}
    """
    add_verbose_level()

    # Note: format strings should NOT be Unicode ones!
    # Otherwise the %-interpolation may fail trying to interpolate b''-strings
    # containing some special symbols.
    stdout_format_str = \
        '(%(process)d/%(thread)x) [%(levelname)s] %(name)s: %(message)s'
    file_format_str = \
        '%(asctime)s (%(process)d/%(thread)x)'\
            ' {ts}'\
            ' [%(levelname)s]'\
            ' %(name)s::%(funcName)s'\
            ' - %(message)s'\
            .format(ts=get_build_timestamp())

    file_handler_class = preferred_file_handler_class(
                             limit=limit, backup_count=backup_count)

    formatter_class = ColoredFormatter if colorama is not None \
                                       else logging.Formatter
    formatter_stdout = formatter_class(stdout_format_str, None)
    formatter_file = formatter_error_file = logging.Formatter(file_format_str)

    if postfix is None:
        log_filename = '{}.log'.format(prefix)
        log_error_filename = '{}-error.log'.format(prefix)
    else:
        log_filename = '{}-{}.log'.format(prefix, postfix)
        log_error_filename = '{}-{}-error.log'.format(prefix, postfix)

    log_filename = os.path.join(dir_path, log_filename)
    log_error_filename = os.path.join(dir_path, log_error_filename)

    # Some initial assumptions what are the minimal levels to log
    # for every possible log destination.
    # If need to change it for the development, use host_magic.py!
    if __debug__:
        log_level_stdout = logging.DEBUG
        log_level_file = logging.DEBUG
    else:
        log_level_stdout = logging.WARNING
        log_level_file = logging.DEBUG  # in release, must become WARNING

    # But if we have the overrides defined, let's use them instead
    log_level_stdout = coalesce(log_level_stdout_override, log_level_stdout)
    log_level_file = coalesce(log_level_file_override, log_level_file)
    log_level_error = logging.ERROR
    main_level = min(log_level_stdout, log_level_file, log_level_error)

    global LOG_FILE_PATHS
    global ERROR_LOG_FILE_PATHS
    LOG_FILE_PATHS = [log_error_filename, log_filename]
    ERROR_LOG_FILE_PATHS = [log_error_filename]

    # Print all debug information to console...
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(log_level_stdout)
    stdout_handler.setFormatter(formatter_stdout)

    global __old_stdout_handler, __old_file_handler, __old_file_error_handler

    # ... and to the file as well
    file_handler = file_handler_class(log_filename)
    file_handler.setLevel(log_level_file)
    file_handler.setFormatter(formatter_file)

    # Special error-only log, which is always generated
    # if any ERROR-level logs occur.
    file_error_handler = file_handler_class(log_error_filename)
    file_error_handler.setLevel(log_level_error)
    file_error_handler.setFormatter(formatter_error_file)

    root_logger = logging.getLogger()

    # Bind the STDOUT handler, and unbind the previous one if any
    if debug_to_stdout:
        if __old_stdout_handler is not None:
            root_logger.removeHandler(__old_stdout_handler)
        root_logger.addHandler(stdout_handler)

    # Bind the file-out handler, and unbind the previous one if any
    if __old_file_handler is not None:
        root_logger.removeHandler(__old_file_handler)
    root_logger.addHandler(file_handler)

    # Bind the error file-out handler, and unbind the previous one if any
    if __old_file_error_handler is not None:
        root_logger.removeHandler(__old_file_error_handler)
    root_logger.addHandler(file_error_handler)

    (__old_stdout_handler,
     __old_file_handler,
     __old_file_error_handler) = (stdout_handler,
                                  file_handler,
                                  file_error_handler)

    root_logger.setLevel(main_level)



#
# Doctests
#

if __debug__:
    __test__ = {
        'WatchedRotatingFileHandler._do_rollover':
            r"""
            >>> f = WatchedRotatingFileHandler._do_rollover
            >>> base_filename = '/v/l/app.log'

            >>> # Simulate the directory that has both
            >>> # the valid log-looking files
            >>> # (like app.log.1, app.log.42.bz2)
            >>> # and invalid ones
            >>> # (like app.log.117abc.de, app.log.12.abc).

            >>> import mock

            >>> # default_dir_contents1 - non-compressed logs are higher
            >>> default_dir_contents1 = frozenset([
            ...     '/v/l/1.txt', '/v/l/123', '/v/l/app.log',
            ...     '/v/l/app.log.1', '/v/l/app.log.2',
            ...     '/v/l/app.log.40', '/v/l/app.log.84',
            ...     '/v/l/app.log.117abc.de', '/v/l/app.log.122.abc',
            ...     '/v/l/app.log.4.gz', '/v/l/app.log.5.gz',
            ...     '/v/l/app.log.42.bz2', '/v/l/app.log.43.bz2'
            ... ])
            >>> # default_dir_contents2 - compressed logs are higher
            >>> default_dir_contents2 = frozenset([
            ...     '/v/l/1.txt', '/v/l/123', '/v/l/app.log',
            ...     '/v/l/app.log.1', '/v/l/app.log.2',
            ...     '/v/l/app.log.40', '/v/l/app.log.41',
            ...     '/v/l/app.log.117abc.de', '/v/l/app.log.122.abc',
            ...     '/v/l/app.log.4.gz', '/v/l/app.log.5.gz',
            ...     '/v/l/app.log.42.bz2', '/v/l/app.log.43.bz2'
            ... ])

            >>> new_os_path_exists = lambda path: path in dir_contents

            >>> def new_os_rename(p1, p2):
            ...     assert p1 in dir_contents
            ...     assert p2 not in dir_contents
            ...     dir_contents.add(p2)
            ...     dir_contents.remove(p1)
            ...     print(u'os.rename({!r}, {!r})'.format(p1, p2))

            >>> def new_os_remove(path):
            ...     assert path in dir_contents
            ...     dir_contents.remove(path)
            ...     print(u'os.remove({!r})'.format(path))

            >>> def new_glob(pattern):
            ...     from fnmatch import fnmatch
            ...     print(u'glob.glob({!r})'.format(pattern))
            ...     return [f for f in dir_contents if fnmatch(f, pattern)]

            >>> # Test 1a: non-compressed logs are higher; unlimited log count.
            >>> dir_contents = set(default_dir_contents1)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...     f(base_filename, None)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.rename(u'/v/l/app.log.84', u'/v/l/app.log.85')
            os.rename(u'/v/l/app.log.43.bz2', u'/v/l/app.log.44.bz2')
            os.rename(u'/v/l/app.log.42.bz2', u'/v/l/app.log.43.bz2')
            os.rename(u'/v/l/app.log.40', u'/v/l/app.log.41')
            os.rename(u'/v/l/app.log.5.gz', u'/v/l/app.log.6.gz')
            os.rename(u'/v/l/app.log.4.gz', u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.2', u'/v/l/app.log.3')
            os.rename(u'/v/l/app.log.1', u'/v/l/app.log.2')
            os.rename('/v/l/app.log', '/v/l/app.log.1')

            >>> # Test 1b: non-compressed logs are higher; log count limit 15.
            >>> dir_contents = set(default_dir_contents1)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...      f(base_filename, 15)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.remove(u'/v/l/app.log.84')
            os.remove(u'/v/l/app.log.43.bz2')
            os.remove(u'/v/l/app.log.42.bz2')
            os.remove(u'/v/l/app.log.40')
            os.rename(u'/v/l/app.log.5.gz', u'/v/l/app.log.6.gz')
            os.rename(u'/v/l/app.log.4.gz', u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.2', u'/v/l/app.log.3')
            os.rename(u'/v/l/app.log.1', u'/v/l/app.log.2')
            os.rename('/v/l/app.log', '/v/l/app.log.1')

            >>> # Test 1c: non-compressed logs are higher; log count limit 5.
            >>> dir_contents = set(default_dir_contents1)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...      f(base_filename, 5)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.remove(u'/v/l/app.log.84')
            os.remove(u'/v/l/app.log.43.bz2')
            os.remove(u'/v/l/app.log.42.bz2')
            os.remove(u'/v/l/app.log.40')
            os.remove(u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.4.gz', u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.2', u'/v/l/app.log.3')
            os.rename(u'/v/l/app.log.1', u'/v/l/app.log.2')
            os.rename('/v/l/app.log', '/v/l/app.log.1')

            >>> # Test 1d: non-compressed logs are higher; log count limit 1.
            >>> dir_contents = set(default_dir_contents1)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...      f(base_filename, 1)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.remove(u'/v/l/app.log.84')
            os.remove(u'/v/l/app.log.43.bz2')
            os.remove(u'/v/l/app.log.42.bz2')
            os.remove(u'/v/l/app.log.40')
            os.remove(u'/v/l/app.log.5.gz')
            os.remove(u'/v/l/app.log.4.gz')
            os.remove(u'/v/l/app.log.2')
            os.remove(u'/v/l/app.log.1')
            os.rename('/v/l/app.log', '/v/l/app.log.1')

            >>> # Test 1e: non-compressed logs are higher; log count limit 0.
            >>> dir_contents = set(default_dir_contents1)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...      f(base_filename, 0)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.remove(u'/v/l/app.log.84')
            os.remove(u'/v/l/app.log.43.bz2')
            os.remove(u'/v/l/app.log.42.bz2')
            os.remove(u'/v/l/app.log.40')
            os.remove(u'/v/l/app.log.5.gz')
            os.remove(u'/v/l/app.log.4.gz')
            os.remove(u'/v/l/app.log.2')
            os.remove(u'/v/l/app.log.1')
            os.remove('/v/l/app.log')

            >>> # Test 2a: compressed logs are higher; log count limit 15.
            >>> dir_contents = set(default_dir_contents2)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...     f(base_filename, 15)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.remove(u'/v/l/app.log.43.bz2')
            os.remove(u'/v/l/app.log.42.bz2')
            os.remove(u'/v/l/app.log.41')
            os.remove(u'/v/l/app.log.40')
            os.rename(u'/v/l/app.log.5.gz', u'/v/l/app.log.6.gz')
            os.rename(u'/v/l/app.log.4.gz', u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.2', u'/v/l/app.log.3')
            os.rename(u'/v/l/app.log.1', u'/v/l/app.log.2')
            os.rename('/v/l/app.log', '/v/l/app.log.1')

            >>> # Test 2b: compressed logs are higher; unlimited log count.
            >>> dir_contents = set(default_dir_contents2)
            >>> with mock.patch('glob.glob', new_callable=lambda: new_glob), \
            ...      mock.patch('os.path.exists',
            ...                 new_callable=lambda: new_os_path_exists), \
            ...      mock.patch('os.rename',
            ...                 new_callable=lambda: new_os_rename), \
            ...      mock.patch('os.remove',
            ...                 new_callable=lambda: new_os_remove):
            ...
            ...     f(base_filename, None)
            glob.glob('/v/l/app.log.[0-9]*')
            glob.glob('/v/l/app.log.[0-9]*.*')
            os.rename(u'/v/l/app.log.43.bz2', u'/v/l/app.log.44.bz2')
            os.rename(u'/v/l/app.log.42.bz2', u'/v/l/app.log.43.bz2')
            os.rename(u'/v/l/app.log.41', u'/v/l/app.log.42')
            os.rename(u'/v/l/app.log.40', u'/v/l/app.log.41')
            os.rename(u'/v/l/app.log.5.gz', u'/v/l/app.log.6.gz')
            os.rename(u'/v/l/app.log.4.gz', u'/v/l/app.log.5.gz')
            os.rename(u'/v/l/app.log.2', u'/v/l/app.log.3')
            os.rename(u'/v/l/app.log.1', u'/v/l/app.log.2')
            os.rename('/v/l/app.log', '/v/l/app.log.1')
            """,
    }

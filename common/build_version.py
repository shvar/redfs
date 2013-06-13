#!/usr/bin/python
"""
System release build versions.

When used as a separate executable, allows to get the datetime
of the latest "tip" changeset in the Mercurial repository,
in a special compact timestamp format. In this case, it accepts
the path to the repository as the sole argument.

When using as a module, also allows to fallback to the version
stored in the file inside the primary redistribution,
as there is no .hg repository in the real world installation.
"""

#
# Imports
#

from __future__ import absolute_import

import os
from datetime import datetime, timedelta
from calendar import timegm

from .settings import conf_dir


#
# Constants
#

__VERSION_FILE_NAME = 'build_version'



#
# Functions
#

def _dt_from_timetuple(timetuple):
    r"""Get a datetime from Mercurial-style timetuple.

    >>> _dt_from_timetuple((1354055308.0, -14400))
    datetime.datetime(2012, 11, 27, 22, 28, 28)

    @note: should not be changed to C{datetime.utcfromtimestamp}!

    @type timetuple: tuple
    @rtype: datetime
    """
    return datetime.fromtimestamp(timetuple[0]) \
           + timedelta(seconds=timetuple[1])


def get_build_timestamp(repo_path=None, fallback_to_file=True):
    """Get build timestamp of the current release.

    @param repo_path: The path to the root of repository.
        If C{None}, the repository will be guessed from the current project.
    @type repo_path: basestring, NoneType

    @param fallback_to_file: Whether we need to read the release version
                             from the special file, if we cannot properly
                             access the Mercurial repository.

    @rtype: str
    @postcondition: len(result) == 8 and int(result, 16)

    @raises Exception: If the version file could no be parsed.
    """
    if repo_path is None:
        repo_path = os.path.abspath(os.path.join(__file__, '../../..'))

    try:
        from mercurial import hg, ui

        _ui = ui.ui()
        _repo = hg.repository(_ui, repo_path)
        working_ctx = _repo[None]
        parent_ctxes = working_ctx.parents()
        num_parent_ctxes = len(parent_ctxes)
        if num_parent_ctxes < 1:
            raise Exception(u'No parents for {!r}'.format(working_ctx))
        else:
            # There are maybe multiple parent CTXes.
            # Get the latest timestamp among them.
            max_dt = max(_dt_from_timetuple(p.date())
                             for p in parent_ctxes)

            # should not be changed to datetime.utcfromtimestamp!
            return dt_to_timestamp(max_dt)

    except Exception:
        if fallback_to_file:
            _path = os.path.abspath(os.path.join(conf_dir(),
                                                 __VERSION_FILE_NAME))
            with open(_path) as version_file:
                data = ''.join(version_file).strip()
                # Make sure it contains the valid contents
                try:
                    int(data, 16)  # ignore the result
                except ValueError:
                    # Re-raise it but do nothing here
                    raise

                return data
        else:
            raise


def dt_to_timestamp(dt):
    """Convert a (UTC) datetime to a timestamp used in build numbers.

    >>> dt_to_timestamp(datetime(2012, 11, 27, 22, 28, 28))
    '50b53e8c'

    @type dt: datetime
    @rtype: basestring
    """
    return '{0:08x}'.format(timegm(dt.timetuple()))


def timestamp_to_utc_time(ts):
    r"""Given a timestamp, get the proper UTC datetime.

    >>> timestamp_to_utc_time('50b53e8c')
    datetime.datetime(2012, 11, 27, 22, 28, 28)

    @type ts: str
    @precondition: len(ts) == 8 and int(ts, 16)

    @rtype: datetime
    """
    return datetime.utcfromtimestamp(int(ts, 16))


def timestamp_to_local_time(ts):
    r"""Given a timestamp, get the proper local datetime.

    @type ts: str
    @precondition: len(ts) == 8 and int(ts, 16)
    """
    # should not be changed to datetime.utcfromtimestamp
    return datetime.fromtimestamp(int(ts, 16))

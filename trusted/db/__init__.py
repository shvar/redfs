#!/usr/bin/python
"""Node-specific database access layer.

@var RDB: A database-access wrapper factory.
@type RDB: DatabaseWrapperFactory
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import gc
import json
import logging
import numbers
import posixpath
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from functools import partial
from itertools import chain, ifilter, imap
from operator import attrgetter
from textwrap import dedent as _dd
from types import NoneType
from uuid import UUID

from sqlalchemy import func, literal_column, and_, or_, asc, desc
from sqlalchemy.sql import functions as sql_functions
from sqlalchemy.sql.expression import (
    select as sql_select, update as sql_update,
    insert as sql_insert, delete as sql_delete,
    true as sqla_true, false as sqla_false,
    bindparam, over, union)

from contrib.dbc import consists_of, contract_epydoc

from common import data_operations as common_data_ops
from common.chunks import Chunk, ChunkInfo, LocalPhysicalFileState
from common.crypto import Fingerprint
from common.datatypes import ProgressNotificationPerHost, TrustedHostCaps
from common.datetime_ex import TimeEx, TimeDeltaEx
from common.db import (DatabaseWrapperFactory, DatabaseWrapperSQLAlchemy,
                       Queries, create_db_schema, models as cmodels)
from common.db.types import crc32_to_signed, crc32_to_unsigned
from common.db.utils import insert_from_select
from common.path_ex import decode_posix_path, encode_posix_path
from common.inhabitants import TrustedHostAtNode, HostAtNode
from common.itertools_ex import groupby_to_dict, inonempty
from common.typed_uuids import ChunkUUID, DatasetUUID, HostUUID, UserGroupUUID
from common.utils import (
    coalesce, duration_logged, exceptions_logged, gen_uuid, memoized,
    td_to_sec, td_to_ms, ms_to_td, time_to_ms, ms_to_time, fix_strptime)

from . import models



#
# Logging
#

logger = logging.getLogger(__name__)



#
# Variables/constants
#

RDB = None

STAT_TIME_QUANT = TimeDeltaEx(minutes=15)
assert not timedelta(days=1) % STAT_TIME_QUANT, \
       'STAT_TIME_QUANT = {!r}'.format(timedelta(days=1) % STAT_TIME_QUANT)



#
# Functions
#

@contract_epydoc
def get_prev_time_quant(dt):
    """
    For a given datetime, find the datetime of the previous "time quant"
    that can be stored in the DB.
    If the datetime is the time of time quant start itself,
    it is returned itself.

    >>> get_prev_time_quant(datetime(2012, 1, 1, 0, 0))
    datetime.datetime(2012, 1, 1, 0, 0)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 1, 1))
    datetime.datetime(2012, 1, 15, 1, 0)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 1, 50))
    datetime.datetime(2012, 1, 15, 1, 0)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 7, 32))
    datetime.datetime(2012, 1, 15, 1, 0)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 16, 32))
    datetime.datetime(2012, 1, 15, 1, 15)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 43, 17))
    datetime.datetime(2012, 1, 15, 1, 30)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 44, 59))
    datetime.datetime(2012, 1, 15, 1, 30)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 45))
    datetime.datetime(2012, 1, 15, 1, 45)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 45, 6))
    datetime.datetime(2012, 1, 15, 1, 45)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 1, 59, 59))
    datetime.datetime(2012, 1, 15, 1, 45)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 2, 0))
    datetime.datetime(2012, 1, 15, 2, 0)
    >>> get_prev_time_quant(datetime(2012, 1, 15, 23, 59, 59))
    datetime.datetime(2012, 1, 15, 23, 45)

    @type dt: datetime

    @return: The time (UTC) when the previous time quant
             (before the given time stamp) was started.
    @rtype: datetime
    @postcondition: timedelta(0) <= dt - result < STAT_TIME_QUANT
    """
    time_from_midnight = TimeEx.from_time(dt.time())
    td_from_midnight = \
        TimeDeltaEx.from_microseconds(time_from_midnight.in_microseconds())

    td_last_time_quant_begin = \
        td_from_midnight // STAT_TIME_QUANT * STAT_TIME_QUANT
    last_time_quant_begin = \
        TimeEx.from_microseconds(td_last_time_quant_begin.in_microseconds())

    return dt.replace(hour=last_time_quant_begin.hour,
                      minute=last_time_quant_begin.minute,
                      second=last_time_quant_begin.second,
                      microsecond=last_time_quant_begin.microsecond)


@contract_epydoc
def get_next_time_quant(dt):
    """
    For a given datetime, find the datetime of the next "time quant"
    that can be stored in the DB.

    If the datetime is the time of time quant start itself,
    the next time quant is returned rather than the datetime itself.

    >>> get_next_time_quant(datetime(2012, 1, 1, 0, 0))
    datetime.datetime(2012, 1, 1, 0, 15)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 1, 1))
    datetime.datetime(2012, 1, 15, 1, 15)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 1, 50))
    datetime.datetime(2012, 1, 15, 1, 15)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 7, 32))
    datetime.datetime(2012, 1, 15, 1, 15)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 16, 32))
    datetime.datetime(2012, 1, 15, 1, 30)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 43, 17))
    datetime.datetime(2012, 1, 15, 1, 45)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 44, 59))
    datetime.datetime(2012, 1, 15, 1, 45)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 45))
    datetime.datetime(2012, 1, 15, 2, 0)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 45, 6))
    datetime.datetime(2012, 1, 15, 2, 0)
    >>> get_next_time_quant(datetime(2012, 1, 15, 1, 59, 59))
    datetime.datetime(2012, 1, 15, 2, 0)
    >>> get_next_time_quant(datetime(2012, 1, 15, 2, 0))
    datetime.datetime(2012, 1, 15, 2, 15)
    >>> get_next_time_quant(datetime(2012, 1, 15, 23, 59, 59))
    datetime.datetime(2012, 1, 16, 0, 0)

    @type dt: datetime

    @return: The time (UTC) when the next time quant
             (after the given time stamp) will be started.
    @rtype: datetime
    @postcondition: timedelta(0) < result - dt <= STAT_TIME_QUANT
    """
    return get_prev_time_quant(dt) + STAT_TIME_QUANT



#
# Classes
#

class TrustedQueries(Queries):
    """A dummy namespace for all the common trusted-side-specific queries.

    @note: you do not ever need to instance this class.
    """

    __slots__ = tuple()


    class TrustedSettings(Queries.Settings):
        """All DB (node-only) queries related to the various host settings."""

        __slots__ = tuple()


        SELECT_HOST_SETTING = _dd("""\
            SELECT update_time, value
                FROM setting_view
                WHERE host_uuid = :host_uuid AND name = :name
            """)

        # Arguments:
        # :setting_name
        # :host_uuid
        # :lu_time (last update time)
        # :setting_value
        INSERT_SETTING = _dd("""\
            INSERT INTO setting(
                    setting_name_id,
                    host_id,
                    update_time,
                    value
                )
                VALUES
                (
                    (SELECT id FROM setting_name WHERE name = :setting_name),
                    (SELECT id FROM host_view WHERE uuid = :host_uuid),
                    :lu_time,
                    :setting_value
                )
            """)

        UPDATE_SETTING = _dd("""\
            UPDATE setting
                SET
                    update_time = :lu_time,
                    value = :setting_value
                WHERE
                    setting_name_id = (SELECT id
                                           FROM setting_name
                                           WHERE name = :setting_name) AND
                    host_id = (SELECT id
                                   FROM host_view
                                   WHERE uuid = :host_uuid)
            """)

        @classmethod
        @contract_epydoc
        def set_setting(cls,
                        rdbw,
                        host_uuid, setting_name, setting_value, setting_time,
                        direct=False):
            """
            Set the setting, no matter was any value present
            or absent for this setting before.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @type host_uuid: UUID

            @type setting_name: basestring
            @precondition: setting_name in Queries.Settings.ALL_SETTINGS

            @type setting_time: (NoneType, datetime)

            @param direct: whether the value should be written
                           without the conversion.
            @type direct: bool
            """
            base = Queries.Settings

            _time = setting_time if setting_time is not None \
                                 else datetime.utcnow()
            _value = setting_value if direct \
                                   else base._encode(setting_name,
                                                     setting_value)

            existing_row = rdbw.query_value(cls.SELECT_HOST_SETTING,
                                            {'host_uuid': host_uuid,
                                             'name': setting_name})
            # INSERT if it doesn't exist, UPDATE otherwise
            if existing_row is None:
                rdbw.query(cls.INSERT_SETTING,
                           {'setting_name': setting_name,
                            'host_uuid': host_uuid,
                            'lu_time': _time,
                            'setting_value': _value})
            else:
                # The row exists; but if its update time is newer,
                # we should not overwrite it.
                if _time > existing_row.update_time:
                    rdbw.query(cls.UPDATE_SETTING,
                               {'setting_name': setting_name,
                                'host_uuid': host_uuid,
                                'lu_time': _time,
                                'setting_value': _value})


        @classmethod
        @contract_epydoc
        def update_host_settings_directly(cls, host_uuid, settings, dt):
            """
            Given an inhabitant UUID, and the settings map
            (name to value/last update time), update the settings in the DB.

            Note that the values in the settings should be already "raw".

            @type host_uuid: UUID
            @type settings: dict
            @type dt: datetime
            @precondition: all(name in Queries.Settings.ALL_SETTINGS
                                   for name in settings.iterkeys())
            """
            assert RDB is not None

            with RDB() as rdbw:
                for name, value in settings.iteritems():
                    cls.set_setting(rdbw,
                                    host_uuid, name, value, dt,
                                    direct=True)


        @classmethod
        @contract_epydoc
        def get_setting_or_default(cls, rdbw, setting_name, host_uuid):
            """
            Given a host uuid and a setting name, return its value
            (or a default value instead).

            @todo: use parent .get() method

            @note: The value of the setting is decoded to the proper type
                   if needed.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @type host_uuid: UUID
            """
            base = Queries.Settings

            row = rdbw.query_value(cls.SELECT_HOST_SETTING,
                                   {'host_uuid': host_uuid,
                                    'name': setting_name})

            return base._decode(setting_name, row.value) \
                       if row is not None \
                       else Queries.Settings.get_default(setting_name)


        # Arguments:
        # :name - setting name.
        SELECT_SETTING_FOR_ALL_HOSTS = _dd("""\
            SELECT host_uuid, value
                FROM setting_view
                WHERE name = :name
            """)

        @classmethod
        @contract_epydoc
        def get_for_all_hosts(cls, setting_name):
            """
            Given a setting name, return the map of the host UUIDs
            to the value of this setting, whenever exist.

            @type setting_name: str
            @precondition: setting_name in Queries.Settings.ALL_SETTINGS
            """
            base = Queries.Settings

            assert RDB is not None

            with RDB() as rdbw:
                rows = rdbw.query(cls.SELECT_SETTING_FOR_ALL_HOSTS,
                                 {'name': setting_name})
                return {row.host_uuid: base._decode(setting_name, row.value)
                            for row in rows}


        @classmethod
        @contract_epydoc
        def get_all_schedules(cls):
            """
            @returns: The dict, mapping the host UUID to the tuple of
                      (1st) list of schedules, and
                      (2nd) the name of the host timezone (if it doesn't exist,
                      UTC is assumed).
            @rtype: dict
            @postcondition: consists_of(result.iterkeys(), UUID)
            @postcondition: consists_of(result.itervalues(), tuple)
            """
            base = Queries.Settings

            result = {}
            schedules_dict = cls.get_for_all_hosts(base.BACKUP_SCHEDULE)
            for host_uuid, schedules in schedules_dict.iteritems():
                _tz_name = cls.get(base.TIMEZONE, host_uuid)
                tz_name = _tz_name if _tz_name else 'utc'

                result[host_uuid] = (schedules, tz_name)

            return result



    class TrustedUsers(Queries.Inhabitants):
        """All DB (node-only) queries related to the cloud users."""

        __slots__ = tuple()


        @classmethod
        def add_user_with_group(cls, username, group, digest, dw):
            """
            Overrides C{add_user_with_group} from C{Queries.Inhabitants}.
            """
            user_id = super(TrustedQueries.TrustedUsers, cls) \
                          .add_user_with_group(username, group, digest, dw)
            cls.__add_user_to_node(user_id, dw)
            return user_id


        @classmethod
        def __add_user_to_node(cls, user_id, dw):
            cl_insert_user_at_node = models.users_at_node.insert()\
                                           .values(user_at_node_id=user_id)
            res = dw.execute(cl_insert_user_at_node)
            assert res.rowcount == 1, res.rowcount


        @classmethod
        def is_user_suspended_by_username(cls, username, dw):
            """
            @rtype: bool
            """
            m = models

            user_is_suspended = \
                sql_select([m.users_at_node.c.suspended],
                           (m.users.c.name == username),
                           m.users
                                 .join(m.users_at_node,
                                       m.users_at_node.c.user_at_node_id ==
                                           m.users.c.user_id))
            return dw.execute(user_is_suspended).scalar()


        @classmethod
        def is_user_suspended_by_host(cls, host_uuid, dw):
            """
            @rtype: bool
            """
            user_is_suspended = \
                sql_select([models.users_at_node.c.suspended],
                           (models.inhabitants.c.uuid == host_uuid),
                           models.inhabitants
                                 .join(models.hosts,
                                       models.hosts.c.host_id ==
                                           models.inhabitants.c.id)
                                 .join(models.users_at_node,
                                       models.users_at_node.c.user_at_node_id
                                           == models.hosts.c.user))
            return dw.execute(user_is_suspended).scalar()


        @classmethod
        def update_user_suspended(cls, username, rdbw, suspended=True):
            """Suspend/unsuspend user."""
            user_id = sql_select([models.users.c.user_id],
                                  models.users.c.name == username,
                                  models.users).scalar()
            update_suspended = \
                sql_update(models.users_at_node)\
                    .where(models.users_at_node.c.user_at_node_id == user_id)\
                    .values(suspended=suspended)
            rdbw.execute(update_suspended)



    class TrustedBlocks(Queries.Blocks):
        """All DB (node-only) queries related to the backup blocks."""

        __slots__ = tuple()


        @classmethod
        @contract_epydoc
        def __get_query(cls, with_files, with_blocks, with_chunks):
            """
            Return the SQL query to get the chunk/block data
            for some set of files.

            The query may accept arguments:
              - 'ds_uuid': dataset UUID;
              - 'base_dir': base directory;
              - 'rel_path': relative path of a file.

            @param with_files: do we need the files in the output rows?
            @param with_blocks: do we need the blocks in the output rows?
            @param with_chunks: do we need the chunks in the output rows?
            """

            # Arguments:
            # :ds_uuid - dataset UUID;
            # :base_dir -  base directory;
            # :rel_path - relative path of a file.
            # TODO:::: f_l_time_changed; group uuid; maybe, host UUID
            return _dd("""\
                SELECT {}
                """.format(  # if selecting chunks but not blocks,
                             # there may be dupes in the result,
                             # due to the multiple blocks.
                           'DISTINCT' if with_chunks and not with_blocks
                                      else '') \
                + ','.join(ifilter(
                               None,
                               ("""
                                    file_local_view.crc32
                                        AS f_l_crc32,
                                    file_local_view.uuid
                                        AS f_l_uuid,
                                    file_local_view.fingerprint
                                        AS f_l_fingerprint,
                                    file_local_view.base_directory_path
                                        AS f_l_base_dir_path,
                                    file_local_view.rel_path
                                        AS f_l_rel_path,
                                    file_local_view.attrs
                                        AS f_l_attrs,
                                    dataset.time_started
                                        AS f_l_time_changed
                                """  if with_files else None,
                                """
                                    block.offset_in_file
                                        AS block_offset_in_file,
                                    block.offset_in_chunk
                                        AS block_offset_in_chunk,
                                    block.size
                                        AS block_size
                                """ if with_blocks else None,
                                """
                                    chunk.maxsize_code AS chunk_maxsize_code,
                                    chunk.uuid AS chunk_uuid,
                                    chunk.size AS chunk_size,
                                    chunk.hash AS chunk_hash,
                                    chunk.crc32 AS chunk_crc32
                                """ if with_chunks else None))) \
                + """
                    FROM
                        dataset
                        INNER JOIN file_local_view
                            ON file_local_view.dataset_id = dataset.id
                """ \
                + ("""
                        INNER JOIN block
                            ON block.file_id = file_local_view.file_id
                """ if (with_blocks or with_chunks) else '') \
                + ("""
                        INNER JOIN chunk
                            ON block.chunk_id = chunk.id
                """ if with_chunks else '') \
                + """
                    WHERE
                        dataset.uuid = :ds_uuid AND
                        file_local_view.isdir = CAST(0 AS BOOLEAN) AND
                        ((:base_dir IS NOT NULL AND
                          file_local_view.base_directory_path = :base_dir) OR
                         (:base_dir IS NULL)) AND
                        ((:rel_path IS NOT NULL AND
                          file_local_view.rel_path = :rel_path) OR
                         (:rel_path IS NULL))
                """)

        @classmethod
        @contract_epydoc
        def __query_blocks_for_files(
                cls,
                restore_host_uuid, dataset_uuid, rel_paths_for_basedirs,
                with_files, with_blocks, with_chunks,
                rdbw):
            """
            Get the files/blocks/chunks for some dataset and for,
            optionally, some its files.

            @param rel_paths_for_basedirs: either a mapping that maps
                a base directory to the iterable (actually, list) of the paths
                which should be queries;
                or C{None} if the whole dataset is needed.
            @type rel_paths_for_basedirs: col.Mapping, NoneType

            @type with_files: bool
            @type with_blocks: bool
            @type with_chunks: bool
            @rtype: col.Iterable
            """
            logger.debug('Querying blocks for DS %s@host %s '
                             '(with_files %r, with_blocks %r, with_chunks %r) '
                             '- paths are %r',
                         dataset_uuid, restore_host_uuid,
                         with_files, with_blocks, with_chunks,
                         rel_paths_for_basedirs)

            # Now goes the most interesting part:
            # 1. rel_paths_for_basedirs may be None, but we need to loop
            #    over its items. So we'll coalesce it with {None: None}.
            # 2. at any point, rel_paths may be None, but again, we need
            #    to loop over it. So we similarly coalesce it with [None].
            return (entry
                        # rel_paths_for_basedirs may be None!
                        for base_dir, rel_paths
                            in coalesce(rel_paths_for_basedirs,
                                        {None: None}).iteritems()
                        # rel_paths may be None!
                        for rel_path in coalesce(rel_paths, [None])
                        for entry in rdbw.query(cls.__get_query(with_files,
                                                                with_blocks,
                                                                with_chunks),
                                                {'ds_uuid': dataset_uuid,
                                                 'base_dir': base_dir,
                                                 'rel_path': rel_path}))


        @classmethod
        def __get_uniqueness_enforcer_function(cls, file_paths_for_basedirs):
            """Get the minimal function that enforces the unique results.

            If there is more than 1 basedir or more than 1 path selected,
            there will be more than 1 SELECT in the final query,
            and the loop over the SELECTs may lead to duplicate results.
            So we must force uniqueness in this case and abandon memory
            economy (though,for sync/wildcard restores of a single
            directory it will be preserved).

            @rtype: col.Callable
            """
            __need_to_force_uniqueness = \
                file_paths_for_basedirs is not None and \
                (len(file_paths_for_basedirs.keys()) > 1
                 or
                 sum(len(files)
                         for files
                             in file_paths_for_basedirs.itervalues()) > 1)
            # This functor forces uniqueness of an iterator but only if
            # the uniqueness is needed to be enforced.
            return frozenset if __need_to_force_uniqueness else iter


        @classmethod
        @contract_epydoc
        def get_chunks_for_files(
                cls, restore_host_uuid, dataset_uuid, file_paths_for_basedirs,
                rdbw):
            """Get the chunks that consitute the dataset (and maybe its files).

            @param restore_host_uuid: the UUID of the host which created the
                dataset.
            @type restore_host_uuid: UUID

            @todo: C{restore_host_uuid} is ignored now, must use group instead.

            @type dataset_uuid: UUID

            @param file_paths_for_basedirs: the mapping that maps
                a base directory path to the iterable over the (relative) paths
                to the files;
                or C{None} if all files are needed.
            @type file_paths_for_basedirs: col.Mapping, NoneType

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the mapping from chunk UUIDs to the chunks.
            @rtype: col.Mapping

            @todo: remove heavy postconditions

            @postcondition: (isinstance(result, dict) and
                             consists_of(result.iterkeys(), UUID) and
                             consists_of(result.itervalues(), Chunk))
            """
            logger.debug('get_chunks_for_files(%r, %r, %r)',
                         restore_host_uuid,
                         dataset_uuid,
                         file_paths_for_basedirs)

            force_unique_chunks = cls.__get_uniqueness_enforcer_function(
                                      file_paths_for_basedirs)

            # For each file path, find its blocks in DB,
            # and group all the blocks together.

            # Create the simple "entries getter" curried function,
            # which needs just "with_blocks" and "with_chunks" arguments.
            entries_getter = partial(cls.__query_blocks_for_files,
                                     restore_host_uuid,
                                     dataset_uuid,
                                     file_paths_for_basedirs)

            # Unique chunks

            # Note: it's a rare case when ChunkInfo contain
            # not-None .blocks!
            _chunks_maybe_not_unique = \
                (ChunkInfo(  # ChunkInfo-specific
                           crc32=crc32_to_unsigned(row.chunk_crc32),
                             # Chunk-specific
                           uuid=row.chunk_uuid,
                           hash=str(row.chunk_hash),
                           size=row.chunk_size,
                           maxsize_code=int(row.chunk_maxsize_code))
                             for row in entries_getter(with_files=False,
                                                       with_blocks=False,
                                                       with_chunks=True,
                                                       rdbw=rdbw))
            _chunks = force_unique_chunks(_chunks_maybe_not_unique)

            if __debug__:
                # Make sure that the chunks are indeed unique
                _chunks = list(_chunks)
                __l1 = len(_chunks)
                __l2 = len(frozenset(_chunks))
                __l3 = len({c.uuid for c in _chunks})
                assert __l1 == __l2 == __l3, \
                       (__l1, __l2, __l3, _chunks)

            return {c.uuid: c for c in _chunks}


        @classmethod
        @contract_epydoc
        def get_blocks_for_files(
                cls,
                restore_host_uuid, dataset_uuid, file_paths_for_basedirs,
                rdbw):
            """Get the blocks that consitute the dataset (and maybe its files).

            @type restore_host_uuid: UUID

            @type dataset_uuid: UUID

            @param file_paths_for_basedirs: the mapping that maps
                a base directory path to the iterable over the (relative) paths
                to the files;
                or C{None} if all files are needed.
            @type file_paths_for_basedirs: col.Mapping, NoneType

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: a tuple of:
                1. File map (full path to C{LocalPhysicalFileState}),
                2. Block map (file full path to the list of C{Block}-s).
                Each file contains the proper list of appropriate C{Chunk}s.
                Each C{Block} is bound to the proper C{Chunk}.
            @rtype: tuple

            @todo: remove heavy postconditions

            @todo: C{restore_host_uuid} is ignored now, must use group instead.

            @postcondition: len(result) == 2
            @postcondition: (isinstance(result[0], dict) and
                             consists_of(result[0].iterkeys(),
                                         basestring) and
                             consists_of(result[0].itervalues(),
                                         LocalPhysicalFileState))
            @postcondition: (isinstance(result[1], dict) and
                             consists_of(result[1].iterkeys(), basestring) and
                             all(consists_of(lst, Chunk.Block)
                                     for lst in result[1].itervalues()))
            """
            logger.debug('get_blocks_for_files(%r, %r, %r)',
                         restore_host_uuid,
                         dataset_uuid,
                         file_paths_for_basedirs)

            force_unique_files = cls.__get_uniqueness_enforcer_function(
                                     file_paths_for_basedirs)

            # For each file path, find its blocks in DB,
            # and group all the blocks together.

            # Create the simple "entries getter" curried function,
            # which needs just "with_blocks" and "with_chunks" arguments.
            entries_getter = partial(cls.__query_blocks_for_files,
                                     restore_host_uuid,
                                     dataset_uuid,
                                     file_paths_for_basedirs)

            # First, get the chunks.
            chunks_by_uuid = cls.get_chunks_for_files(restore_host_uuid,
                                                      dataset_uuid,
                                                      file_paths_for_basedirs,
                                                      rdbw)

            # Unique files;
            _files_maybe_not_unique = \
                (LocalPhysicalFileState(
                         # LocalPhysicalFileState-specific
                         time_changed=fix_strptime(row.f_l_time_changed),
                         # LocalFile-specific
                         rel_path=row.f_l_rel_path,
                         base_dir=row.f_l_base_dir_path,
                         attrs=row.f_l_attrs,
                         # File-specific
                         fingerprint=None if row.f_l_fingerprint is None
                                          else Fingerprint.from_buffer(
                                                   row.f_l_fingerprint),
                         crc32=None if row.f_l_crc32 is None
                                    else crc32_to_unsigned(row.f_l_crc32),
                         uuid=row.f_l_uuid)
                     for row in entries_getter(with_files=True,
                                               with_blocks=False,
                                               with_chunks=False,
                                               rdbw=rdbw))
            _files = force_unique_files(_files_maybe_not_unique)

            if __debug__:
                # Make sure that the files are indeed unique
                _files = list(_files)
                assert (len(_files) ==
                        len(frozenset(_files)) ==
                        len({f.rel_path for f in _files})), \
                       repr(_files)

            files_by_rel_path = {f.rel_path: f for f in _files}

            del _files  # help GC!
            gc.collect()

            # Initialize the mapping by the already known file names
            blocks_by_file_rel_path = \
                {rel_path: []
                     for rel_path in files_by_rel_path.iterkeys()}

            # Blocks
            _blocks = \
                (Chunk.Block(file=files_by_rel_path[row.f_l_rel_path],
                             offset_in_file=int(row.block_offset_in_file),
                             offset_in_chunk=int(row.block_offset_in_chunk),
                             size=int(row.block_size),
                             chunk=chunks_by_uuid[row.chunk_uuid])
                     for row in entries_getter(with_files=True,
                                               with_blocks=True,
                                               with_chunks=True,
                                               rdbw=rdbw))

            for block in _blocks:
                # Bind to chunk
                block.chunk.blocks.append(block)
                assert block.file.rel_path in blocks_by_file_rel_path, \
                       (block.file.rel_path, blocks_by_file_rel_path)
                # Add to the mapping
                blocks_by_file_rel_path[block.file.rel_path].append(block)

            del _blocks  # help GC!
            gc.collect()

            assert (frozenset(files_by_rel_path) ==
                    frozenset(blocks_by_file_rel_path)), \
                   (files_by_rel_path, blocks_by_file_rel_path)

            return (files_by_rel_path,
                    blocks_by_file_rel_path)



    class TrustedChunks(Queries.Chunks):
        """All DB (node-only) queries related to the backup chunks."""

        __slots__ = tuple()


        @classmethod
        def __get_chunks_info_iters(cls, target_host_id, notifications):
            """
            Given a (non-reiterable) Iterable over the notifications,
            yield the (several) iterators over the tuples ready for
            INSERT_CHUNK_PER_HOST_VIEW.
            The outer iterator iterates over the single notifications.
            """
            for notif in notifications:
                # If no chunks, the following calculation will cause
                # division by zero.
                if notif.chunks:
                    yield ({'host_id': target_host_id,
                            'crc32': crc32_to_signed(chunk.crc32),
                            'uuid': chunk.uuid,
                            'hash': buffer(chunk.hash),
                            'size': chunk.size(),
                            'maxsize_code': coalesce(chunk.maxsize_code, 0)}
                               for chunk in notif.chunks)


        @classmethod
        def __get_chunks_stat_info_iters(cls,
                                         uploader_host_uuid,
                                         target_host_uuid,
                                         notifications):
            """
            Given a (non-reiterable) Iterable over the notifications,
            yield the (several) iterators over the tuples ready for
            INSERT_CHUNK_TRAFFIC_STAT_VIEW.
            The outer iterator iterates over the single notifications.
            """
            for notif in notifications:
                if notif.duration is not None and notif.end_ts is not None:
                    # chunks will be iterated twice, so just to be sure...
                    chunks = list(notif.chunks)

                    bps = (sum(c.size() for c in notif.chunks)
                           / td_to_sec(notif.duration))
                    yield ({'src_host_uuid': uploader_host_uuid,
                            'dst_host_uuid': target_host_uuid,
                            'chunk_uuid': chunk.uuid,
                            'time_completed': notif.end_ts,
                            'bps': bps}
                               for chunk in notif.chunks)


        @classmethod
        @duration_logged()
        @contract_epydoc
        def chunks_are_uploaded_to_the_host(
                cls, uploader_host_uuid, target_host_uuid, notifications,
                rdbw):
            """
            Given the iterable of the chunks,
            mark that they were successfully uploaded
            from uploader host to the target host.

            @param uploader_host_uuid: the host UUID which uploaded the chunks.
                May be C{None} if the chunks were uploaded by some other way,
                rather than from a host. In such case, the statistics won't be
                updated.
            @type uploader_host_uuid: UUID, NoneType

            @param target_host_uuid: The host UUID which received the chunks.
            @type target_host_uuid: UUID

            @param notifications: the data with the chunks information,
                contains C{ProgressNotificationPerHost}.
            @type notifications: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @todo: maybe, it could be converted to use generators better.
            """
            # Notifications will be iterated twice, unfortunately.
            notifications = list(notifications)

            #
            # 1. Bind chunks to the target host
            #

            host_row = rdbw.query_value(
                           Queries.Inhabitants.SELECT_HOST_BY_UUID,
                           {'uuid': target_host_uuid})
            assert host_row is not None
            _host_id = host_row.id

            all_chunks_iter_iter = cls.__get_chunks_info_iters(_host_id,
                                                               notifications)

            # Now we've collected all the data regarding
            # which chunks were uploaded to this host;
            # let's update the DB.
            rdbw.querymany(cls.INSERT_CHUNK_PER_HOST_VIEW,
                           chain.from_iterable(all_chunks_iter_iter))

            #
            # 2. Update the statistics for the uploader-host.
            #

            if uploader_host_uuid is not None:
                all_chunk_stat_iter_iter = \
                    cls.__get_chunks_stat_info_iters(uploader_host_uuid,
                                                     target_host_uuid,
                                                     notifications)
                rdbw.querymany(TrustedQueries.TrafficStat
                                             .INSERT_CHUNK_TRAFFIC_STAT_VIEW,
                               chain.from_iterable(all_chunk_stat_iter_iter))


        @classmethod
        @contract_epydoc
        def get_total_size_of_chunks(cls, chunk_uuids, rdbw):
            """Get the total size of all the passed chunks.

            @type chunk_uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: the size of the chunks.
            @rtype: numbers.Integral
            """
            chunks = models.chunks

            cl_select_total_size = sql_select([func.sum(chunks.c.size)],
                                              chunks.c.uuid.in_(
                                                  list(chunk_uuids)),
                                              from_obj=chunks)

            return rdbw.execute(cl_select_total_size).scalar()


        @classmethod
        @contract_epydoc
        def get_host_uuids_for_chunk(cls, chunk_uuid, rdbw):
            """Get the hosts (actually, UUIDs) which have this chunk.

            @type chunk_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a (non-reiterable) Iterable of host UUIDs for the hosts
                      which has this chunk.
            @rtype: col.Iterable
            """
            cph_view = models.ChunkPerHost.chunk_per_host_view().alias()
            cl_select = sql_select([cph_view.c.host_uuid],
                                   cph_view.c.chunk_uuid == chunk_uuid,
                                   from_obj=cph_view)

            return (row.host_uuid for row in rdbw.execute(cl_select))


        @classmethod
        @contract_epydoc
        def get_host_uuids_for_chunks(cls, chunk_uuids, rdbw):
            """Get the hosts (actually, UUIDs) which have these chunks.

            @todo: this function is unsafe for large volumes, and will
                definitely fail if the dataset contains too many chunks.

            @type chunk_uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the mapping from the host UUID to the set of UUIDs
                of the chunks at this host.
            @rtype: dict
            @postcondition: all(isinstance(hu, HostUUID)
                                    for hu in result.iterkeys())
            @postcondition: consists_of(result.itervalues(), set)
            @postcondition: all(consists_of(chunk_set, ChunkUUID)
                                    for chunk_set in result.itervalues())
            """
            if __debug__:
                chunk_uuids = list(chunk_uuids)
                assert consists_of(chunk_uuids, UUID), repr(chunk_uuids)
                logger.verbose('Getting hosts for chunks: %r', chunk_uuids)

            # There are two possible implementations:
            # 1. Supports any size of chunk_uuids, but runs O(n) queries,
            #    one for each chunk.
            #    I.e.: "slow with small size, works with large size".
            # 2. Runs a single query with SQL "IN" operator, but the query
            #    is huge, and WILL fail with too large chunk_uuids.
            #    I.e.: "fast with small size, fails with large size".
            if False:
                # V1 (disabled): "slow with small size, works with large size"
                host_chunk_pairs = \
                    ((h_uuid, ch_uuid)
                         for ch_uuid in chunk_uuids
                         for h_uuid in cls.get_host_uuids_for_chunk(ch_uuid,
                                                                    rdbw))

                # Do it in one pass:
                result = {}
                for h_uuid, ch_uuid in host_chunk_pairs:
                    result.setdefault(h_uuid, set()).add(ch_uuid)
            else:
                # V2 (enabled): "fast with small size, fails with large size"

                # In future, maybe, in should be done in portions, small enough
                # to don't overfill the query size.
                cph_view = models.ChunkPerHost.chunk_per_host_view().alias()
                cl_select = sql_select([cph_view.c.chunk_uuid,
                                        cph_view.c.host_uuid],
                                       cph_view.c.chunk_uuid.in_(
                                           list(chunk_uuids)),
                                       from_obj=cph_view)

                # Do it in one pass:
                result = {}
                for row in rdbw.execute(cl_select):
                    result.setdefault(row.host_uuid, set()).add(row.chunk_uuid)

            return {HostUUID.safe_cast_uuid(k): {ChunkUUID.safe_cast_uuid(c_u)
                                                     for c_u in v}
                        for k, v in result.iteritems()}


        @classmethod
        @contract_epydoc
        def get_chunk_uuids_for_host(cls, host_uuid, rdbw):
            """Get the chunks stored on this host.

            @type host_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            """
            cl_select = \
                models.ChunkPerHost.select_chunk_uuids_by_host_uuid_expr()

            rows = rdbw.execute(cl_select,
                                {'host_uuid': host_uuid})
            return (row.uuid for row in rows)


        # Must be used with .format()
        # which embeds the SQL query returning the table
        # of chunk UUIDs to check.
        SELECT_NONEXISTING_CHUNKS_AMONG_THESE = _dd("""\
            SELECT c_uuid
                FROM
                    ({0})
                    LEFT JOIN chunk
                        ON c_uuid = chunk.uuid
                WHERE chunk.uuid IS NULL
            """)

        @classmethod
        def check_chunks_for_existence(cls, chunk_uuids):
            """
            Given an iterable of chunks to check, return those ones of them
            which do not exist in any dataset at all.

            @type chunks: col.Iterable
            @precondition: all(isinstance(cu, UUID) for cu in chunk_uuids)

            @rtype: col.Iterable
            @postcondition: all(isinstance(cu, UUID) for cu in result)
            """
            assert RDB is not None

            # Create an "UNION"-based SQL expression
            # to dynamically construct the table with all the chunks
            # which we want to test for existence in the cloud.
            union_string = ' UNION '.join(
                "SELECT '{0.hex}'{1}".format(u,
                                             ' AS c_uuid' if i == 0 else '')
                    for i, u in enumerate(chunk_uuids))

            with RDB() as rdbw:
                chunks_unp = \
                    rdbw.query(cls.SELECT_NONEXISTING_CHUNKS_AMONG_THESE
                                 .format(union_string))
                return frozenset(UUID(row.c_uuid) for row in chunks_unp)


        @classmethod
        def delete_missing_chunks_for_host(cls, host_uuid,
                                           chunk_uuids_to_delete, rdbw):
            """
            Drop the records that the host contains some chunks
            for the chunks which are actually missing from that host.

            @note: one query, but it may become too long if there are too many
                items in C{chunk_uuids_to_delete}.

            @type host_uuid: UUID

            @type chunk_uuids_to_delete: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            m = models

            # cl_select_host has bindparam 'host_uuid'
            cl_select_host = m.Host.select_host_id_by_host_uuid_expr()
            cl_select_chunks = sql_select([m.chunks.c.id],
                                          whereclause=m.chunks.c.uuid.in_(
                                                        chunk_uuids_to_delete))
            cl_delete = m.chunks_per_host.delete(
                            (m.chunks_per_host.c.host == cl_select_host) &
                            m.chunks_per_host.c.chunk.in_(cl_select_chunks))

            rdbw.execute(cl_delete, {'host_uuid': host_uuid})


        # Arguments:
        # :host_uuid - host UUID to ignore
        SELECT_CLOUD_SIZES = _dd("""\
            SELECT
                (
                    SELECT coalesce(sum(CAST(value AS BIGINT)), 0)
                        FROM setting_view
                        WHERE name = '{}' AND host_uuid != :host_uuid
                ) AS total_size,
                (
                    SELECT coalesce(sum(chunk_size)/1048576.0, 0)
                        FROM chunk_per_host_view
                        WHERE host_uuid != :host_uuid
                ) AS used_size
            """).format(Queries.Settings.MAX_STORAGE_SIZE_MIB)


        @classmethod
        @contract_epydoc
        def get_cloud_sizes(cls, ignore_uuid):
            """
            @param ignore_uuid: Which host UUID should be ignored
                                during calculation of cloud size.
            @type ignore_uuid: UUID

            @return: The tuple of the cloud total size (in Mb, int/long),
                     and the cloud used size (in Mb, float).
            @rtype: tuple
            """
            assert RDB is not None

            with RDB() as rdbw:
                row = rdbw.query_value(cls.SELECT_CLOUD_SIZES,
                                       {'host_uuid': ignore_uuid})
                assert row is not None

                return (int(row.total_size), int(row.used_size))


        @classmethod
        @contract_epydoc
        def save_chunks_and_get_duplicates(cls, chunks, rdbw):
            """
            Given some chunks, get their already existing duplicates.
            Save the non-duplicated chunks as well.

            @todo: for increased safety, one must look for duplicates
                passing not just the chunk hash and size, but the UUID of
                the expected user group as well.

            @type chunks: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a mapping from chunk UUID of one of the argument chunks
                to the UUID of the already existing chunk which is a duplicate
                for that.
            @rtype: col.Mapping
            """
            chunks_by_uuid = {c.uuid: c for c in chunks}
            assert consists_of(chunks_by_uuid.itervalues(), Chunk), \
                   repr(chunks_by_uuid)

            all_incoming_chunk_uuids = chunks_by_uuid.viewkeys()

            duplicate_uuids_map = \
                dict(Queries.Chunks.find_duplicate_chunk_uuids(
                         chunks_by_uuid.itervalues(),
                         rdbw))

            if duplicate_uuids_map:
                logger.verbose('Found the duplicate UUIDs: %r',
                               duplicate_uuids_map)

            duplicate_colliding_uuids = duplicate_uuids_map.viewkeys()

            # New UUIDs are the ones which were not colliding.
            # Let's store them for the future, to prevent a chance
            # of another key collision.
            new_uuids = all_incoming_chunk_uuids - duplicate_colliding_uuids
            new_chunks = {chunks_by_uuid[u] for u in new_uuids}
            logger.verbose('Adding new chunks unconditionally: %r', new_chunks)
            Queries.Chunks.add_chunks(new_chunks, rdbw)

            return duplicate_uuids_map


        # Parameters
        # :ds_uuid
        # :path
        # :recursive
        # Results:
        # 1. file_count
        # 2. file_size
        # 3. uniq_file_count
        # 4. TODO: uniq_file_size
        # 5. TODO: full_replicas_count
        SELECT_DATA_FILE_STATS = _dd("""\
            SELECT
                    count(distinct file_local_view.id)
                        AS file_count,
                    coalesce(sum(file_local_view.size), 0)
                        AS file_size,
                    count(distinct file_local_view.file_id)
                        AS uniq_file_count,
                    -1
                        AS uniq_file_size, -- TODO
                    -1
                        AS full_replicas_count -- TODO
                FROM
                    file_local_view
                    {join_dataset}
                WHERE
                    1
                    {where_cond_dataset}
                    {where_cond_path}
            """)

        SELECT_DATA_CHUNK_STATS = _dd("""\
            SELECT
                    count(distinct chunk_per_host_view.chunk_id)
                        AS chunk_count,
                    count(distinct chunk_per_host_view.id)
                        AS chunk_replicas_count,
                    count(distinct chunk_per_host_view.host_id)
                        AS hosts_count
                FROM
                    file_local_view
                    INNER JOIN block
                        ON block.file_id = file_local_view.file_id
                    INNER JOIN chunk_per_host_view
                        ON chunk_per_host_view.chunk_id = block.chunk_id
                    {join_dataset}
                WHERE
                    1
                    {where_cond_dataset}
                    {where_cond_path}
            """)

        JOIN_DATASET = _dd("""\
                    INNER JOIN dataset
                        ON file_local_view.dataset_id = dataset.id
            """)

        WHERE_COND_DATASET = _dd("""\
                    AND (dataset.uuid = :ds_uuid)
            """)
        NO_WHERE_COND_DATASET = ""

        # If :path ends by "/", it is a directory, filter for :path%.
        # If :recursive is True, :path is a directory name without
        # trailing slash, filter for :path/%.
        # Otherwise filter for :path equality to the rel path.
        WHERE_COND_PATH = _dd("""\
                    AND (CASE
                             WHEN substr(:path, -1) = '/'
                                 THEN file_local_view.rel_path
                                          LIKE (:path || '%')
                             WHEN :recursive = CAST(1 AS BOOLEAN)
                                 THEN file_local_view.rel_path
                                          LIKE (:path || '/%')
                             ELSE
                                 file_local_view.rel_path = :path
                             END)
            """)
        NO_WHERE_COND_PATH = ""


        @classmethod
        @contract_epydoc
        def get_data_stats(cls, ds_uuid, path, path_rec):
            """
            @param ds_uuid: The UUID of the dataset to get the information for.
            @type ds_uuid: (UUID, NoneType)

            @param path: The path which statistics needs to be evaluated,
                         or None if any path is sufficient.
            @type path: (basestring, NoneType)

            @param path_rec: Whether the path should be investigated
                             recursively.
            @type path_rec: bool, NoneType

            @rtype: dict
            """
            assert RDB is not None

            path_enc = encode_posix_path(path) if path is not None \
                                               else None

            with RDB() as rdbw:
                tmpl_param_dict = {
                    'join_dataset':
                        cls.JOIN_DATASET if ds_uuid is not None
                                         else '',
                    'where_cond_dataset':
                        cls.WHERE_COND_DATASET if ds_uuid is not None
                                               else cls.NO_WHERE_COND_DATASET,
                    'where_cond_path':
                        cls.WHERE_COND_PATH if path_enc is not None
                                            else cls.NO_WHERE_COND_PATH
                }
                tmpl_param_values = {'ds_uuid': ds_uuid if ds_uuid is not None
                                                        else '',
                                     'path': coalesce(path_enc, ''),
                                     'recursive': 1 if path_rec else 0}

                file_stats_query = cls.SELECT_DATA_FILE_STATS \
                                      .format(**tmpl_param_dict)
                file_stats_row = rdbw.query_value(file_stats_query,
                                                  tmpl_param_values)
                assert file_stats_row is not None

                chunks_stats_query = cls.SELECT_DATA_CHUNK_STATS \
                                        .format(**tmpl_param_dict)
                chunks_stats_row = rdbw.query_value(chunks_stats_query,
                                                    tmpl_param_values)
                assert chunks_stats_row is not None

                return {
                    'file_count': file_stats_row.file_count,
                    'file_size': file_stats_row.file_size,
                    'uniq_file_count': file_stats_row.uniq_file_count,
                    # TODO: uniq_file_size = -1
                    'uniq_file_size': file_stats_row.uniq_file_size,
                    # TODO: full_replicas_count = -1
                    'full_replicas_count': file_stats_row.full_replicas_count,
                    'chunk_count': chunks_stats_row.chunk_count,
                    'chunk_replicas_count':
                        chunks_stats_row.chunk_replicas_count,
                    'hosts_count': chunks_stats_row.hosts_count
                }


        @classmethod
        @memoized
        def _datasets_with_chunk_view_expr(cls):
            r"""
            Get an expression that finds the datasets containing a given chunk.

            The query accepts a single bound parameter C{chunk_uuid},
            and returns rows with C{dataset_id} and C{dataset_uuid} columns.

            >>> str(TrustedQueries.TrustedChunks \
            ...                   ._datasets_with_chunk_view_expr()) \
            ...     # doctest:+NORMALIZE_WHITESPACE
            'SELECT DISTINCT
                dataset.id AS dataset_id,
                dataset.uuid AS dataset_uuid
            \nFROM
                chunk
                JOIN block ON chunk.id = block.chunk_id
                JOIN file ON file.id = block.file_id
                JOIN file_local ON file.id = file_local.file_id
                JOIN base_directory
                    ON base_directory.id = file_local.base_directory_id
                JOIN dataset ON dataset.id = base_directory.dataset_id
            \nWHERE chunk.uuid = :chunk_uuid'
            """
            m = models

            return sql_select(
                columns=[m.datasets.c.id.label('dataset_id'),
                         m.datasets.c.uuid.label('dataset_uuid')],
                from_obj=m.chunks.join(m.blocks)
                                 .join(m.files)
                                 .join(m.file_locals)
                                 .join(m.base_directories)
                                 .join(m.datasets),
                whereclause=(m.chunks.c.uuid == bindparam('chunk_uuid')),
                distinct=True)


        @classmethod
        @memoized
        def _chunk_first_sync_hosts_expr(cls):
            r"""
            Get an expression that, for a given chunk, suggests the UUIDs
            of the hosts which some time ago were the first "syncers"
            of this chunk; i.e., this chunk was someday present in any "sync"
            dataset, and such dataset was first marked as synced
            on this particular host.

            The query accepts a single bound parameter C{chunk_uuid},
            and returns rows with C{host_uuid} columns.

            >>> str(TrustedQueries.TrustedChunks
            ...                   ._chunk_first_sync_hosts_expr()) \
            ...     # doctest:+NORMALIZE_WHITESPACE
            'SELECT DISTINCT
                first_value(inhabitant.uuid)
                    OVER (PARTITION BY dataset_on_host.dataset_id
                          ORDER BY dataset_on_host.time ASC)
                    AS host_uuid
            \nFROM
                (SELECT DISTINCT
                    dataset.id AS dataset_id,
                    dataset.uuid AS dataset_uuid
                \nFROM
                    chunk
                    JOIN block ON chunk.id = block.chunk_id
                    JOIN file ON file.id = block.file_id
                    JOIN file_local ON file.id = file_local.file_id
                    JOIN base_directory
                        ON base_directory.id = file_local.base_directory_id
                    JOIN dataset ON dataset.id = base_directory.dataset_id
                \nWHERE chunk.uuid = :chunk_uuid)
                    AS anon_1
                JOIN dataset_on_host
                    ON anon_1.dataset_id = dataset_on_host.dataset_id
                JOIN inhabitant ON inhabitant.id = dataset_on_host.host_id'
            """
            m = models

            # What datasets contain the chunk bound to C{"chunk_uuid"}?
            # Create an expression.
            datasets_for_chunk_expr = \
                cls._datasets_with_chunk_view_expr().alias()
            # bound: chunk_uuid, result: dataset_id, dataset_uuid

            # For each of the datasets above (which contain the chunk
            # bound to C{"chunk_uuid"}), find out the earliest
            # C{dataset_on_host} record, and the UUID of the appropriate host.
            return sql_select(
                columns=[over(func.first_value(m.inhabitants.c.uuid),
                              partition_by=m.datasets_on_host.c.dataset,
                              order_by=asc(m.datasets_on_host.c.time))
                             .label('host_uuid')],
                from_obj=datasets_for_chunk_expr
                             .join(m.datasets_on_host)
                             .join(m.inhabitants,
                                   m.inhabitants.c.id
                                       == m.datasets_on_host.c.host),
                distinct=True)


        @classmethod
        @memoized
        def _chunk_heal_sources_expr(cls):
            r"""
            Get an expression that suggests all the hosts which may have
            some chunk and help with its healing.

            The query accepts a single bound parameter C{chunk_uuid},
            and returns rows with C{host_uuid} columns.

            >>> str(TrustedQueries.TrustedChunks._chunk_heal_sources_expr()) \
            ...     # doctest:+NORMALIZE_WHITESPACE
            'SELECT DISTINCT
                anon_1.host_uuid
            \nFROM
                (SELECT DISTINCT
                    first_value(inhabitant.uuid)
                    OVER (PARTITION BY dataset_on_host.dataset_id
                          ORDER BY dataset_on_host.time ASC) AS host_uuid
                \nFROM
                    (SELECT DISTINCT
                        dataset.id AS dataset_id,
                        dataset.uuid AS dataset_uuid
                    \nFROM
                        chunk
                        JOIN block ON chunk.id = block.chunk_id
                        JOIN file ON file.id = block.file_id
                        JOIN file_local ON file.id = file_local.file_id
                        JOIN base_directory
                            ON base_directory.id = file_local.base_directory_id
                        JOIN dataset ON dataset.id = base_directory.dataset_id
                    \nWHERE chunk.uuid = :chunk_uuid) AS anon_2
                JOIN dataset_on_host
                    ON anon_2.dataset_id = dataset_on_host.dataset_id
                JOIN inhabitant ON inhabitant.id = dataset_on_host.host_id
                UNION
                SELECT
                    inhabitant.uuid AS host_uuid
                \nFROM
                    chunk
                    JOIN chunk_per_host ON chunk.id = chunk_per_host.chunk_id
                    JOIN host ON host.id = chunk_per_host.host_id
                    JOIN inhabitant ON inhabitant.id = host.id
                \nWHERE chunk.uuid = :chunk_uuid) AS anon_1'
            """
            m = models

            # Source 1: chunk may be present on the hosts who were marked as
            # "synced to" first.
            hosts_first_synced_from_expr = \
                cls._chunk_first_sync_hosts_expr()

            # Source 2: chunk is present on the host via chunk_per_host table.
            hosts_with_chunk_expr = sql_select(
                columns=[m.inhabitants.c.uuid.label('host_uuid')],
                from_obj=m.chunks.join(m.chunks_per_host)
                                 .join(m.hosts)
                                 .join(m.inhabitants),
                whereclause=(m.chunks.c.uuid == bindparam('chunk_uuid')))

            union_expr = union(hosts_first_synced_from_expr,
                               hosts_with_chunk_expr).alias()

            total_expr = sql_select(
                columns=[union_expr.c.host_uuid],
                from_obj=union_expr,
                distinct=True)
            return total_expr


        @classmethod
        @contract_epydoc
        def find_chunk_potential_heal_sources(cls, chunk_uuid, rdbw):
            """Find the hosts which may have provide this chunk for healing.

            @type chunk_uuid: ChunkUUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the UUIDs of the hosts which probably have this chunk
                and can provide its contents.
            @rtype: col.Iterable
            """
            rows = rdbw.execute(cls._chunk_heal_sources_expr(),
                                {'chunk_uuid': chunk_uuid})
            return (HostUUID.safe_cast_uuid(row.host_uuid) for row in rows)



    class TrustedFiles(Queries.Files):
        """All DB (node-only) queries related to the backup files."""

        __slots__ = tuple()


        @classmethod
        @contract_epydoc
        def get_last_file_locals_for_user_dir(cls, username, base_path,
                                              max_time_started, rdbw):
            """
            Get all file_locals (a bit extended version) for C{username}
            based on C{base_path} (to show files only on his current path) and
            with upper bound by C{max_time_started}. Use only not merged
            datasets.

            @param username: the name of the user.
            @type username: basestring

            @param base_path: current path where user is
            @type base_path: basestring

            @param max_time_started: upper bound for search
            @type max_time_started: datetime

            @param rdbw: database wrapper.
            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the non-reiterable iterable over extended file_locals.
            @rtype: col.Iterable
            """
            # Unique rel_paths with latest time_started by grouped
            # by dataset.group_id
            latest_urp = \
                models.FileLocal.get_latest_uniq_rel_paths_by_group_expr(
                        latest_time_started=bindparam('max_time_started'),
                        is_time_completed_specified=False)\
                      .alias()
            # bindparams: max_time_started

            # Get view with all file_locals.
            file_locals = models.FileLocal.file_local_view().alias()

            # Users file_locals
            users_file_locals = \
                sql_select(
                    [(file_locals.c.file != None).label('has_file'),
                     file_locals.c.isdir,
                     file_locals.c.rel_path,
                     file_locals.c.path,
                     file_locals.c.uuid,
                     file_locals.c.sync,
                     file_locals.c.time_started,
                     (file_locals.c.time_completed == None).label('in_proc')],
                    and_(# Checking username.
                         models.users.c.name == bindparam('username'),
                         # Check for merged datasets. We don't need them.
                         file_locals.c.merged == sqla_false(),
                         # Count slashes to get only nested file_locals
                         func.CountSlashesInString(latest_urp.c.rel_path) ==
                             func.CountSlashesInString(
                                 bindparam('base_path')),
                         # And check that rel_path starts with needed path
                         latest_urp.c.rel_path.startswith(
                             bindparam('base_path')),
                         # DO NOT TOUCH! WORKS ON MAGIC!
                         (((file_locals.c.time_completed == None) &
                           (file_locals.c.time_started >=
                                bindparam('max_time_started') -
                                timedelta(days=1))) |
                          ((file_locals.c.time_completed != None) &
                           (file_locals.c.time_started <=
                                bindparam('max_time_started'))))),
                    from_obj=latest_urp
                                 .join(file_locals,
                                       and_(latest_urp.c.rel_path ==
                                                file_locals.c.rel_path,
                                            latest_urp.c.group ==
                                                file_locals.c.group,
                                            latest_urp.c.ds_mts ==
                                                file_locals.c.time_started))
                                 .join(models.memberships,
                                       latest_urp.c.group ==
                                           models.memberships.c.group)
                                 .join(models.users,
                                       models.memberships.c.user ==
                                           models.users.c.user_id))\
                    .order_by(desc('in_proc'), 'rel_path')

            return rdbw.execute(users_file_locals,
                                {'base_path': base_path,
                                 'username': username,
                                 'max_time_started': max_time_started})


        @classmethod
        def revert_file_version(cls, username, ds_uuid, base_path, rel_path,
                                rdbw):
            """
            Create a new dataset, containing the file version
            as of a passed file.

            @param username: the name of the user.
            @type username: basestring

            @param ds_uuid: The UUID of the dataset to get the information for.
            @type ds_uuid: DatasetUUID

            @param base_path: current path where user is
            @type base_path: basestring

            @param rel_path: rel_path of the file.
            @type rel_path: basestring

            @param rdbw: database wrapper.
            @type rdbw: DatabaseWrapperSQLAlchemy

            @todo: instead of validating the rights of the user in the "insert"
                section, validate it as the very first action in the function,
                and then run the following queries assuming everything
                is validated properly.
                Or even require them to be validated outside.
            """
            m = models

            old_dataset = Queries.Datasets.get_dataset_by_uuid(
                              ds_uuid, None, rdbw)
            new_ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())
            utcnow = datetime.utcnow()

            # Create new dataset.
            Queries.Datasets.add_dataset(
                ugroup_uuid=old_dataset.ugroup_uuid,
                name='', ds_uuid=new_ds_uuid, sync=True, merged=False,
                time_started=utcnow, time_completed=utcnow,
                rdbw=rdbw)

            # Create new base_directory.
            new_base_path = sql_select([bindparam('base_dir')
                                            .label('base_directory'),
                                        m.datasets.c.id],
                                       (m.datasets.c.uuid == new_ds_uuid),
                                       m.datasets).alias()

            rdbw.execute(insert_from_select([m.base_directories.c.path,
                                             m.base_directories.c.dataset],
                                            new_base_path),
                         {'base_dir': base_path})

            new_base_path_id = sql_select([m.base_directories.c.id],
                                          (m.datasets.c.uuid == new_ds_uuid),
                                          m.base_directories.join(m.datasets))\
                                   .alias()

            # Create new file_local.
            new_file_local = \
                sql_select([m.file_locals.c.file,
                            new_base_path_id,
                            m.file_locals.c.isdir,
                            m.file_locals.c.rel_path,
                            m.file_locals.c.attrs],
                           from_obj=m.file_locals
                                     .join(m.base_directories)
                                     .join(m.datasets)
                                     .join(models.memberships,
                                           m.datasets.c.group ==
                                               m.memberships.c.group)
                                     .join(m.users,
                                           m.memberships.c.user ==
                                               m.users.c.user_id),
                           whereclause=(m.users.c.name == username) &
                                       (m.datasets.c.uuid == ds_uuid) &
                                       (m.base_directories.c.path ==
                                            base_path) &
                                       (m.file_locals.c.rel_path == rel_path))\
                    .alias()

            rdbw.execute(insert_from_select([m.file_locals.c.file,
                                             m.file_locals.c.base_directory,
                                             m.file_locals.c.isdir,
                                             m.file_locals.c.rel_path,
                                             m.file_locals.c.attrs],
                                            new_file_local))


        @classmethod
        def delete_file(cls, username, ds_uuid, base_dir, rel_path, rdbw):
            """
            Create new dataset, that contains a mention that some file
            should be deleted.

            @param username: the name of the user.
            @type username: basestring

            @param ds_uuid: the UUID of the dataset which is used
                as a reference (for example, to clone its user group etc etc).
                Seems should be fixed and simplified.
            @type ds_uuid: DatasetUUID

            @param base_dir: current path where user is
            @type base_dir: basestring

            @param rel_path: rel_path of the file.
            @type rel_path: basestring

            @param rdbw: database wrapper.
            @type rdbw: DatabaseWrapperSQLAlchemy

            @todo: instead of validating the rights of the user in the "insert"
                section, validate it as the very first action in the function,
                and then run the following queries assuming everything
                is validated properly.
                Or even require them to be validated outside.
            """
            m = models

            # Validate rel_path: it can be incorrect or it can be a directory.
            validate_path_clause = sql_select(
                [and_(m.file_locals.c.file != None,
                      m.file_locals.c.isdir == sqla_false()).label('is_file')],
                and_(m.users.c.name == username,
                     m.file_locals.c.rel_path == rel_path),
                m.file_locals.join(m.base_directories)
                             .join(m.datasets)
                             .join(models.memberships,
                                   m.datasets.c.group == m.memberships.c.group)
                             .join(m.users,
                                   m.memberships.c.user == m.users.c.user_id))\
                    .order_by(desc(m.datasets.c.time_started))

            validate_path = rdbw.execute(validate_path_clause).scalar()

            # A bit excessive check, to express the fact that the C{.scalar()}
            # may return None (if no rows found), and that if it is not None,
            # it is a bool.
            if validate_path is not None and validate_path:
                # Yep, it's a file.
                old_dataset = TrustedQueries.Datasets.get_dataset_by_uuid(
                                  ds_uuid, None, rdbw)
                new_ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())
                utcnow = datetime.utcnow()

                # Create new dataset.
                TrustedQueries.Datasets.add_dataset(
                    ugroup_uuid=old_dataset.ugroup_uuid,
                    name='', ds_uuid=new_ds_uuid, sync=True, merged=False,
                    time_started=utcnow, time_completed=utcnow,
                    rdbw=rdbw)

                # Create new base_directory.
                new_base_dir = sql_select([bindparam('base_dir')
                                                .label('base_directory'),
                                            m.datasets.c.id],
                                           (m.datasets.c.uuid == new_ds_uuid),
                                           m.datasets)\
                                   .alias()

                rdbw.execute(insert_from_select([m.base_directories.c.path,
                                                 m.base_directories.c.dataset],
                                                new_base_dir),
                             {'base_dir': base_dir})

                new_base_dir_id = \
                    sql_select([m.base_directories.c.id],
                               (m.datasets.c.uuid == new_ds_uuid),
                               m.base_directories.join(m.datasets)) \
                        .alias()

                # Create new file_local.
                new_file_local = \
                    sql_select([bindparam('file'),
                                new_base_dir_id,
                                bindparam('isdir'),
                                m.file_locals.c.rel_path,
                                m.file_locals.c.attrs],
                               and_(m.users.c.name == username,
                                    m.datasets.c.uuid == ds_uuid,
                                    # m.base_directories.c.path == base_dir,
                                    m.file_locals.c.rel_path == rel_path),
                               m.file_locals
                                   .join(m.base_directories)
                                   .join(m.datasets)
                                   .join(models.memberships,
                                         m.datasets.c.group ==
                                             m.memberships.c.group)
                                   .join(m.users,
                                         m.memberships.c.user ==
                                             m.users.c.user_id)) \
                        .alias()

                rdbw.execute(
                    insert_from_select([m.file_locals.c.file,
                                        m.file_locals.c.base_directory,
                                        m.file_locals.c.isdir,
                                        m.file_locals.c.rel_path,
                                        m.file_locals.c.attrs],
                                       new_file_local),
                    {'file': None, 'isdir': False})


        __CREATE_FILE_LOCAL_SLASHED_INDEX_POSTGRESQL = _dd("""
            CREATE OR REPLACE FUNCTION countslashesinstring(inp_str text)
                RETURNS integer AS $$
            DECLARE
                substr_idx integer := 0;
                slash_idx integer := 0;
                str_len integer := length(inp_str);
                slashes_count integer := 0;
            BEGIN
                WHILE True LOOP
                    substr_idx = strpos(substring(inp_str
                                                      FROM slash_idx + 1
                                                      FOR str_len - slash_idx),
                                        '/');
                    EXIT WHEN substr_idx = 0;
                    slash_idx = slash_idx + substr_idx;
                    slashes_count = slashes_count + 1;
                END LOOP;
                RETURN slashes_count;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE STRICT;

            CREATE INDEX file_local_basedir_isdir_countslashfunc_rel_path_index
                ON file_local
            (
                base_directory_id,
                isdir,
                countslashesinstring(rel_path),
                rel_path
            );
            """)

        CREATE_FILE_LOCAL_SLASHED_INDEX = {
            'sqlite':
                [],  # not implemented!
            'postgresql':
                [__CREATE_FILE_LOCAL_SLASHED_INDEX_POSTGRESQL]
        }



    class HostAtNode(Queries.Inhabitants):
        """All DB (node-only) queries related to the hosts."""

        __slots__ = tuple()


        CREATE_HOST_AT_NODE_VIEW = _dd("""\
            CREATE VIEW host_at_node_view AS
                SELECT
                    inhabitant.id AS id,
                    "user".id AS user_id,
                    "user".name AS user_name,
                    "user".digest AS user_digest,
                    inhabitant.uuid AS uuid,
                    inhabitant.urls AS urls,
                    host.name AS name,
                    host_at_node.last_seen AS last_seen
                FROM
                    inhabitant
                    INNER JOIN host
                        USING (id)
                    INNER JOIN host_at_node
                        USING (id)
                    INNER JOIN "user"
                        ON host.user_id = "user".id
            """)

        __CREATE_HOST_AT_NODE_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS host_at_node_view_insert_trigger
                INSTEAD OF INSERT ON host_at_node_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO inhabitant(uuid, urls)
                        VALUES (
                            new.uuid,
                            new.urls
                        );
                    INSERT OR IGNORE INTO host(id, user_id, name)
                        VALUES (
                            (SELECT id FROM inhabitant WHERE uuid = new.uuid),
                            coalesce(new.user_id,
                                     (SELECT id
                                          FROM "user"
                                          WHERE name = new.user_name)),
                            new.name
                        );
                    INSERT OR ROLLBACK INTO host_at_node(id, last_seen)
                        VALUES (
                            (SELECT id FROM inhabitant WHERE uuid = new.uuid),
                            new.last_seen
                        );
                END
            """)

        __CREATE_HOST_AT_NODE_VIEW_UPDATE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS host_at_node_view_update_trigger
                INSTEAD OF UPDATE ON host_at_node_view
                FOR EACH ROW
                BEGIN
                    UPDATE OR ROLLBACK inhabitant
                        SET
                            urls = new.urls
                        WHERE id = old.id;
                    UPDATE OR ROLLBACK host
                        SET
                            name = new.name
                        WHERE id = old.id;
                    UPDATE OR ROLLBACK host_at_node
                        SET last_seen = new.last_seen
                        WHERE id = old.id;
                END;
            """)

        __CREATE_HOST_AT_NODE_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS host_at_node_view_delete_trigger
                INSTEAD OF DELETE ON host_at_node_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM host_at_node WHERE id = old.id;
                END
            """)

        # Partly cloned from __CREATE_HOST_VIEW_TRIGGER_PROC_POSTGRESQL
        __CREATE_HOST_AT_NODE_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION host_at_node_view_trigger_proc()
            RETURNS TRIGGER AS $$
            DECLARE
                _inhabitant_id inhabitant.id%TYPE;
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    -- INSERT OR IGNORE INTO inhabitant

                    SELECT id
                        FROM inhabitant
                        WHERE uuid = new.uuid
                        INTO _inhabitant_id;

                    IF NOT FOUND THEN
                        INSERT INTO inhabitant(uuid, urls)
                            VALUES (new.uuid, new.urls)
                            RETURNING id INTO _inhabitant_id;
                    END IF;

                    -- INSERT OR IGNORE INTO host

                    PERFORM id FROM host WHERE id = _inhabitant_id;

                    IF NOT FOUND THEN
                        INSERT INTO host(id, user_id, name)
                            VALUES (
                                _inhabitant_id,
                                coalesce(new.user_id,
                                         (SELECT id
                                              FROM "user"
                                              WHERE name = new.user_name)),
                                new.name
                            );
                    END IF;

                    INSERT INTO host_at_node(id, last_seen)
                        VALUES (_inhabitant_id, new.last_seen);

                    RETURN new;
                ELSIF (TG_OP = 'UPDATE') THEN
                    UPDATE inhabitant
                        SET urls = new.urls
                        WHERE id = old.id;
                    UPDATE host
                        SET name = new.name
                        WHERE id = old.id;
                    UPDATE host_at_node
                        SET last_seen = new.last_seen
                        WHERE id = old.id;

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM host_at_node WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_HOST_AT_NODE_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER host_at_node_view_trigger
                INSTEAD OF INSERT OR UPDATE OR DELETE ON host_at_node_view
                FOR EACH ROW
                EXECUTE PROCEDURE host_at_node_view_trigger_proc()
            """)

        CREATE_HOST_AT_NODE_VIEW_TRIGGERS = {
            'sqlite':
                [__CREATE_HOST_AT_NODE_VIEW_INSERT_TRIGGER_SQLITE,
                 __CREATE_HOST_AT_NODE_VIEW_UPDATE_TRIGGER_SQLITE,
                 __CREATE_HOST_AT_NODE_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql':
                [__CREATE_HOST_AT_NODE_VIEW_TRIGGER_PROC_POSTGRESQL,
                 __CREATE_HOST_AT_NODE_VIEW_TRIGGER_POSTGRESQL]
        }


        SELECT_ALL_HOSTS_FOR_NODES = _dd("""\
            SELECT
                    uuid,
                    urls,
                    name,
                    user_name,
                    last_seen
                FROM host_at_node_view
            """)


        @staticmethod
        @contract_epydoc
        def __db_host_at_node_row_to_host(username_uc_user_map, row):
            """
            @param username_uc_user_map: The mapping from
                the uppercased user name to the appropriate C{User} class.
                This mapping will be called directly rather than
                with C{.get()}, so it either must always provide the result
                (and provide C{None} explicitly if needed), or fail.
            @type username_uc_user_map: col.Mapping

            @precondition: isinstance(row.last_seen,
                                      (NoneType, datetime, basestring))
            """
            return HostAtNode(uuid=HostUUID.safe_cast_uuid(row.uuid),
                              urls=json.loads(row.urls),
                              # Host-specific
                              name=str(row.name),
                              user=username_uc_user_map[row.user_name.upper()],
                              # HostAtNode-specific
                              last_seen=fix_strptime(row.last_seen))


        @staticmethod
        @contract_epydoc
        def __db_trusted_host_at_node_row_to_host(username_uc_user_map, row):
            """
            @param username_uc_user_map: The mapping from
                the uppercased user name to the appropriate C{User} class.
                This mapping will be called directly rather than
                with C{.get()}, so it either must always provide the result
                (and provide C{None} explicitly if needed), or fail.
            @type username_uc_user_map: col.Mapping
            @precondition: (isinstance(row.for_storage, bool) and
                            isinstance(row.for_restore, bool))

            @todo: improve and fill all columns, if needed. Or remove them,
                on the other hand.
            """
            return TrustedHostAtNode(
                       # AbstractInhabitant-specific
                       uuid=HostUUID.safe_cast_uuid(row.uuid),
                       urls=None,  # don't care
                        # Host-specific
                       name=None,  # don't care
                       user=username_uc_user_map[row.user_name.upper()],
                        # HostAtNode-specific
                       last_seen=None,  # don't care
                        # TrustedHostAtNode-specific
                       for_storage=bool(row.for_storage),
                       for_restore=bool(row.for_restore))


        @classmethod
        @contract_epydoc
        def get_all_hosts(cls, username_uc_user_map=None, rdbw=None):
            """
            Return all the hosts stored in the DB in the HostAtNode
            representation.
            If C{username_uc_user_map} is provided, it will be used
            to bind the users.

            @type username_uc_user_map: NoneType, col.Mapping

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a (possibly, non-reiterable) iterable of C{HostAtNode}
                objects.
            @rtype: col.Iterable
            """
            # If there is no mapping, we want that every host has
            # the user field set to None.
            # If there is a mapping provided, we want for every host to be
            # bound to the users; absence of a binding is a error.
            if username_uc_user_map is None:
                username_uc_user_map = defaultdict(lambda: None)

            rows = rdbw.query(cls.SELECT_ALL_HOSTS_FOR_NODES)
            return (cls.__db_host_at_node_row_to_host(username_uc_user_map,
                                                      row)
                        for row in rows)


        @classmethod
        @contract_epydoc
        def add_host(cls, username, hostname, host_uuid, trusted_host_caps,
                     rdbw):
            """For a user given by their name, add host to the DB.

            @type username: str

            @type username: basestring

            @type host_uuid: UUID

            @type trusted_host_caps: NoneType, TrustedHostCaps

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            m = models

            inh_id = Queries.Inhabitants.add_inhabitant(host_uuid, '[]', rdbw)

            cl_user_id = sql_select([m.users.c.user_id],
                                    func.upper(m.users.c.name) ==
                                        username.upper())

            cl_insert_host = m.hosts.insert().values(host_id=inh_id,
                                                     name=hostname,
                                                     user=cl_user_id)
            rdbw.execute(cl_insert_host)

            cl_insert_host_at_node = m.hosts_at_node \
                                      .insert() \
                                      .values(host_at_node_id=inh_id)
            rdbw.execute(cl_insert_host_at_node)

            if trusted_host_caps is not None:
                cl_insert_trusted_host_at_node = \
                    m.trusted_hosts_at_node \
                     .insert() \
                     .values(trusted_host_at_node_id=inh_id,
                             for_storage=trusted_host_caps.storage,
                             for_restore=trusted_host_caps.restore)
                rdbw.execute(cl_insert_trusted_host_at_node)


        @classmethod
        @contract_epydoc
        def get_all_trusted_hosts(cls,
                                  username_uc_user_map=None,
                                  for_storage=None, for_restore=None,
                                  rdbw=None):
            """Get the iterable over the trusted hosts.

            @param username_uc_user_map: The mapping from
                the uppercased user name to the appropriate C{User} class.
                This mapping will be called directly rather than
                with C{.get()}, so it either must always provide the result
                (and provide C{None} explicitly if needed), or fail.
                If C{username_uc_user_map} is C{None}, the defaultdict
                will be used.
            @type username_uc_user_map: NoneType, col.Mapping

            @param for_storage: whether the expected Trusted Hosts
                must be usable for in-house storage.
                C{True} for "must be usable", C{False} for "must be unusable",
                C{None} for "do not care".
            @type for_storage: bool, NoneType

            @param for_restore: whether the expected Trusted Hosts
                must be usable for web restore process.
                C{True} for "must be usable", C{False} for "must be unusable",
                C{None} for "do not care".
            @type for_restore: bool, NoneType

            @param rdbw: database wrapper. Must NOT be None!
            @type rdbw: DatabaseWrapperSQLAlchemy

            @result: the (possibly, non-reiterable) iterable over
                C{TrustedHostAtNode} objects. Note that only
                the .C{uuid}, C{.for_storage} and C{.for_restore} fields are
                guaranteed to be correct in the object; other fields may be
                missing or empty.
            @rtype: col.Iterable
            """
            if username_uc_user_map is None:
                username_uc_user_map = defaultdict(lambda: None)

            thn = models.trusted_hosts_at_node

            # Create filtering "WHERE" clause dynamically
            where_clause = sqla_true()
            if for_storage is not None:
                # ... AND .for_storage = {for_storage}
                where_clause &= \
                    thn.c.for_storage == (sqla_true() if for_storage
                                                      else sqla_false())
            if for_restore is not None:
                # ... AND .for_restore = {for_restore}
                where_clause &= \
                    thn.c.for_restore == (sqla_true() if for_restore
                                                      else sqla_false())

            cl_select_trusted_host = \
                sql_select(
                    columns=[thn.c.trusted_host_at_node_id,
                             thn.c.for_storage,
                             thn.c.for_restore,
                             cmodels.inhabitants.c.uuid,
                             cmodels.users.c.name.label('user_name')],
                    whereclause=where_clause,
                    from_obj=thn.join(cmodels.inhabitants,
                                      cmodels.inhabitants.c.id ==
                                          thn.c.trusted_host_at_node_id)
                                .join(cmodels.hosts,
                                      cmodels.hosts.c.host_id ==
                                          cmodels.inhabitants.c.id)
                                .join(cmodels.users,
                                      cmodels.users.c.user_id ==
                                          cmodels.hosts.c.user))

            rows = rdbw.execute(cl_select_trusted_host)
            return (cls.__db_trusted_host_at_node_row_to_host(
                            username_uc_user_map, row)
                        for row in rows)


        INSERT_HOST_AT_NODE_VIEW = _dd("""\
            INSERT INTO host_at_node_view (
                    user_id,
                    uuid,
                    urls,
                    name,
                    last_seen
                )
                VALUES (
                    (SELECT id
                         FROM "user"
                         WHERE upper(name) = upper(:user_name)),
                    :uuid,
                    :urls,
                    :name,
                    :last_seen
                )
            """)

        @classmethod
        @contract_epydoc
        def add_peers(cls, hfns):
            """Add the information for multiple HostAtNode objects into the DB.

            @type hfns: col.Iterable
            """
            assert RDB is not None

            with RDB() as rdbw:
                hfns_iter = ({'uuid': peer.uuid,
                              'urls': json.dumps(peer.urls),
                              'name': peer.name,
                              'user_name': peer.user.name,
                              'last_seen': peer.last_seen}
                                 for peer in hfns)
                rdbw.querymany(cls.INSERT_HOST_AT_NODE_VIEW, hfns_iter)


        UPDATE_HOST_AT_NODE_VIEW = _dd("""\
            UPDATE host_at_node_view
                SET
                    urls = :urls,
                    name = :name,
                    last_seen = :last_seen
                WHERE uuid = :uuid
            """)

        @classmethod
        @contract_epydoc
        def update_peers(cls, hfns, timeout=timedelta(seconds=15)):
            """
            Update the information for multiple HostAtNode objects in the DB.
            For an existing host, only the inhabitant urls, host name and
            last_seen fields may be changed.

            @type hfns: col.Iterable
            @type timeout: timedelta
            """
            assert RDB is not None

            with RDB(timeout=timeout.total_seconds()) as rdbw:
                hfns_iter = ({'uuid': peer.uuid,
                              'urls': json.dumps(peer.urls),
                              'name': peer.name,
                              'last_seen': peer.last_seen}
                                 for peer in hfns)
                rdbw.querymany(cls.UPDATE_HOST_AT_NODE_VIEW, hfns_iter)



    class TrustedDatasets(Queries.Datasets):
        """All DB (node-only) queries related to the backup datasets."""

        __slots__ = ()


        @classmethod
        @contract_epydoc
        def get_completed_datasets_by_username(cls, username, rdbw):
            """Given a name of the user, get their completed datasets.

            @param username: the name of the user.
            @type username: basestring

            @param rdbw: database wrapper.
            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the non-reiterable iterable over datasets.
            @rtype: col.Iterable
            """
            m = models

            dataset_list_select =\
                sql_select(columns=[m.datasets.c.uuid.label('uuid'),
                                    m.datasets.c.time_started.label('start'),
                                    func.count(m.file_locals.c.id)
                                        .label('count')],
                           whereclause=((m.user_groups.c.name == username) &
                           # ATTENTION! Please, do not touch this line
                           # even you think, it can be written as 'is not'!
                           # SQLAlchemy must get this to provide it to SQL!
                                        (m.datasets.c.time_completed != None)),
                           from_obj=m.datasets
                                     .join(m.user_groups)
                                     .join(m.base_directories)
                                     .join(m.file_locals))\
                    .group_by(m.datasets.c.uuid,
                              m.datasets.c.time_started)\
                    .order_by(m.datasets.c.time_started)
            return rdbw.execute(dataset_list_select)


        CREATE_DATASET_ON_HOST_VIEW = _dd("""
            CREATE VIEW dataset_on_host_view AS
                SELECT
                    dataset_on_host.id AS id,
                    dataset.uuid AS dataset_uuid,
                    host_view.uuid AS host_uuid
                FROM
                    dataset_on_host
                    INNER JOIN dataset
                        ON dataset.id = dataset_on_host.dataset_id
                    INNER JOIN host_view
                        ON host_view.id = dataset_on_host.host_id
            """)


        __IS_DATASET_SYNCED_TO_HOST = _dd("""\
            SELECT dataset.id, host_view.id
                FROM
                    dataset
                    INNER JOIN dataset_on_host
                        ON dataset_on_host.dataset_id = dataset.id
                    INNER JOIN host_view
                        ON host_view.id = dataset_on_host.host_id
                WHERE
                    dataset.uuid = :ds_uuid AND
                    host_view.uuid = :host_uuid
            """)

        __MARK_DATASET_AS_SYNCED_TO_HOST = _dd("""\
            INSERT INTO dataset_on_host(dataset_id, host_id, time)
                VALUES (
                    (SELECT id FROM dataset WHERE uuid = :ds_uuid),
                    (SELECT id FROM host_view WHERE uuid = :host_uuid),
                    :time
                )
            """)

        @classmethod
        @contract_epydoc
        def mark_dataset_as_synced_to_host(cls,
                                           dataset_uuid, host_uuid, rdbw):
            """Mark that dataset was synced to the host.

            @type dataset_uuid: UUID
            @type host_uuid: UUID
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            # Let's check it is not marked yet.
            logger.debug('Is dataset %r synced to host %r?',
                         dataset_uuid, host_uuid)
            row_is_synced = \
                rdbw.query_value(cls.__IS_DATASET_SYNCED_TO_HOST,
                                 {'ds_uuid': dataset_uuid,
                                  'host_uuid': host_uuid})
            if row_is_synced is None:
                logger.debug('No, not synced')
                rdbw.query(cls.__MARK_DATASET_AS_SYNCED_TO_HOST,
                           {'ds_uuid': dataset_uuid,
                            'host_uuid': host_uuid,
                            'time': datetime.utcnow()})
            else:
                logger.debug('Yes, synced: %r', row_is_synced)


        @classmethod
        @contract_epydoc
        def get_ds_uuids_not_synced_to_host(cls, host_uuid, rdbw):
            """
            Return the iterable over the dataset UUIDs for datasets which
            should be restored to this host cause they are not synced yet.
            The returned datasets are sync-created (dataset.sync = TRUE)
            and fully completed.

            @type host_uuid: UUID
            @type rdbw: DatabaseWrapperSQLAlchemy
            @rtype: col.Iterable
            """
            select_expr = models.DatasetOnHost \
                                .select_datasets_not_synced_to_host_expr()
            rows = rdbw.execute(select_expr,
                                {'host_uuid': host_uuid})
            return (r.ds_uuid for r in rows)


        @classmethod
        @contract_epydoc
        def get_host_uuids_lacking_sync_dataset(cls, ds_uuid, rdbw):
            """
            Return the iterable over the host UUIDs which do not contain
            the dataset and require it to be cloned to them (if it is a sync
            dataset).

            @param ds_uuid: the UUID of the Dataset.
            @type ds_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            """
            select_expr = models.DatasetOnHost \
                                .select_host_uuids_lacking_sync_dataset_expr()
            rows = rdbw.execute(select_expr, {'ds_uuid': ds_uuid})
            return (r.uuid for r in rows)


        @classmethod
        @contract_epydoc
        def merge_sync_datasets(cls, host_uuid, ds_uuids, rdbw):
            """
            Merge some ("sync-style") datasets into new one(s),
            and rebind their "dataset_on_host" sync presence properly.

            If we have multiple sync-style datasets that are absent on some
            specific host (C{host_uuid}), these datasets are grouped
            so that each group (where the host belongs) gets no more than one
            dataset.

            @note: for consistency, B{all} of the code below must occur
                within the same SQL transaction.

            @param host_uuid: the UUID of the host, whose datasets
                are being merged.
            @type host_uuid: HostUUID

            @param ds_uuids: the iterable of UUIDs of the datasets
                to be merged.
            @type ds_uuids: col.Iterable

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the iterable over the UUIDs of newly created datasets.
            @rtype: col.Iterable
            """
            m = models

            # Force evaluation for reiterability
            ds_uuids = frozenset(ds_uuids)

            # We should've get validated these UUIDs before, but double-check.
            # Select all non-synced DS-UUIDs.
            _incl_ds_expr = \
                m.DatasetOnHost.select_datasets_not_synced_to_host_expr()\
                               .alias()
                # bindparam: host_uuid

            # Present UUIDs as well, bound to the same (!) host.
            _excl_ds_expr = \
                m.DatasetOnHost.select_ds_uuids_groups_synced_to_host_expr()\
                               .alias()
                # bindparam: host_uuid

            _ds_incl_rows = rdbw.execute(_incl_ds_expr,
                                         {'host_uuid': host_uuid})
            # _incl_ds_expr contains ALL non-synced UUIDs, but we need only
            # ds_uuids
            _ds_incl_rows_filtered = (row for row in _ds_incl_rows
                                          if row.ds_uuid in ds_uuids)
            ds_incl_rows_by_ugroup_uuid = \
                groupby_to_dict(_ds_incl_rows_filtered,
                                attrgetter('ugroup_uuid'))

            _ds_excl_rows = rdbw.execute(_excl_ds_expr,
                                         {'host_uuid': host_uuid})
            ds_excl_rows_by_ugroup_uuid = \
                groupby_to_dict(_ds_excl_rows, attrgetter('ugroup_uuid'))

            # Help GC
            del _ds_incl_rows, _ds_excl_rows

            # But what if we requested to merge some datasets which are NOT
            # among missing ones?
            _incl_uuids = \
                {row.ds_uuid
                     for per_group in ds_incl_rows_by_ugroup_uuid.itervalues()
                     for row in per_group}
            if not ds_uuids.issubset(_incl_uuids):
                logger.warning('Requested to merge datasets %r, but missing '
                                   'on the host are %r; what to do with %r?',
                               ds_uuids, _incl_uuids, ds_uuids - _incl_uuids)
            del _incl_uuids  # help GC

            # All validations more-or-less succeeded, go on with merging.

            result_ds_uuids = []

            # There are multiple datasets missing from this host.
            # They need to be merged, but only within same user groups.
            for group_uuid, ds_incl_rows_per_group \
                    in ds_incl_rows_by_ugroup_uuid.iteritems():
                # Will reuse multiple time, so force evaluation
                ds_incl_rows = list(ds_incl_rows_per_group)
                # Will not reuse.
                ds_excl_rows = ds_excl_rows_by_ugroup_uuid.get(group_uuid, [])

                if ds_incl_rows:
                    # Will create dataset indeed
                    time_started = min(r.ds_time_started
                                           for r in ds_incl_rows)
                    time_completed = max(r.ds_time_completed
                                             for r in ds_incl_rows)
                    ds_incl_uuids = [r.ds_uuid for r in ds_incl_rows]
                    ds_excl_uuids = [r.ds_uuid for r in ds_excl_rows]

                    new_ds_uuid = cls._merge_same_group_sync_datasets(
                                      host_uuid,
                                      UserGroupUUID.safe_cast_uuid(group_uuid),
                                      ds_incl_uuids, ds_excl_uuids,
                                      time_started, time_completed,
                                      rdbw)
                    result_ds_uuids.append(new_ds_uuid)

            return result_ds_uuids


        @classmethod
        @duration_logged()
        @contract_epydoc
        def _merge_same_group_sync_datasets(
                cls, host_uuid, ugroup_uuid, ds_incl_uuids, ds_excl_uuids,
                time_started, time_completed, rdbw):
            """
            Merge some ("sync-style") datasets (which definitely belong
            to the same group!) into new one(s),
            and rebind their "dataset_on_host" sync presence properly.

            @param host_uuid: the UUID of the host, whose datasets
                are being merged.
            @type host_uuid: HostUUID

            @param ugroup_uuid: the UUID of the User Group common
                for the datasets.
            @type ugroup_uuid: UserGroupUUID

            @param ds_incl_uuids: the iterable of UUIDs of the datasets
                to be added together to result.
            @type ds_incl_uuids: col.Iterable

            @param ds_excl_uuids: the iterable of UUIDs of the datasets
                to be substracted from the result.
            @type ds_excl_uuids: col.Iterable

            @param time_started: the desired time of dataset start.
            @type time_started: datetime

            @param time_completed: the desired time of dataset end.
            @type time_completed: datetime

            @type rdbw: DatabaseWrapperSQLAlchemy

            @return: the UUID of the new dataset.
            @rtype: DatasetUUID

            @todo: someday this should be enabled to delete the directories
                recursively if they are deleted.
            """
            # Now we need to find data to include, and to find data
            # to exclude.
            # RESULT = (Dincl1 | Dincl2 | ...) - (Dexcl1 | Dexcl2 | ...)

            # That is (very high-level):
            # 1. we take the latest states of every file in ds_incl_uuids
            #    as incl_files;
            # 2. we take the latest states of every file in ds_excl_uuids
            #    as excl_files;
            # 3. result will be:
            #    [file for file in incl_files if file not in excl_files]

            m = models

            # will reuse multiple times
            ds_incl_uuids = list(ds_incl_uuids)
            ds_excl_uuids = list(ds_excl_uuids)

            logger.debug('Will merge datasets for group %s from %s to %s: '
                             '+(%r), -(%r)',
                         ugroup_uuid, time_started, time_completed,
                         ds_incl_uuids, ds_excl_uuids)

            new_ds_uuid = DatasetUUID.safe_cast_uuid(gen_uuid())

            #
            # [1/3]: So, let's create a new dataset.
            #
            Queries.Datasets.add_dataset(
                ugroup_uuid=ugroup_uuid,
                name='', ds_uuid=new_ds_uuid, sync=True, merged=True,
                time_started=time_started, time_completed=time_completed,
                rdbw=rdbw)
            logger.debug('Added a new merged dataset %s', new_ds_uuid)

            # Get the dataset ID explicitly.
            inserted_ds = Queries.Datasets.get_dataset_by_uuid(
                              dataset_uuid=new_ds_uuid,
                              host_uuid=host_uuid, rdbw=rdbw)

            assert inserted_ds is not None
            _dataset_id = inserted_ds._id
            _group_id = inserted_ds._ugroup_id
            # We've added a new dataset, great!
            # Now let's merge base directories...

            #
            # [2/3]: Add base directories.
            #

            # TODO: ticket:244, fix when multiple base directories
            # are supported!
            # For now, all base dirs are merged into a single one :(
            sole_base_dir_record = {
                'dataset': _dataset_id,
                'path': common_data_ops.DEFAULT_SYNC_BASE_DIR_NAME
            }
            rdbw.execute(m.base_directories.insert(),
                         [sole_base_dir_record])
            # And what we've just added?
            select_just_added_base_dir_id_expr = \
                sql_select([m.base_directories.c.id],
                           whereclause=(m.base_directories.c.dataset
                                        == bindparam('dataset')) &
                                       (m.base_directories.c.path
                                        == bindparam('path')))
            _base_dir_id = rdbw.execute(select_just_added_base_dir_id_expr,
                                        sole_base_dir_record).scalar()

            #
            # [3/3]: Add file_local-s
            #

            # Get the latest file local state for each file_local
            # among all the passed datasets.
            # Note: for now, the latest times of file states are taken
            # from the datasets.
            latest_states_incl_expr = \
                sql_select([sql_functions.max(m.datasets.c.time_started)
                                         .label('latest_file_time'),
                            m.files_local.c.rel_path],
                           from_obj=m.datasets.join(m.base_directories)
                                              .join(m.files_local),
                           whereclause=m.datasets.c.uuid.in_(ds_incl_uuids) &
                                       (m.datasets.c.group
                                            == bindparam('_group_id')),
                           group_by=[m.files_local.c.rel_path]) \
                    .alias()
                # bindparam: _group_id

            latest_states_excl_expr = \
                sql_select([sql_functions.max(m.datasets.c.time_started)
                                         .label('latest_file_time'),
                            m.files_local.c.rel_path],
                           from_obj=m.datasets.join(m.base_directories)
                                              .join(m.files_local),
                           whereclause=m.datasets.c.uuid.in_(ds_excl_uuids) &
                                       (m.datasets.c.group
                                            == bindparam('_group_id')),
                           group_by=[m.files_local.c.rel_path]) \
                    .alias()
                # bindparam: _group_id

            # Here we've found the latest states for every file.
            # We could've excluded the _excl-files even here;
            # but this would mean that we exclude the files if they were
            # last changed at the same time as the included one.
            # This is unlikely in reality; but it is likely that the file
            # is considered last changed (note, for now we use dataset's time
            # as a "last changed" mark!) in different time, but still has
            # the same filename and contents!
            #
            # Thus we'll exclude the files whose "_excl" version exists
            # and has the same fingerprint (!) as their "_incl" counterpart

            # Note bindparam('base_dir') !
            select_latest_file_states_incl_expr = \
                sql_select(
                    [m.files_local.c.file.label('file'),
                     bindparam('base_dir').label('base_directory'),
                     m.files_local.c.isdir.label('isdir'),
                     m.files_local.c.rel_path.label('rel_path'),
                     m.files_local.c.attrs.label('attrs')],
                    from_obj=m.datasets
                              .join(m.base_directories)
                              .join(m.files_local)
                              .join(latest_states_incl_expr,
                                    (latest_states_incl_expr.c.latest_file_time
                                     == m.datasets.c.time_started) &
                                    (latest_states_incl_expr.c.rel_path
                                     == m.files_local.c.rel_path)),
                    whereclause=m.datasets.c.uuid.in_(ds_incl_uuids) &
                                (m.datasets.c.group
                                     == bindparam('_group_id')))
                # bindparam: _group_id, ^^^base_dir

            # Note bindparam('base_dir') !
            select_latest_file_states_excl_expr = \
                sql_select(
                    [m.files_local.c.file.label('file'),
                     bindparam('base_dir').label('base_directory'),
                     m.files_local.c.isdir.label('isdir'),
                     m.files_local.c.rel_path.label('rel_path'),
                     m.files_local.c.attrs.label('attrs')],
                    from_obj=m.datasets
                              .join(m.base_directories)
                              .join(m.files_local)
                              .join(latest_states_excl_expr,
                                    (latest_states_excl_expr.c.latest_file_time
                                     == m.datasets.c.time_started) &
                                    (latest_states_excl_expr.c.rel_path
                                     == m.files_local.c.rel_path)),
                    whereclause=m.datasets.c.uuid.in_(ds_excl_uuids) &
                                (m.datasets.c.group
                                     == bindparam('_group_id')))
                # bindparam: _group_id, ^^^base_dir

            # We add all "_incl" files; but if this file (precisely this,
            # with the same filename, isdir flag and "file" FK i.e. contents,
            # and even the same attrs) exists in "_excl", we exclude it.
            #
            # Another hint: obviously, we don't need to even do any "excl"
            # calculations, if "ds_excl_uuids" is empty,
            # i.e. False in boolean contents.
            if True:  # someday when stable, fix it to "if ds_excl_uuids:"
                _incl = select_latest_file_states_incl_expr.alias()
                _excl = select_latest_file_states_excl_expr.alias()
                select_latest_file_states_to_insert_expr = \
                    sql_select(
                        [_incl.c.file,
                         _incl.c.base_directory,
                         _incl.c.isdir,
                         _incl.c.rel_path,
                         _incl.c.attrs],
                        from_obj=_incl.join(_excl,
                                            (_incl.c.file == _excl.c.file) &
                                            (_incl.c.base_directory
                                                 == _excl.c.base_directory) &
                                            (_incl.c.isdir == _excl.c.isdir) &
                                            (_incl.c.rel_path
                                                 == _excl.c.rel_path) &
                                            (_incl.c.attrs == _excl.c.attrs),
                                            isouter=True),
                        whereclause=(_excl.c.file == None))
                        # note "==" instead of "is", for SQLAlchemy.
            else:
                select_latest_file_states_to_insert_expr = \
                    select_latest_file_states_incl_expr

            insert_from_select_expr = \
                insert_from_select([m.files_local.c.file,
                                    m.files_local.c.base_directory,
                                    m.files_local.c.isdir,
                                    m.files_local.c.rel_path,
                                    m.files_local.c.attrs],
                                   select_latest_file_states_to_insert_expr)

            rdbw.execute(insert_from_select_expr,
                         {'base_dir': _base_dir_id,
                          '_group_id': _group_id})

            #
            # [Final] now let's rebind the datasets:
            #
            cls._rebind_sync_datasets_to_dataset(
                ugroup_uuid, ds_incl_uuids, new_ds_uuid, rdbw)

            return new_ds_uuid


        @classmethod
        @contract_epydoc
        def _rebind_sync_datasets_to_dataset(
                cls, ugroup_uuid, src_ds_uuids, dst_ds_uuid, rdbw):
            """
            Given a sequence of unmerged (sync) dataset UUIDs (C{src_ds_uuids))
            and and UUID of the merged (sync) dataset (C{dst_ds_uuid}),
            do the following:

            1. Get all the hosts which belong to the common user group
               (passed by C{ugroup_uuid}).

            2. Re-mark the presence of the datasets where applicable
               (see below). Note these actions cannot be mass-applied in turn,
               instead, the calculations of the hosts being modified on both
               steps B{must} be performed before any modification is made.

              2.1. For every host which has B{none} of the unmerged
                   datasets,
                   B{mark} all the unmerged datasets as present on this host,
                   B{do not mark} the merged dataset.

              2.2. For every host which has B{any} of the unmerged
                   datasets,
                   B{mark} the merged dataset as present on this host,
                   B{do not mark} all the unmerged datasets.

            @param ugroup_uuid: the UUID of the User Group common
                for the datasets.
            @type ugroup_uuid: UserGroupUUID

            @param src_ds_uuids: the iterable of UUIDs of the unmerged datasets
                to be merged.
            @type src_ds_uuids: col.Iterable

            @param dst_ds_uuid: the UUID of the new, merged dataset.
            @type dst_ds_uuid: DatasetUUID

            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            m = models

            src_ds_uuids = list(src_ds_uuids)  # eagerly evaluate

            # 1. Get the hosts.

            all_host_uuids_expr = \
                m.UserGroup.select_host_uuids_by_user_group_uuid_expr().alias()
            # bindparam('user_group_uuid')

            all_host_rows = rdbw.execute(all_host_uuids_expr,
                                         {'user_group_uuid': ugroup_uuid})

            all_host_uuids = frozenset(r.uuid for r in all_host_rows)

            hosts_with_at_least_one_ds_expr = \
                sql_select([m.inhabitants.c.uuid],
                           distinct=True,
                           from_obj=m.inhabitants.join(m.hosts)
                                                 .join(m.datasets_on_host)
                                                 .join(m.datasets),
                           whereclause=
                               m.inhabitants.c.uuid.in_(all_host_uuids) &
                               m.datasets.c.uuid.in_(src_ds_uuids))

            hosts_with_at_least_one_ds_rows = \
                rdbw.execute(hosts_with_at_least_one_ds_expr,
                             {'user_group_uuid': ugroup_uuid})
            host_uuids_with_at_least_one_ds = \
                frozenset(r.uuid for r in hosts_with_at_least_one_ds_rows)

            host_uuids_with_no_dses = \
                all_host_uuids - host_uuids_with_at_least_one_ds

            logger.debug('While merging %i dataset(s) (%r) into %s, '
                             'among %i host(s) (%r) %i one(s) (%r) had its '
                             "parts, and %i one(s) (%r) hadn't any.",
                         len(all_host_uuids), all_host_uuids, dst_ds_uuid,
                         len(all_host_uuids), all_host_uuids,
                         len(host_uuids_with_at_least_one_ds),
                         host_uuids_with_at_least_one_ds,
                         len(host_uuids_with_no_dses),
                         host_uuids_with_no_dses)

            # 2.1. For every host in C{host_uuids_with_no_dses},
            #      bind all C{src_ds_uuids}
            # 2.2. For every host in C{host_uuids_with_at_least_one_ds},
            #      bind C{dst_ds_uuid}

            # We know the dataset UUIDs and host UUIDs, but, to mass-insert,
            # we must know the foreign keys.
            select_2_1_expr = \
                sql_select([m.datasets.c.id.label('dataset_id'),
                            m.inhabitants.c.id.label('host_id')],
                           whereclause=m.datasets.c.uuid.in_(src_ds_uuids) &
                                       m.inhabitants.c.uuid.in_(
                                           host_uuids_with_no_dses))
            select_2_2_expr = \
                sql_select([m.datasets.c.id.label('dataset_id'),
                            m.inhabitants.c.id.label('host_id')],
                           whereclause=(m.datasets.c.uuid == dst_ds_uuid) &
                                       m.inhabitants.c.uuid.in_(
                                           host_uuids_with_at_least_one_ds))

            insert_from_select_expr = \
                insert_from_select([m.datasets_on_host.c.dataset,
                                    m.datasets_on_host.c.host],
                                   union(select_2_1_expr, select_2_2_expr))

            rdbw.execute(insert_from_select_expr)



    class SpaceStat(object):
        """All DB (node-only) queries related to the storage statistics."""

        __slots__ = tuple()

        __CREATE_MAX_STORAGE_SPACE_VIEW_SQLITE = _dd("""\
            CREATE VIEW max_storage_space_view AS
                SELECT
                    host_id,
                    value * 1048576.0 AS max_size
                FROM setting_view
                WHERE name = '{}'
            """).format(Queries.Settings.MAX_STORAGE_SIZE_MIB)

        __CREATE_MAX_STORAGE_SPACE_VIEW_POSTGRESQL = _dd("""\
            CREATE VIEW max_storage_space_view AS
                SELECT
                    host_id,
                    to_number(concat('0', value), 'S999999999999999D99')
                        * 1048576.0 AS max_size
                FROM setting_view
                WHERE name = '{}'
            """).format(Queries.Settings.MAX_STORAGE_SIZE_MIB)

        CREATE_MAX_STORAGE_SPACE_VIEW = {
            'sqlite': __CREATE_MAX_STORAGE_SPACE_VIEW_SQLITE,
            'postgresql': __CREATE_MAX_STORAGE_SPACE_VIEW_POSTGRESQL
        }

        # If the used space is higher than the max size,
        # the free space will be 0.0
        CREATE___USED_SPACE_STAT_VIEW = _dd("""\
            CREATE VIEW __used_space_stat_view AS
                SELECT
                    host.id AS host_id,
                    coalesce(sum(chunk_per_host_view.chunk_size), 0.0)
                        AS used_size
                FROM
                    host
                    LEFT JOIN chunk_per_host_view
                        ON chunk_per_host_view.host_id = host.id
                GROUP BY host.id
            """)

        __CREATE_SPACE_STAT_VIEW_SQLITE = _dd("""\
            CREATE VIEW space_stat_view AS
                SELECT
                    inhabitant.id AS host_id,
                    inhabitant.uuid AS host_uuid,
                    max_view.max_size AS max_size,
                    used_view.used_size AS used_size,
                    max(max_view.max_size - used_view.used_size, 0.0)
                        AS free_size
                FROM
                    inhabitant
                    INNER JOIN host
                        USING(id)
                    INNER JOIN max_storage_space_view AS max_view
                        ON max_view.host_id = host.id
                    INNER JOIN __used_space_stat_view AS used_view
                        ON used_view.host_id = host.id
            """)

        __CREATE_SPACE_STAT_VIEW_POSTGRESQL = _dd("""\
            CREATE VIEW space_stat_view AS
                SELECT
                    inhabitant.id AS host_id,
                    inhabitant.uuid AS host_uuid,
                    max_view.max_size AS max_size,
                    used_view.used_size AS used_size,
                    greatest(max_view.max_size - used_view.used_size, 0.0)
                        AS free_size
                FROM
                    inhabitant
                    INNER JOIN host
                        USING(id)
                    INNER JOIN max_storage_space_view AS max_view
                        ON max_view.host_id = host.id
                    INNER JOIN __used_space_stat_view AS used_view
                        ON used_view.host_id = host.id
            """)

        # Sqlite3 can use function max() where PostgreSQL uses greatest()
        CREATE_SPACE_STAT_VIEW = {
            'sqlite': __CREATE_SPACE_STAT_VIEW_SQLITE,
            'postgresql': __CREATE_SPACE_STAT_VIEW_POSTGRESQL
        }


        SELECT_HOSTS_SPACE_STAT = _dd("""\
            SELECT host_uuid, max_size, used_size, free_size
                FROM space_stat_view
            """)

        HostsSpaceStatType = namedtuple('namedtuple',
                                        ('max_size', 'used_size', 'free_size'))


        @classmethod
        @contract_epydoc
        def __db_parse_hosts_space_stat_data(cls, row):
            """
            @returns: a tuple for the dict.
            @rtype: tuple
            """
            return (row.host_uuid,
                    cls.HostsSpaceStatType(max_size=long(row.max_size),
                                           used_size=long(row.used_size),
                                           free_size=long(row.free_size)))


        @classmethod
        @contract_epydoc
        def get_hosts_space_stat(cls):
            """Get a mapping from host UUID to the space statistics.

            @todo: are both C{get_hosts_space_stat} and
                C{get_hosts_space_stat2} redundant?

            @rtype: dict
            @postcondition: consists_of(result.itervalues(), \
                            TrustedQueries.SpaceStat.HostsSpaceStatType)
            """
            assert RDB is not None

            with RDB() as rdbw:
                return dict(imap(cls.__db_parse_hosts_space_stat_data,
                                 rdbw.query(cls.SELECT_HOSTS_SPACE_STAT)))


        GetHostsSpaceStat2Result = \
            namedtuple('GetHostsSpaceStat2Result',
                       ('host_name', 'max_size', 'used_size'))

        @classmethod
        @contract_epydoc
        def get_hosts_space_stat2(cls, group_uuid, rdbw):
            """Get host space stat for a group (by its UUID).

            @todo: are both C{get_hosts_space_stat} and
                C{get_hosts_space_stat2} redundant?

            @type group_uuid: UUID

            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Iterable
            @return: an iterable of C{GetHostsSpaceStat2Result}.
            """
            m = models

            groups_hosts_max_storage_space_var = \
                m.User.personal_groups_hosts_max_storage_space_view() \
                 .alias('groups_hosts_max_storage_space')

            chunk_per_host_var = \
                m.ChunkPerHost.chunk_per_host_view() \
                 .alias('chunk_per_host_view')

            used_on_group_hosts = \
                sql_select([m.hosts.c.host_id.label('host_id'),
                            func.coalesce(func.sum(chunk_per_host_var
                                                       .c.chunk_size),
                                          0)
                                .label('used_on_size')],
                           from_obj=m.hosts
                                     .join(chunk_per_host_var,
                                           m.hosts.c.host_id ==
                                               chunk_per_host_var.c.host_id,
                                           isouter=True))\
                    .group_by(m.hosts.c.host_id)\
                    .alias('used_on_group_hosts')

            user_space_select = \
                sql_select([m.hosts.c.name.label('host_name'),
                            groups_hosts_max_storage_space_var.c.max_size
                                .label('max_size'),
                            used_on_group_hosts.c.used_on_size
                                .label('used_on_size')],
                           (m.user_groups.c.uuid == group_uuid),
                           from_obj=m.hosts
                                     .join(groups_hosts_max_storage_space_var,
                                           groups_hosts_max_storage_space_var
                                               .c.host_id == m.hosts.c.host_id)
                                     .join(m.users,
                                           m.hosts.c.user ==
                                               m.users.c.user_id)
                                     .join(m.user_groups)
                                     .join(used_on_group_hosts,
                                           m.hosts.c.host_id ==
                                               used_on_group_hosts.c.host_id))

            return (cls.GetHostsSpaceStat2Result(host_name=row.host_name,
                                                 max_size=row.max_size,
                                                 used_size=row.used_on_size)
                        for row in rdbw.execute(user_space_select))


        PerGroupSpaceStatResult = \
            namedtuple('PerGroupSpaceStatResult',
                       ('group_uuid', 'group_name', 'max_size',
                        'file_hold_size', 'file_uniq_size'))

        @classmethod
        def get_per_group_space_stat(cls, rdbw):
            """Get the space stats per every available user group.

            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: an iterable of
                C{TrustedQueries.SpaceStat.SpaceStatResult} objects.
            """
            m = models

            user_used_space_var = \
                models.User.group_used_space_view() \
                      .alias('group_used_space_view')

            groups_hosts_max_storage_space_var = \
                models.User.personal_groups_hosts_max_storage_space_view() \
                      .alias('groups_hosts_max_storage_space')

            groups_max_storage_space_var = \
                sql_select(
                    [groups_hosts_max_storage_space_var.c.group_name,
                     groups_hosts_max_storage_space_var.c.group_uuid,
                     func.sum(groups_hosts_max_storage_space_var.c.max_size)
                         .label('max_size')],
                    from_obj=groups_hosts_max_storage_space_var)\
                .group_by(groups_hosts_max_storage_space_var.c.group_name,
                          groups_hosts_max_storage_space_var.c.group_uuid)\
                .order_by(groups_hosts_max_storage_space_var.c.group_name)\
                .alias('group_max_storage_space')

            user_space_select = \
                sql_select(
                    [groups_max_storage_space_var.c.group_uuid,
                     groups_max_storage_space_var.c.group_name,
                     groups_max_storage_space_var.c.max_size,
                     user_used_space_var.c.file_hold_size,
                     user_used_space_var.c.file_uniq_size],
                    from_obj=groups_max_storage_space_var
                                 .join(user_used_space_var,
                                       user_used_space_var.c.group_name ==
                                           groups_max_storage_space_var
                                               .c.group_name))

            return (cls.PerGroupSpaceStatResult(
                            group_uuid=row.group_uuid,
                            group_name=row.group_name,
                            max_size=row.max_size,
                            file_hold_size=row.file_hold_size,
                            file_uniq_size=row.file_uniq_size)
                        for row in rdbw.execute(user_space_select))



    class ChunksReplicaStat(object):
        """All DB (node-only) queries related to the replication statistics."""

        __slots__ = tuple()


        # The candidates are the hosts which don't have this chunk yet.
        #
        # ATTENTION: a chunk present in "chunk" table may be missing
        # from this view, if it does not have any candidate hosts to upload to.
        CREATE_CHUNK_CANDIDATES_VIEW = _dd("""\
            CREATE VIEW chunk_candidates_view AS
                SELECT
                    chunk.id AS chunk_id,
                    host_view.id AS host_id
                FROM
                    chunk
                    CROSS JOIN host_view
                    LEFT JOIN chunk_per_host
                        ON chunk.id = chunk_per_host.chunk_id AND
                           host_view.id = chunk_per_host.host_id
                WHERE
                    chunk_per_host.id IS NULL
            """)

        # Only the chunks which are stored at least on a single host
        # in the cloud are present in this view.
        CREATE_CHUNK_REPLICA_STAT_VIEW = _dd("""\
            CREATE VIEW chunk_replica_stat_view AS
                SELECT
                    chunk_id,
                    count(*) AS chunk_replica_count
                FROM chunk_per_host_view
                GROUP BY chunk_id
            """)


        # Parameters:
        # :max_replicas - max replicas
        # :max_chunks - max chunks to return
        SELECT_SOME_CHUNKS_TO_REPLICATE = _dd("""\
            SELECT
                    chunk.uuid AS chunk_uuid,
                    chunk.size AS chunk_size,
                    chunk_per_host_view.chunk_hash AS chunk_hash,
                    chunk_per_host_view.chunk_crc32 AS chunk_crc32,
                    string_agg(DISTINCT CAST(chunk_per_host_view.host_uuid
                                                 AS TEXT),
                               ',')
                        AS src_hosts,
                    string_agg(DISTINCT CAST(chunk_candidate_host.uuid
                                                 AS TEXT),
                               ',')
                        AS dst_hosts
                FROM
                    chunk
                    INNER JOIN chunk_replica_stat_view AS repl_view
                        ON repl_view.chunk_id = chunk.id
                    INNER JOIN chunk_per_host_view
                        ON chunk_per_host_view.chunk_id = repl_view.chunk_id
                    INNER JOIN chunk_candidates_view
                        ON chunk_candidates_view.chunk_id = repl_view.chunk_id
                    INNER JOIN host_view AS chunk_candidate_host
                        ON chunk_candidate_host.id =
                           chunk_candidates_view.host_id
                WHERE repl_view.chunk_replica_count BETWEEN 1 AND :max_replicas
                GROUP BY
                    chunk.id,
                    chunk.uuid,
                    chunk.size,
                    chunk_per_host_view.chunk_hash,
                    chunk_per_host_view.chunk_crc32,
                    repl_view.ORDER
                chunk_replica_count BY
                    repl_view.chunk_replica_count ASC,
                    chunk.id ASC
                LIMIT :max_chunks
            """)

        ChunkReplicaStatType = \
            namedtuple('namedtuple',
                       ('chunk', 'src_host_uuids', 'dst_host_uuids'))

        @classmethod
        @contract_epydoc
        def get_some_chunk_data_for_replication(cls, max_replicas, max_chunks,
                                                rdbw):
            """
            @type rdbw: DatabaseWrapperSQLAlchemy

            @returns: a non-reiterable Iterable of cls.ChunkReplicaStatType
            @rtype: col.Iterable
            """
            _mkuuid = ChunkUUID.safe_cast_uuid  # preliminary optimization
            _rtype = cls.ChunkReplicaStatType  # another one

            rows = rdbw.query(cls.SELECT_SOME_CHUNKS_TO_REPLICATE,
                              {'max_replicas': max_replicas,
                               'max_chunks': max_chunks})

            return (_rtype(chunk=ChunkInfo(  # ChunkInfo-specific
                                           crc32=crc32_to_unsigned(
                                                     row.chunk_crc32),
                                             # Chunk-specific
                                           uuid=_mkuuid(row.chunk_uuid),
                                           hash=row.chunk_hash,
                                           size=row.chunk_size),
                           src_host_uuids=map(HostUUID,
                                              row.src_hosts.split(',')),
                           dst_host_uuids=map(HostUUID,
                                              row.dst_hosts.split(',')))
                        for row in rows)



    class TrafficStat(object):
        """All DB (node-only) queries related to the traffic statistics."""

        __slots__ = tuple()


        CREATE_CHUNK_TRAFFIC_STAT_VIEW = _dd("""\
            CREATE VIEW chunk_traffic_stat_view AS
                SELECT
                    chunk_traffic_stat.id AS id,

                    src_host_view.id AS src_host_id,
                    src_host_view.uuid AS src_host_uuid,
                    dst_host_view.id AS dst_host_id,
                    dst_host_view.uuid AS dst_host_uuid,

                    chunk.id AS chunk_id,
                    chunk.uuid AS chunk_uuid,
                    chunk.size AS chunk_size,

                    chunk_traffic_stat.time_completed AS time_completed,
                    chunk_traffic_stat.bps AS bps
                FROM
                    chunk_traffic_stat
                    INNER JOIN host_view AS src_host_view
                        ON chunk_traffic_stat.src_host_id = src_host_view.id
                    INNER JOIN host_view AS dst_host_view
                        ON chunk_traffic_stat.dst_host_id = dst_host_view.id
                    INNER JOIN chunk
                        ON chunk_traffic_stat.chunk_id = chunk.id
            """)

        # To insert the chunk_traffic_stat records, you need
        # to know only the:
        # * src_host_uuid,
        # * dst_host_uuid,
        # * chunk_uuid,
        # * time_completed,
        # * bps.
        # The foreign keys are calculated upon them.
        __CREATE_CHUNK_TRAFFIC_STAT_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS chunk_traffic_stat_view_insert_trigger
                INSTEAD OF INSERT ON chunk_traffic_stat_view
                FOR EACH ROW
                BEGIN
                    INSERT OR ROLLBACK INTO chunk_traffic_stat(
                            src_host_id,
                            dst_host_id,
                            chunk_id,
                            time_completed,
                            bps
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM
                                        host
                                        INNER JOIN inhabitant
                                            USING(id)
                                    WHERE inhabitant.uuid = new.src_host_uuid
                            ),
                            (
                                SELECT id
                                    FROM
                                        host
                                        INNER JOIN inhabitant
                                            USING(id)
                                    WHERE inhabitant.uuid = new.dst_host_uuid
                            ),
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE chunk.uuid = new.chunk_uuid
                            ),
                            new.time_completed,
                            new.bps
                        );
                END
            """)

        # Delete only the chunk_traffic_stat_view mapping
        # rather than the anything besides.
        __CREATE_CHUNK_TRAFFIC_STAT_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS chunk_traffic_stat_view_delete_trigger
                INSTEAD OF DELETE ON chunk_traffic_stat_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM chunk_traffic_stat WHERE id = old.id;
                END
            """)

        __CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGER_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION chunk_traffic_stat_view_trigger_proc()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO chunk_traffic_stat(
                            src_host_id,
                            dst_host_id,
                            chunk_id,
                            time_completed,
                            bps
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM
                                        host
                                        INNER JOIN inhabitant
                                            USING(id)
                                    WHERE inhabitant.uuid = new.src_host_uuid
                            ),
                            (
                                SELECT id
                                    FROM
                                        host
                                        INNER JOIN inhabitant
                                            USING(id)
                                    WHERE inhabitant.uuid = new.dst_host_uuid
                            ),
                            (
                                SELECT id
                                    FROM chunk
                                    WHERE chunk.uuid = new.chunk_uuid
                            ),
                            new.time_completed,
                            new.bps
                        );

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM chunk_traffic_stat WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGER_POSTGRESQL = _dd("""\
            CREATE TRIGGER chunk_traffic_stat_view_trigger
                INSTEAD OF INSERT OR DELETE ON chunk_traffic_stat_view
                FOR EACH ROW
                EXECUTE PROCEDURE chunk_traffic_stat_view_trigger_proc()
            """)

        CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGERS = {
            'sqlite':
                [__CREATE_CHUNK_TRAFFIC_STAT_VIEW_INSERT_TRIGGER_SQLITE,
                 __CREATE_CHUNK_TRAFFIC_STAT_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql':
                [__CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGER_PROC_POSTGRESQL,
                 __CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGER_POSTGRESQL]
        }


        # The queries are explicitly joined with host_view,
        # so that every host is present in the list
        # even if there is no real statistics for it yet.
        CREATE_AGGR_CHUNK_TRAFFIC_UP_STAT_VIEW = _dd("""\
            CREATE VIEW aggr_chunk_traffic_up_stat_view AS
                SELECT
                    host.id AS host_id,
                    count(chunk_traffic_stat_view.src_host_id) AS chunks,
                    coalesce(sum(chunk_size), 0) AS total_size,
                    max(time_completed) AS last_time,
                    min(bps) AS min_bps,
                    avg(bps) AS avg_bps,
                    max(bps) AS max_bps
                FROM
                    host
                    LEFT JOIN chunk_traffic_stat_view
                        ON host.id = chunk_traffic_stat_view.src_host_id
                GROUP BY host.id
            """)

        CREATE_AGGR_CHUNK_TRAFFIC_DOWN_STAT_VIEW = _dd("""\
            CREATE VIEW aggr_chunk_traffic_down_stat_view AS
                SELECT
                    host.id AS host_id,
                    count(chunk_traffic_stat_view.dst_host_id) AS chunks,
                    coalesce(sum(chunk_size), 0) AS total_size,
                    max(time_completed) AS last_time,
                    min(bps) AS min_bps,
                    avg(bps) AS avg_bps,
                    max(bps) AS max_bps
                FROM
                    host
                    LEFT JOIN chunk_traffic_stat_view
                        ON host.id = chunk_traffic_stat_view.dst_host_id
                GROUP BY host.id
            """)

        CREATE_AGGR_CHUNK_TRAFFIC_STAT_VIEW = _dd("""\
            CREATE VIEW aggr_chunk_traffic_stat_view AS
                SELECT
                    host.id AS host_id,
                    inhabitant.uuid AS host_uuid,

                    up_stat.chunks AS up_chunks,
                    down_stat.chunks AS down_chunks,

                    up_stat.total_size AS up_total_size,
                    down_stat.total_size AS down_total_size,

                    up_stat.last_time AS up_last_time,
                    down_stat.last_time AS down_last_time,

                    up_stat.min_bps AS up_min_bps,
                    up_stat.avg_bps AS up_avg_bps,
                    up_stat.max_bps AS up_max_bps,

                    down_stat.min_bps AS down_min_bps,
                    down_stat.avg_bps AS down_avg_bps,
                    down_stat.max_bps AS down_max_bps
                FROM
                    inhabitant
                    INNER JOIN host
                        USING(id)
                    INNER JOIN aggr_chunk_traffic_up_stat_view AS up_stat
                        ON host.id = up_stat.host_id
                    INNER JOIN aggr_chunk_traffic_down_stat_view AS down_stat
                        ON host.id = down_stat.host_id
            """)

        # The queries are explicitly joined with host_view,
        # so that every host is present in the list
        # even if there is no real statistics for it yet.
        CREATE___AGGR_CHUNK_DIRECTIONAL_TRAFFIC_STAT_VIEW = _dd("""\
            CREATE VIEW __aggr_chunk_directional_traffic_stat_view
            AS
                SELECT
                    src_host.id AS src_host_id,
                    dst_host.id AS dst_host_id,

                    count(*) AS chunks,
                    coalesce(sum(chunk_size), 0) AS total_size,
                    max(time_completed) AS last_time,
                    min(bps) AS min_bps,
                    avg(bps) AS avg_bps,
                    max(bps) AS max_bps
                FROM
                    host AS src_host
                    CROSS JOIN host AS dst_host
                    LEFT JOIN chunk_traffic_stat_view AS stat
                        ON src_host.id = stat.src_host_id AND
                           dst_host.id = stat.dst_host_id
                GROUP BY src_host.id, dst_host.id
            """)

        CREATE_AGGR_CHUNK_DIRECTIONAL_TRAFFIC_STAT_VIEW = _dd("""\
            CREATE VIEW aggr_chunk_directional_traffic_stat_view
            AS
                SELECT
                    src_inh.id AS src_host_id,
                    dst_inh.id AS dst_host_id,

                    src_inh.uuid AS src_host_uuid,
                    dst_inh.uuid AS dst_host_uuid,

                    stats.chunks AS chunks,
                    stats.total_size AS total_size,
                    stats.last_time AS last_time,
                    stats.min_bps AS min_bps,
                    stats.avg_bps AS avg_bps,
                    stats.max_bps AS max_bps
                FROM
                    __aggr_chunk_directional_traffic_stat_view AS stats
                    INNER JOIN inhabitant AS src_inh
                        ON src_inh.id = stats.src_host_id
                    INNER JOIN inhabitant AS dst_inh
                        ON dst_inh.id = stats.dst_host_id
            """)

        INSERT_CHUNK_TRAFFIC_STAT_VIEW = _dd("""\
            INSERT INTO chunk_traffic_stat_view(
                    src_host_uuid,
                    dst_host_uuid,
                    chunk_uuid,
                    time_completed,
                    bps
                )
                VALUES (
                    :src_host_uuid,
                    :dst_host_uuid,
                    :chunk_uuid,
                    :time_completed,
                    :bps
                )
            """)



    class PresenceStat(object):
        """All DB (node-only) queries related to the presence statistics."""

        __slots__ = tuple()


        # presence_stat_view supports only inserting;
        # * It never writes to "host_at_node" table
        #   but uses the existing value (found by host uuid);
        # * It never writes to "stat_time_quant" table
        #   but uses the existing value (found by time);
        # * It adds the date to "stat_day" table if missing,
        #   and/or uses the existing one (found by date) for foreign key;
        # * It adds the record to "presence_stat" (if it is missing).
        # It never overwrites anything or aborts.
        CREATE_PRESENCE_STAT_VIEW = _dd("""\
            CREATE VIEW presence_stat_view AS
                SELECT
                    presence_stat.id AS id,

                    h_view.id AS host_id,
                    h_view.uuid AS host_uuid,

                    stat_day.id AS stat_day_id,
                    stat_day.date AS date,

                    stat_time_quant.id AS stat_time_quant_id,
                    stat_time_quant.time AS time
                FROM
                    presence_stat
                    INNER JOIN host_view AS h_view
                        ON presence_stat.host_id = h_view.id
                    INNER JOIN stat_day
                        ON presence_stat.stat_day_id = stat_day.id
                    INNER JOIN stat_time_quant
                        ON presence_stat.stat_time_quant_id =
                           stat_time_quant.id
            """)

        __CREATE_PRESENCE_STAT_VIEW_INSERT_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS presence_stat_view_insert_trigger
                INSTEAD OF INSERT ON presence_stat_view
                FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO stat_day(date)
                        VALUES (new.date);
                    INSERT OR IGNORE INTO presence_stat(
                            host_id,
                            stat_day_id,
                            stat_time_quant_id
                        )
                        VALUES (
                            (
                                SELECT id
                                    FROM host_at_node_view
                                    WHERE uuid = new.host_uuid
                            ),
                            (
                                SELECT id
                                    FROM stat_day
                                    WHERE date = new.date
                            ),
                            (
                                SELECT id
                                    FROM stat_time_quant
                                    WHERE time = new.time
                            )
                        );
                END
            """)

        __CREATE_PRESENCE_STAT_VIEW_DELETE_TRIGGER_SQLITE = _dd("""\
            CREATE TRIGGER IF NOT EXISTS presence_stat_view_insert_trigger
                INSTEAD OF DELETE ON presence_stat_view
                FOR EACH ROW
                BEGIN
                    DELETE FROM presence_stat WHERE id = old.id;
                END
            """)

        __CREATE_PRESENCE_STAT_VIEW_PROC_POSTGRESQL = _dd("""\
            CREATE OR REPLACE FUNCTION presence_stat_view_trigger_proc()
            RETURNS TRIGGER AS $$
            DECLARE
                _stat_day_id stat_day.id%TYPE;
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    -- INSERT OR IGNORE INTO stat_day

                    SELECT id
                        FROM stat_day
                        WHERE date = new.date
                        INTO _stat_day_id;

                    IF NOT FOUND THEN
                        INSERT INTO stat_day(date)
                            VALUES (new.date)
                            RETURNING id INTO _stat_day_id;
                    END IF;

                    -- INSERT OR IGNORE INTO presence_stat

                    PERFORM presence_stat.id
                        FROM
                            presence_stat
                            INNER JOIN host_view
                                ON presence_stat.host_id = host_view.id
                            INNER JOIN stat_time_quant
                                ON stat_time_quant.id =
                                   presence_stat.stat_time_quant_id
                        WHERE
                            host_view.uuid = new.host_uuid AND
                            presence_stat.stat_day_id = _stat_day_id AND
                            stat_time_quant.time = new.time;

                    IF NOT FOUND THEN
                        BEGIN
                            INSERT INTO presence_stat(
                                    host_id,
                                    stat_day_id,
                                    stat_time_quant_id
                                )
                                VALUES (
                                    (
                                        SELECT id
                                            FROM host_view
                                            WHERE uuid = new.host_uuid
                                    ),
                                    _stat_day_id,
                                    (
                                        SELECT id
                                            FROM stat_time_quant
                                            WHERE time = new.time
                                    )
                                );
                        EXCEPTION
                            WHEN unique_violation THEN
                                RAISE NOTICE 'Duplicate in presence_stat';

                        END;
                    END IF;

                    RETURN new;
                ELSIF (TG_OP = 'DELETE') THEN
                    DELETE FROM presence_stat WHERE id = old.id;

                    RETURN old;
                END IF;
            END;
            $$ LANGUAGE plpgsql
            """)

        __CREATE_PRESENCE_STAT_VIEW_POSTGRESQL = _dd("""\
            CREATE TRIGGER presence_stat_view_trigger
                INSTEAD OF INSERT OR DELETE ON presence_stat_view
                FOR EACH ROW
                EXECUTE PROCEDURE presence_stat_view_trigger_proc()
            """)

        CREATE_PRESENCE_STAT_VIEW_TRIGGERS = {
            'sqlite':
                [__CREATE_PRESENCE_STAT_VIEW_INSERT_TRIGGER_SQLITE,
                 __CREATE_PRESENCE_STAT_VIEW_DELETE_TRIGGER_SQLITE],
            'postgresql':
                [__CREATE_PRESENCE_STAT_VIEW_PROC_POSTGRESQL,
                 __CREATE_PRESENCE_STAT_VIEW_POSTGRESQL]
        }


        INSERT_PRESENCE_STAT_VIEW = _dd("""\
            INSERT INTO presence_stat_view(host_uuid, date, time)
                VALUES (:host_uuid, :date, :time)
            """)


        @classmethod
        @contract_epydoc
        def insert_presence_stat(cls, host_uuid, dt, rdbw):
            """
            Register the presence of the host (by its UUID)
            at some point of time.

            @type host_uuid: UUID
            @type dt: datetime
            @type rdbw: DatabaseWrapperSQLAlchemy
            """
            rdbw.query(cls.INSERT_PRESENCE_STAT_VIEW,
                       {'host_uuid': host_uuid,
                        'date': dt.date(),
                        'time': TimeEx.from_time(dt.time())})


        @classmethod
        @contract_epydoc
        def get_user_presence_history(cls, username, rdbw):
            """Get presence history for a particular user.

            @type username: basestring
            @type rdbw: DatabaseWrapperSQLAlchemy

            @rtype: col.Mapping
            @returns: a mapping from the host name (for this user)
                to a list of tuples of (quant datetime, count).
            """
            # Number of days for which the statistics must be considered.
            # Must be a string for PostgreSQL function INTERVAL!
            day_count = '28 days'

            m = models

            # TODO: take out this code into subview with more info
            # about specific user.

            presence_stat_var = \
                m.PresenceStat.presence_stat_view().alias('presence_stat')

            was_online_select =\
                sql_select([m.hosts.c.name.label('host_name'),
                            func.to_char(presence_stat_var.c.datetime,
                                         'D-HH24:MI')
                                .label('datetime')],
                           ((m.users.c.name == username) &
                            "NOW() - datetime < interval '{}'"
                                .format(day_count)),
                           from_obj=m.users
                                     .join(m.hosts, isouter=True)
                                     .join(presence_stat_var,
                                           presence_stat_var.c.host ==
                                               m.hosts.c.host_id,
                                           isouter=True))\
                        .group_by('host_name', 'datetime')\
                        .order_by('host_name', 'datetime')\
                        .alias('was_online_select')
            was_online_select2 =\
                sql_select([was_online_select.c.host_name.label('host_name'),
                            was_online_select.c.datetime.label('datetime'),
                            func.count(was_online_select.c.datetime)
                                .label('count')],
                           from_obj=was_online_select)\
                        .group_by('host_name', 'datetime')\
                        .order_by('host_name', 'datetime')

            result_dict = {}
            # Key: host name
            # Value: tuple of (datetime, count).
            for k, v, c in rdbw.execute(was_online_select2):
                result_dict.setdefault(str(k), []).append((v, c))

            # Sorting dict
            for line in result_dict:
                result_dict[line].sort()

            return result_dict



    ALL_CREATE_NODE_QUERIES = (
        HostAtNode.CREATE_HOST_AT_NODE_VIEW,
        HostAtNode.CREATE_HOST_AT_NODE_VIEW_TRIGGERS,
        TrustedDatasets.CREATE_DATASET_ON_HOST_VIEW,
        TrustedFiles.CREATE_FILE_LOCAL_SLASHED_INDEX,
        ChunksReplicaStat.CREATE_CHUNK_CANDIDATES_VIEW,
        ChunksReplicaStat.CREATE_CHUNK_REPLICA_STAT_VIEW,
        SpaceStat.CREATE_MAX_STORAGE_SPACE_VIEW,
        SpaceStat.CREATE___USED_SPACE_STAT_VIEW,
        SpaceStat.CREATE_SPACE_STAT_VIEW,
        TrafficStat.CREATE_CHUNK_TRAFFIC_STAT_VIEW,
        TrafficStat.CREATE_CHUNK_TRAFFIC_STAT_VIEW_TRIGGERS,
        TrafficStat.CREATE_AGGR_CHUNK_TRAFFIC_UP_STAT_VIEW,
        TrafficStat.CREATE_AGGR_CHUNK_TRAFFIC_DOWN_STAT_VIEW,
        TrafficStat.CREATE_AGGR_CHUNK_TRAFFIC_STAT_VIEW,
        TrafficStat.CREATE___AGGR_CHUNK_DIRECTIONAL_TRAFFIC_STAT_VIEW,
        TrafficStat.CREATE_AGGR_CHUNK_DIRECTIONAL_TRAFFIC_STAT_VIEW,
        PresenceStat.CREATE_PRESENCE_STAT_VIEW,
        PresenceStat.CREATE_PRESENCE_STAT_VIEW_TRIGGERS,
    )



#
# Functions
#

@exceptions_logged(logger)
@contract_epydoc
def create_node_db_schema():
    """
    Initialize the node database structure.

    @precondition: RDB is not None
    """
    # Initialize the time quant table

    time_quants_in_day = timedelta(days=1) // STAT_TIME_QUANT

    with RDB() as rdbw:
        create_db_schema(rdbw=rdbw,
                         extra_queries=TrustedQueries.ALL_CREATE_NODE_QUERIES)

        midnight = TimeEx(0)
        # Calculate what time quants are available
        all_time_quants = (midnight + STAT_TIME_QUANT * i
                               for i in xrange(time_quants_in_day))

        # For some reason, rdbw.querymany doesn't work here!
        # for q in all_time_quants:
        rdbw.session.add_all(models.StatTimeQuant(time=q)
                                 for q in all_time_quants)


@contract_epydoc
def init(*args, **kwargs):
    """Initialize RelDB factory singleton.

    Arguments are the same as ones for the DatabaseWrapperFactory constructor.

    @precondition: RDB is None
    """
    global RDB
    RDB = DatabaseWrapperFactory(*args, **kwargs)


@contract_epydoc
def uninit():
    """Deinitialize the RelDB factory singleton.

    @precondition: RDB is not None
    """
    global RDB
    RDB = None

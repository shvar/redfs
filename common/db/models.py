#!/usr/bin/python
"""
SQLAlchemy models common for all components of the system.
"""

#
# Imports
#

from __future__ import absolute_import
import collections as col
import logging
import uuid
from datetime import datetime
from functools import partial
from itertools import chain
from types import NoneType

from sqlalchemy import (
    func, types, event, schema, select, literal_column,
    CheckConstraint, Column, ForeignKey as _ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.sql.expression import cast, column

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.expression import (
    bindparam, text, false as sqla_false, true as sqla_true, not_, union)

from common import crypto
from common.itertools_ex import inonempty
from common.limits import MAX_CHUNK_SIZE_CODE
from common.utils import memoized

from . import model_tweaks, hex_convert, types as custom_types
from common.db.types import crc32_to_signed, crc32_to_unsigned


#
# Constants
#

logger = logging.getLogger(__name__)

MAX_SUPPORTED_DECIMAL_CLOUD_SIZE = 16
"""The max number of decimal places in cloud size for some queries.

Used for some DB queries only, not that much feature-affecting.
16 means that maximum size (9 999 999 999 999 999) is about 10 PB.
"""



#
# Accessory classes
#

class Fingerprint(types.TypeDecorator):
    """Fingerprint mapper to FINGERPRINT sqlalchemy type.

    @todo: move to common.db.types!!!

    See also: http://docs.sqlalchemy.org/en/latest/core/types.html
              #backend-agnostic-guid-type

    @todo: is explicit COLLATE BINARY needed, or it works without it?
           Sqlite3 defaults to binary, so maybe it is redundant.
    """

    impl = types.BINARY


    def __init__(self, *args, **kwargs):
        super(Fingerprint, self).__init__(*args, **kwargs)
        self.length = 72  # length of a FINGERPRINT stored as a blob


    def adapt(self, impltype):
        return impltype(length=self.length)


    def copy(self):
        """
        @todo: do we still need it?
        """
        return Fingerprint(self.impl.length)



class ChunkHash(types.TypeDecorator):
    """Hash mapper to CHUNKHASH sqlite3 type.

    @todo: move to common.db.types!!!

    See also: http://docs.sqlalchemy.org/en/latest/core/types.html
              #backend-agnostic-guid-type
    """

    impl = types.BINARY


    def process_bind_param(self, value, dialect):
        r"""Implement TypeDecorator interface.

        >>> from sqlalchemy.dialects import sqlite as sqlite_dialect
        >>> res_b_sq = ChunkHash().process_bind_param(buffer('abcd'),
        ...                                           sqlite_dialect.dialect())
        >>> res_b_sq, str(res_b_sq)  # doctest:+ELLIPSIS
        (<read-only buffer for ..., size -1, offset 0 at ...>, 'abcd')
        >>> res_s_sq = ChunkHash().process_bind_param('abcd',
        ...                                           sqlite_dialect.dialect())
        >>> res_s_sq, str(res_s_sq)  # doctest:+ELLIPSIS
        (<read-only buffer for ..., size -1, offset 0 at ...>, 'abcd')

        >>> from sqlalchemy.dialects import postgresql as pg_dialect
        >>> res_b_pg = ChunkHash().process_bind_param(buffer('abcd'),
        ...                                           pg_dialect.dialect())
        >>> res_b_pg, str(res_b_pg)  # doctest:+ELLIPSIS
        (<read-only buffer for ..., size -1, offset 0 at ...>, 'abcd')
        >>> res_s_pg = ChunkHash().process_bind_param('abcd',
        ...                                           pg_dialect.dialect())
        >>> res_s_pg, str(res_s_pg)  # doctest:+ELLIPSIS
        (<read-only buffer for ..., size -1, offset 0 at ...>, 'abcd')
        """
        return buffer(value)


    def process_result_value(self, value, dialect):
        r"""Implement TypeDecorator interface."""
        return buffer(value)


@compiles(custom_types.UUID, 'sqlite')
def compile_uuid_sqlite(type_, compiler, **kw):
    return 'UUIDTEXT'


@compiles(custom_types.UUID, 'postgresql')
def compile_uuid_postgresql(type_, compiler, **kw):
    return 'UUID'


@compiles(Fingerprint, 'sqlite')
def compile_fingerprint_sqlite(type_, compiler, **kw):
    return 'FINGERPRINT'


@compiles(Fingerprint, 'postgresql')
def compile_fingerprint_postgresql(type_, compiler, **kw):
    return 'bytea'


@compiles(ChunkHash, 'sqlite')
def compile_chunkhash_sqlite(type_, compiler, **kw):
    return 'CHUNKHASH'


@compiles(ChunkHash, 'postgresql')
def compile_chunkhash_postgresql(type_, compiler, **kw):
    return 'bytea'


@compiles(custom_types.CRC32, 'sqlite')
def compile_crc32_sqlite(type_, compiler, **kw):
    return 'CRC32'


@compiles(custom_types.CRC32, 'postgresql')
def compile_crc32_postgresql(type_, compiler, **kw):
    return 'int4'



Base = declarative_base()

ForeignKey = partial(_ForeignKey,
                     ondelete='CASCADE', onupdate='CASCADE',
                     deferrable=False, initially='IMMEDIATE')



#
# Model classes
#

class CheckUUIDConstraint(CheckConstraint):
    """
    A special variant of C{sqlalchemy.CheckConstraint} that can be used
    to add the constraint to check the UUID text string length,
    and which is compiled differently for sqlite3 and PostgreSQL.
    """
    pass


@compiles(CheckUUIDConstraint, 'sqlite')
def compile_CheckUUIDConstraint_sqlite(element, compiler, **kw):
    """
    For Sqlite3, the C{CheckUUIDConstraint} is compiled as an usual
    C{sqlalchemy.CheckConstraint}.
    """
    return compiler.visit_check_constraint(element, **kw)


@compiles(CheckUUIDConstraint, 'postgresql')
def compile_CheckUUIDConstraint_postgresql(element, compiler, **kw):
    """
    For PostgreSQL, the C{CheckUUIDConstraint} is compiled into
    an empty clause.
    """
    return ''

UUID_LENGTH_CHECK_CONSTRAINT = CheckUUIDConstraint('length(uuid) = 32')



class CheckCRC32Constraint(CheckConstraint):
    """
    A special variant of C{sqlalchemy.CheckConstraint} that can be used
    to add the constraint to check the CRC32 bounds,
    and which is compiled differently for sqlite3 and PostgreSQL.
    """
    pass


@compiles(CheckCRC32Constraint, 'sqlite')
def compile_CheckCRC32Constraint_sqlite(element, compiler, **kw):
    """
    For Sqlite3, the C{CheckCRC32Constraint} is compiled as an usual
    C{sqlalchemy.CheckConstraint}.
    """
    return compiler.visit_check_constraint(element, **kw)


@compiles(CheckCRC32Constraint, 'postgresql')
def compile_CheckCRC32Constraint_postgresql(element, compiler, **kw):
    """
    For PostgreSQL, the C{CheckCRC32Constraint} is compiled into
    an empty clause, so that it won't do extra checks, as they are already
    enforced on the type level.
    """
    return ''

CRC32_CHECK_CONSTRAINT = \
    CheckCRC32Constraint('-2147483648 <= crc32 AND crc32 <= 2147483647')



class UserGroup(Base):
    """
    User group.
    """

    __tablename__ = 'group'
    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    uuid = Column(custom_types.UUID,
                  UUID_LENGTH_CHECK_CONSTRAINT,
                  nullable=False,
                  unique=True)

    name = Column(types.Text,
                  nullable=False)

    private = Column(types.Boolean,
                     nullable=False,
                     default=True,
                     server_default='1')

    enc_key = Column(types.Text,
                     CheckConstraint('length(enc_key) = 44'),
                     nullable=False)


    @classmethod
    @memoized
    def group_used_space_view(cls):
        r"""
        Get an expression that combines each group with the total size occupied
        by their chunks (plus all replicas) in the cloud.

        >>> str(UserGroup.group_used_space_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            group_files_size.group_name AS group_name,
            coalesce(sum(group_files_size.chunk_size), 0)
                AS file_uniq_size,
            CAST(coalesce(sum(group_files_size.chunk_size), 0)
                 * coalesce(avg(chunk_replication_level.replic_lvl), 0.0)
                AS NUMERIC(16, 0)) AS file_hold_size
        \nFROM
            (SELECT DISTINCT
                "group".name AS group_name,
                chunk.id AS chunk_id,
                chunk.size AS chunk_size
            \nFROM
                "group"
                LEFT OUTER JOIN dataset ON "group".id = dataset.group_id
                LEFT OUTER JOIN base_directory
                    ON dataset.id = base_directory.dataset_id
                LEFT OUTER JOIN file_local
                    ON base_directory.id = file_local.base_directory_id
                LEFT OUTER JOIN file ON file.id = file_local.file_id
                LEFT OUTER JOIN block ON file.id = block.file_id
                LEFT OUTER JOIN chunk ON chunk.id = block.chunk_id)
            AS group_files_size
            LEFT OUTER JOIN
                (SELECT DISTINCT
                    chunk.id AS chunk_id,
                    count(chunk_per_host.chunk_id) AS replic_lvl
                \nFROM
                    chunk
                    LEFT OUTER JOIN chunk_per_host
                        ON chunk.id = chunk_per_host.chunk_id
                GROUP BY chunk.id)
            AS chunk_replication_level
                ON chunk_replication_level.chunk_id = group_files_size.chunk_id
        GROUP BY group_files_size.group_name'

        @note: all groups are returned, even ones which have no data stored
            in the cloud.

        @rtype: sqlalchemy.sql.expression.Executable
        """
        group_files_size = select([user_groups.c.name.label('group_name'),
                                   chunks.c.id.label('chunk_id'),
                                   chunks.c.size.label('chunk_size')],
                                  from_obj=user_groups.join(datasets,
                                                            isouter=True)
                                                      .join(base_directories,
                                                            isouter=True)
                                                      .join(file_locals,
                                                            isouter=True)
                                                      .join(files,
                                                            isouter=True)
                                                      .join(blocks,
                                                            isouter=True)
                                                      .join(chunks,
                                                            isouter=True),
                                  distinct=True)\
                               .alias('group_files_size')
        chunk_replication_level = select([chunks.c.id.label('chunk_id'),
                                          func.count(chunks_per_host.c.chunk)
                                              .label('replic_lvl')],
                                         from_obj=chunks.join(chunks_per_host,
                                                              isouter=True),
                                         distinct=True)\
                                      .group_by(chunks.c.id)\
                                      .alias('chunk_replication_level')

        # Very long strings... D'oh!
        l1 = func.coalesce(func.sum(group_files_size.c.chunk_size),
                           literal_column('0')).label('file_uniq_size')
        l2 = func.coalesce(func.avg(chunk_replication_level.c.replic_lvl),
                           literal_column('0.0')).label('replic_lvl')
        return select([group_files_size.c.group_name.label('group_name'),
                       l1,
                       cast(l1 * l2,
                            types.Numeric(
                                precision=MAX_SUPPORTED_DECIMAL_CLOUD_SIZE,
                                scale=0)).label('file_hold_size')],
                      from_obj=group_files_size
                                   .join(chunk_replication_level,
                                         chunk_replication_level.c.chunk_id ==
                                             group_files_size.c.chunk_id,
                                         isouter=True))\
                   .group_by(group_files_size.c.group_name)


    @classmethod
    @memoized
    def select_host_uuids_by_user_group_uuid_expr(cls):
        r"""
        Get the query that receives the UUID of the User Group and
        returns all the hosts of all the users who belong to the group.

        The query accepts a single bound parameter C{user_group_uuid}.

        >>> str(UserGroup.select_host_uuids_by_user_group_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT DISTINCT inhabitant.uuid
        \nFROM
            inhabitant
            JOIN host ON inhabitant.id = host.id
            JOIN "user" ON "user".id = host.user_id
            JOIN membership ON "user".id = membership.user_id
            JOIN "group" ON "group".id = membership.group_id
        \nWHERE "group".uuid = :user_group_uuid'

        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([inhabitants.c.uuid],
                      from_obj=inhabitants.join(hosts)
                                          .join(users)
                                          .join(memberships)
                                          .join(user_groups),
                      whereclause=(user_groups.c.uuid
                                   == bindparam('user_group_uuid')),
                      distinct=True)


user_groups = UserGroup.__table__



# TODO:
# The digest may be NULL, what means that this is the host DB,
# and the password is not stored for this host.
# On the node though, it should NOT be NULL ever.
class User(UserGroup):
    """
    User.
    """

    __tablename__ = 'user'
    __table_args__ = ({'sqlite_autoincrement': True},)
    # __mapper_args__ = {'polymorphic_identity': 'user'}


    user_id = Column('id',
                     ForeignKey(UserGroup.id),
                     key='user_id',
                     primary_key=True)

    owngroup = relationship('UserGroup',
                            backref=backref('ownuser'))

    name = Column(types.Text,
                  CheckConstraint('length(name) > 0'),
                  nullable=False,
                  unique=True)

    digest = Column(types.CHAR(length=40),
                    CheckConstraint('length(digest) = 40'),
                    nullable=False)


    @classmethod
    @memoized
    def user_used_space_view(cls):
        r"""
        Get an expression that combines each user with the total size occupied
        by their chunks (plus all replicas) in the cloud.

        >>> str(User.user_used_space_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            "group".name AS group_name,
            coalesce(sum(chunks_size.chunk_hold_size), 0)
                AS file_hold_size,
            coalesce(sum(chunks_size.chunk_uniq_size), 0)
                AS file_uniq_size
        \nFROM
            "group"
            LEFT OUTER JOIN dataset ON dataset.group_id = "group".id
            LEFT OUTER JOIN base_directory
                ON base_directory.dataset_id = dataset.id
            LEFT OUTER JOIN file_local
                ON file_local.base_directory_id = base_directory.id
            LEFT OUTER JOIN file ON file.id = file_local.file_id
            LEFT OUTER JOIN
                (SELECT
                    block.file_id AS file_id,
                    chunk.size * uniq_chunks.chunks_count AS chunk_hold_size,
                    chunk.size AS chunk_uniq_size
                \nFROM
                    (SELECT
                        chunk_per_host.chunk_id AS chunk_id,
                        count(chunk_per_host.chunk_id) AS chunks_count
                    \nFROM chunk_per_host
                    GROUP BY chunk_per_host.chunk_id)
                    AS uniq_chunks
                    JOIN chunk ON uniq_chunks.chunk_id = chunk.id
                    JOIN block ON block.chunk_id = chunk.id)
                AS chunks_size
                    ON file.id = chunks_size.file_id
        GROUP BY "group".name'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        uniq_chunks = select([chunks_per_host.c.chunk,
                              func.count(chunks_per_host.c.chunk)
                                  .label('chunks_count')],
                             from_obj=chunks_per_host)\
                          .group_by(chunks_per_host.c.chunk)\
                          .alias('uniq_chunks')
        chunks_size = select([blocks.c.file,
                              (chunks.c.size * uniq_chunks.c.chunks_count)
                                  .label('chunk_hold_size'),
                              chunks.c.size.label('chunk_uniq_size')],
                             from_obj=uniq_chunks
                                          .join(chunks,
                                                uniq_chunks.c.chunk ==
                                                    chunks.c.id)
                                          .join(blocks,
                                                blocks.c.chunk ==
                                                    chunks.c.id))\
                          .alias('chunks_size')
        return select([user_groups.c.name.label('group_name'),
                       func.coalesce(func.sum(chunks_size.c.chunk_hold_size),
                                     literal_column('0'))
                           .label('file_hold_size'),
                       func.coalesce(func.sum(chunks_size.c.chunk_uniq_size),
                                     literal_column('0'))
                           .label('file_uniq_size')],
                      from_obj=user_groups
                                   .join(datasets,
                                         datasets.c.group == user_groups.c.id,
                                         isouter=True)
                                   .join(base_directories,
                                         base_directories.c.dataset ==
                                             datasets.c.id,
                                         isouter=True)
                                   .join(file_locals,
                                         file_locals.c.base_directory ==
                                             base_directories.c.id,
                                         isouter=True)
                                   .join(files,
                                         files.c.id == file_locals.c.file,
                                         isouter=True)
                                   .join(chunks_size,
                                         files.c.id == chunks_size.c.file,
                                         isouter=True))\
                   .group_by(user_groups.c.name)


    @classmethod
    @memoized
    def personal_groups_hosts_max_storage_space_view(cls):
        r"""
        Get an expression that combines each "personal group" of a user
        (i.e. one named like the user themselves),
        all hosts of the user (without their UUIDs, sorry), with their
        per-host information about chunk storage usage and the storage limit.

        >>> str(User.personal_groups_hosts_max_storage_space_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            "group".uuid AS group_uuid,
            "group".name AS group_name,
            host.id,
            coalesce(max_storage_space_view.max_size, 0) AS max_size
        \nFROM
            "group"
            JOIN "user" ON "group".id = "user".id
            LEFT OUTER JOIN host ON "user".id = host.user_id
            LEFT OUTER JOIN
                (SELECT
                    setting_view.host_id AS host_id,
                    to_number(concat(\'0\', setting_view.value),
                              \'S999999999999999D99\')
                        * 1048576 AS max_size
                \nFROM
                    (SELECT
                        setting.id AS id,
                        setting_name.id AS setting_name_id,
                        inhabitant.id AS host_id,
                        inhabitant.uuid AS host_uuid,
                        setting_name.name AS name,
                        setting.update_time AS update_time,
                        setting.value AS value
                    \nFROM
                        setting_name
                        JOIN setting
                            ON setting.setting_name_id = setting_name.id
                        JOIN inhabitant ON setting.host_id = inhabitant.id
                        JOIN host ON host.id = inhabitant.id)
                    AS setting_view
                \nWHERE
                    setting_view.name = \'max storage size\')
                AS max_storage_space_view
                ON max_storage_space_view.host_id = host.id'
        """
        max_storage_space_var = \
            Setting.max_storage_space_view().alias('max_storage_space_view')
        return select([user_groups.c.uuid.label('group_uuid'),
                       user_groups.c.name.label('group_name'),
                       hosts.c.host_id,
                       func.coalesce(max_storage_space_var.c.max_size,
                                     literal_column('0')).label('max_size')],
                      from_obj=user_groups.join(users)
                                          .join(hosts, isouter=True)
                                          .join(max_storage_space_var,
                                                max_storage_space_var.c.host_id
                                                    == hosts.c.host_id,
                                                isouter=True))


users = User.__table__

event.listen(User.__table__,
             'after_create',
             schema.DDL('CREATE UNIQUE INDEX ON "user"(upper(name))',
                        on='postgresql'))



class Membership(Base):
    """
    User membership in a group.
    """

    __tablename__ = 'membership'
    __table_args__ = (UniqueConstraint('group', 'user'),
                      Index('membership_user_id_index',
                            'user'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    group = Column('group_id',
                   ForeignKey(UserGroup.id),
                   key='group',
                   nullable=False)

    user = Column('user_id',
                  ForeignKey(User.user_id),
                  key='user',
                  nullable=False)


memberships = Membership.__table__



class Inhabitant(Base):
    """
    Any networking component in the swarm.
    """

    __tablename__ = 'inhabitant'
    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    uuid = Column(custom_types.UUID,
                  UUID_LENGTH_CHECK_CONSTRAINT,
                  nullable=False,
                  unique=True)

    urls = Column(types.Text,
                  nullable=False,
                  default='[]',
                  server_default=text("'[]'"))


inhabitants = Inhabitant.__table__



class Host(Inhabitant):
    """
    Host client.
    """

    __tablename__ = 'host'

    __table_args__ = (UniqueConstraint('user', 'name'),
                      {'sqlite_autoincrement': True})

    host_id = Column('id',
                     ForeignKey(Inhabitant.id),
                     key='host_id',
                     primary_key=True)

    user = Column('user_id',
                  ForeignKey(User.user_id),
                  key='user',
                  nullable=model_tweaks.LateBooleanBinding(
                               lambda: model_tweaks._HOST_USER_ID_IS_NULLABLE))

    name = Column(types.Text,
                  nullable=False)


    @classmethod
    @memoized
    def host_view(cls):
        r"""Get an expression that combines each host with its UUID.

        >>> str(Host.host_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            inhabitant.id,
            inhabitant.uuid,
            inhabitant.urls,
            host.name,
            host.user_id
        \nFROM host
            JOIN inhabitant ON inhabitant.id = host.id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        return select([inhabitants.c.id,
                       inhabitants.c.uuid,
                       inhabitants.c.urls,
                       hosts.c.name,
                       hosts.c.user],
                      from_obj=hosts.join(inhabitants))


    @classmethod
    @memoized
    def select_host_id_by_host_uuid_expr(cls):
        r"""
        Get the query that receives the ID of the host (note, not just
        the inhabitant, but surely the host!) by its UUID.

        The query accepts a single bound parameter C{host_uuid}.

        >>> str(Host.select_host_id_by_host_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT host.id
        \nFROM
            host
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid'

        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([hosts.c.host_id],
                      from_obj=hosts.join(inhabitants),
                      whereclause=inhabitants.c.uuid == bindparam('host_uuid'))


hosts = Host.__table__



class Node(Inhabitant):
    """
    System node.
    """

    __tablename__ = 'node'
    __table_args__ = ({'sqlite_autoincrement': True},)

    node_id = Column('id',
                     ForeignKey(Inhabitant.id),
                     key='node_id',
                     primary_key=True)


nodes = Node.__table__



class SettingName(Base):
    """
    The name for the setting which is roamed between the node and the host.
    """

    __tablename__ = 'setting_name'

    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    name = Column(types.Text,
                  nullable=False,
                  unique=True)


setting_names = SettingName.__table__



class Setting(Base):
    """
    Some setting which is roamed between the node and the host.

    @todo: maybe, C{setting_fk_host} is redundant and not needed.
    """

    __tablename__ = 'setting'

    __table_args__ = (UniqueConstraint('setting_name', 'host'),
                      Index('setting_fk_host',
                            'host'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    # FK index is a subset of setting_unique_for_host constraint
    setting_name = Column('setting_name_id',
                          ForeignKey(SettingName.id),
                          key='setting_name',
                          nullable=False)

    # FK index is a subset of setting_unique_for_host constraint
    host = Column('host_id',
                  ForeignKey(Host.host_id),
                  key='host',
                  nullable=False)

    update_time = Column(types.DateTime,
                         nullable=False,
                         default=datetime.utcnow,
                         server_default=func.current_timestamp())

    value = Column(types.Text,
                   nullable=False)


    @classmethod
    @memoized
    def setting_view(cls):
        r"""
        Get an expression that combines each setting (plus its name)
        for each host (plus its UUID) with the value of that setting.

        >>> str(Setting.setting_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            setting.id,
            setting_name.id AS setting_name_id,
            inhabitant.id AS host_id,
            inhabitant.uuid AS host_uuid,
            setting_name.name,
            setting.update_time,
            setting.value
        \nFROM
            setting_name
            JOIN setting ON setting.setting_name_id = setting_name.id
            JOIN inhabitant ON setting.host_id = inhabitant.id
            JOIN host ON host.id = inhabitant.id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        return select([settings.c.id,
                       setting_names.c.id.label('setting_name_id'),
                       inhabitants.c.id.label('host_id'),
                       inhabitants.c.uuid.label('host_uuid'),
                       setting_names.c.name,
                       settings.c.update_time,
                       settings.c.value],
                      from_obj=setting_names.join(settings,
                                                  settings.c.setting_name ==
                                                      setting_names.c.id)
                                            .join(inhabitants,
                                                  settings.c.host ==
                                                      inhabitants.c.id)
                                            .join(hosts,
                                                  hosts.c.host_id ==
                                                      inhabitants.c.id))


    @classmethod
    @memoized
    def max_storage_space_view(cls):
        r"""
        Get the expression that combines each host (plus its UUID) with
        the storage size that the host permitted to use for chunks storage.

        >>> str(Setting.max_storage_space_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        "SELECT
            setting_view.host_id,
            to_number(concat('0', setting_view.value),
                      'S999999999999999D99') * 1048576 AS max_size
        \nFROM
            (SELECT
                setting.id AS id,
                setting_name.id AS setting_name_id,
                inhabitant.id AS host_id,
                inhabitant.uuid AS host_uuid,
                setting_name.name AS name,
                setting.update_time AS update_time,
                setting.value AS value
            \nFROM setting_name
            JOIN
                setting ON setting.setting_name_id = setting_name.id
                JOIN inhabitant ON setting.host_id = inhabitant.id
                JOIN host ON host.id = inhabitant.id)
            AS setting_view
        \nWHERE setting_view.name = 'max storage size'"

        @rtype: sqlalchemy.sql.expression.Executable
        """
        setting_var = cls.setting_view().alias('setting_view')

        return select([setting_var.c.host_id,
                       (func.to_number(func.concat(literal_column("'0'"),
                                                   setting_var.c.value),
                                       literal_column("'S999999999999999D99'"))
                           * literal_column('1048576')).label('max_size')],
                      setting_var.c.name ==
                          literal_column("'max storage size'"),
                      setting_var)


    @classmethod
    @memoized
    def select_settings_by_host_uuid_expr(cls):
        r"""
        Get the query that receives all the setting values for some host
        by its UUID.

        The query accepts a single bound parameter C{host_uuid}.

        >>> str(Setting.select_settings_by_host_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT setting_name.name, setting.value, setting.update_time
        \nFROM
            setting
            JOIN setting_name ON setting_name.id = setting.setting_name_id
            JOIN host ON host.id = setting.host_id
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid'

        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([setting_names.c.name,
                       settings.c.value,
                       settings.c.update_time],
                      from_obj=settings.join(setting_names)
                                       .join(hosts)
                                       .join(inhabitants),
                      whereclause=inhabitants.c.uuid == bindparam('host_uuid'))


    @classmethod
    @memoized
    def select_settings_by_host_uuid_expr(cls):
        r"""
        Get the query that receives all the setting values for some host
        by its UUID.

        The query accepts a single bound parameter C{host_uuid}.

        >>> str(Setting.select_settings_by_host_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT setting_name.name, setting.value, setting.update_time
        \nFROM
            setting
            JOIN setting_name ON setting_name.id = setting.setting_name_id
            JOIN host ON host.id = setting.host_id
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid'

        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([setting_names.c.name,
                       settings.c.value,
                       settings.c.update_time],
                      from_obj=settings.join(setting_names)
                                       .join(hosts)
                                       .join(inhabitants),
                      whereclause=inhabitants.c.uuid == bindparam('host_uuid'))


    @classmethod
    @memoized
    def select_setting_by_host_uuid_and_name_expr(cls, with_time):
        r"""
        Get the query that receives the value of the setting for some host
        by its UUID and setting name.

        The query accepts two bound parameter: C{host_uuid}, and C{name}.

        >>> str(Setting.select_setting_by_host_uuid_and_name_expr(True)) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT setting.value, setting.update_time
        \nFROM
            setting
            JOIN setting_name ON setting_name.id = setting.setting_name_id
            JOIN host ON host.id = setting.host_id
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid AND setting_name.name = :name'

        >>> str(Setting.select_setting_by_host_uuid_and_name_expr(False)) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT setting.value
        \nFROM
            setting
            JOIN setting_name ON setting_name.id = setting.setting_name_id
            JOIN host ON host.id = setting.host_id
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid AND setting_name.name = :name'

        @param with_time: whether the result must include the update time
            of the setting.
        @type with_time: bool

        @rtype: sqlalchemy.sql.expression.Select
        """
        columns = [settings.c.value,
                   settings.c.update_time] if with_time \
                                           else [settings.c.value]

        return select(columns,
                      from_obj=settings.join(setting_names)
                                       .join(hosts)
                                       .join(inhabitants),
                      whereclause=(inhabitants.c.uuid
                                   == bindparam('host_uuid')) &
                                  (setting_names.c.name
                                   == bindparam('name')))


settings = Setting.__table__



class LocalSetting(Base):
    """
    Some setting local (not roamed) to the inhabitant.
    """

    __tablename__ = 'local_setting'

    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    update_time = Column(types.DateTime,
                         nullable=False,
                         default=datetime.utcnow,
                         server_default=func.current_timestamp())

    name = Column(types.Text,
                  nullable=False,
                  unique=True)

    value = Column(types.Text,
                   nullable=False)


local_settings = LocalSetting.__table__



class Dataset(Base):
    """
    The data containing the information about a specific backup of some set
    of the files.
    """

    __tablename__ = 'dataset'

    __table_args__ = (Index('dataset_group_id_time_started_index',
                            'group', 'time_started'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    uuid = Column(custom_types.UUID,
                  UUID_LENGTH_CHECK_CONSTRAINT,
                  nullable=False,
                  unique=True)

    sync = Column(types.Boolean,
                  nullable=False,
                  default=True,
                  server_default='1')

    merged = Column(types.Boolean,
                    nullable=False,
                    default=False,
                    server_default='0')

    name = Column(types.Text,
                  nullable=False)

    group = Column('group_id',
                   ForeignKey(UserGroup.id),
                   key='group',
                   nullable=False)

    time_started = Column(types.DateTime,
                          nullable=False)

    time_completed = Column(types.DateTime,
                            nullable=True)


    @classmethod
    def get_id_by_uuid(cls, ds_uuid):
        r"""
        Get an expression that finds out the dataset ID
        by the provided dataset UUID.

        @rtype: sqlalchemy.sql.expression.Select

        >>> str(Dataset.get_id_by_uuid(uuid.uuid4())) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT dataset.id \nFROM dataset \nWHERE dataset.uuid = :uuid_1'
        """
        return select([datasets.c.id],
                      (datasets.c.uuid == ds_uuid),
                      datasets)


    @classmethod
    def get_group_uuid_by_ds_uuid(cls, ds_uuid):
        """
        @return: user_group.uuid which has a dataset with this UUID.
        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([user_groups.c.uuid],
                      (datasets.c.uuid == ds_uuid),
                      datasets.join(user_groups))


    @classmethod
    @memoized
    def insert_dataset_copying_group_from_ds_expr(cls):
        r"""
        Get the query that inserts all the columns for a dataset
        (except C{group}), assuming that the group is passed via C{_ds_uuid}
        (which has this group).

        The query accepts a single bound parameter C{_ds_uuid}.

        >>> dummy_data = {
        ...     'uuid': uuid.uuid4(),
        ...     'sync': False,
        ...     'merged': False,
        ...     'name': '',
        ...     'time_started': datetime.utcnow(),
        ...     'time_completed': None,
        ...     '_ds_uuid': uuid.uuid4()
        ... }
        >>> str(Dataset.insert_dataset_copying_group_from_ds_expr()
        ...            .values(dummy_data)) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'INSERT INTO dataset
            (uuid, sync, merged, name, group_id, time_started, time_completed)
        VALUES
            (:uuid,
            :sync,
            :merged,
            :name,
            (SELECT dataset.group_id
                \nFROM dataset
                \nWHERE dataset.uuid = :_ds_uuid),
            :time_started,
            :time_completed)'
        """
        return datasets.insert(
                   {'group': select([datasets.c.group],
                                    (datasets.c.uuid == bindparam('_ds_uuid')),
                                    datasets)})


    @classmethod
    @memoized
    def update_tc_by_ds_uuid_expr(cls):
        r"""
        Returns an update query which accepts C{_ds_uuid} as a dataset UUID,
        and C{time_completed} as a new value of time_completed, which should be
        updated for that dataset.

        >>> str(Dataset.update_tc_by_ds_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'UPDATE dataset
        SET time_completed=:time_completed
        WHERE dataset.uuid = :_ds_uuid'
        """
        return datasets.update() \
                       .where(datasets.c.uuid == bindparam('_ds_uuid')) \
                       .values(time_completed=bindparam('time_completed'))


    @classmethod
    @memoized
    def get_max_time_started_by_groups_expr(cls):
        r"""Get max time_started for datasets by group.

        >>> str(Dataset.get_max_time_started_by_groups_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT dataset.group_id, max(dataset.time_started) AS max_ts
        \nFROM dataset
        GROUP BY dataset.group_id'
        """
        return select([datasets.c.group,
                       func.max(datasets.c.time_started).label('max_ts')],
                       from_obj=datasets)\
                   .group_by(datasets.c.group)


    @classmethod
    def get_last_started_dataset_for_user_expr(cls, username):
        r"""Get last dataset for user.

        >>> str(Dataset.get_last_started_dataset_for_user_expr(
        ...         'alpha@bravo.com'))  # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            dataset.uuid,
            mts_user.max_ts AS ts
        \nFROM
            (SELECT
                max(max_time_started_by_group.max_ts) AS max_ts,
                "user".name AS username
            \nFROM
                (SELECT
                    dataset.group_id AS group_id,
                    max(dataset.time_started) AS max_ts
                \nFROM dataset
                GROUP BY dataset.group_id) AS max_time_started_by_group
                JOIN membership
                    ON membership.group_id =
                        max_time_started_by_group.group_id
                JOIN "user"
                    ON membership.user_id = "user".id
            \nWHERE "user".name = :name_1
            GROUP BY "user".name) AS mts_user
            JOIN dataset ON dataset.time_started = mts_user.max_ts
            JOIN membership ON membership.group_id = dataset.group_id
            JOIN "user" ON membership.user_id = "user".id
        \nWHERE "user".name = mts_user.username\n
        LIMIT :param_1'
        """
        # Max time started by group.
        mts_group = cls.get_max_time_started_by_groups_expr() \
                       .alias('max_time_started_by_group')

        # Max time started for specific user.
        mts_user = select([func.max(mts_group.c.max_ts).label('max_ts'),
                           users.c.name.label('username')],
                          users.c.name == username,
                          mts_group
                              .join(memberships,
                                    memberships.c.group ==
                                        mts_group.c.group)
                              .join(users,
                                    memberships.c.user ==
                                        users.c.user_id))\
                       .group_by(users.c.name)\
                       .alias('mts_user')

        # UUID of dataset that started last. For user of course.
        # @TODO: refactor it, cause now we do not support several groups for...
        # ...one user
        return select([datasets.c.uuid,
                       mts_user.c.max_ts.label('ts')],
                      users.c.name == mts_user.c.username,
                      mts_user
                          .join(datasets,
                                datasets.c.time_started == mts_user.c.max_ts)
                          .join(memberships,
                                memberships.c.group == datasets.c.group)
                          .join(users,
                                memberships.c.user == users.c.user_id)) \
                   .limit(1)


datasets = Dataset.__table__



class BaseDirectory(Base):
    """
    Base directory in the dataset.
    """

    __tablename__ = 'base_directory'

    __table_args__ = (UniqueConstraint('dataset', 'path'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    dataset = Column('dataset_id',
                     ForeignKey(Dataset.id),
                     key='dataset',
                     nullable=False)

    path = Column(types.Text,
                  nullable=False)


    @classmethod
    def get_from_ds(cls, ds_select):
        ds_subq = ds_select.alias()

        return select([base_directories.c.id,
                       base_directories.c.path],
                      (base_directories.c.dataset == ds_subq.c.id),
                      [base_directories, ds_subq])


    @classmethod
    def clone_basedirs_for_insert(cls, file_locals_sel, ds_id_sel):
        file_locals_subq = file_locals_sel.alias()
        ds_subq = ds_id_sel.alias()

        return select([ds_subq.c.id.label('dataset'),
                       file_locals_subq.c.path.label('path')],
                      from_obj=[file_locals_subq, ds_subq])\
                   .group_by(file_locals_subq.c.path, ds_subq.c.id)


base_directories = BaseDirectory.__table__



class File(Base):
    """The unique content of some file

    Its fingerprint defines it in unique way for a particular user group.
    """

    __tablename__ = 'file'
    __table_args__ = (UniqueConstraint('group', 'fingerprint'),
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    group = Column('group_id',
                   ForeignKey(UserGroup.id),
                   key='group',
                   nullable=False)

    crc32 = Column(custom_types.CRC32,
                   CRC32_CHECK_CONSTRAINT,
                   nullable=False)

    uuid = Column(custom_types.UUID,
                  UUID_LENGTH_CHECK_CONSTRAINT,
                  nullable=model_tweaks.LateBooleanBinding(
                               lambda: model_tweaks._FILE_UUID_IS_NULLABLE),
                  unique=True)

    fingerprint = Column(Fingerprint,
                  CheckConstraint('length(fingerprint) = 72'),
                  nullable=False)


    @classmethod
    @memoized
    def file_view(cls, dialect='postgresql'):
        r"""
        Get an expression that extracts the size information from
        the fingerprint of each file.

        >>> str(File.file_view(dialect='postgresql'))  \
        ... # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            file.id,
            file.group_id,
            file.crc32,
            file.uuid,
            file.fingerprint,
            (72057594037927936 * get_byte(file.fingerprint, 0)
             + 281474976710656 * get_byte(file.fingerprint, 1)
             + 1099511627776 * get_byte(file.fingerprint, 2)
             + 4294967296 * get_byte(file.fingerprint, 3)
             + 16777216 * get_byte(file.fingerprint, 4)
             + 65536 * get_byte(file.fingerprint, 5)
             + 256 * get_byte(file.fingerprint, 6)
             + 1 * get_byte(file.fingerprint, 7)) AS size
        \nFROM file'

        >>> # File.file_view(dialect='sqlite') is tested in
        >>> # C{__test__.File.file_view.sqlite}.

        @rtype: sqlalchemy.sql.expression.Executable
        """
        if dialect == 'postgresql':
            hex_converter = hex_convert.get_postgresql_hex_convert_expr
        elif dialect == 'sqlite':
            hex_converter = hex_convert.get_sqlite3_hex_convert_expr
        else:
            raise NotImplementedError(repr(dialect))

        return select([files.c.id,
                       files.c.group,
                       files.c.crc32,
                       files.c.uuid,
                       files.c.fingerprint,
                       literal_column(hex_converter(files.c.fingerprint))
                           .label('size')],
                      from_obj=files)


files = File.__table__



class FileLocal(Base):
    """
    A file location at some file system.
    """

    __tablename__ = 'file_local'
    __table_args__ = (Index('file_local_fk_base_directory_file',
                            'base_directory', 'isdir', 'rel_path', 'file'),
                      Index('file_local_fk_file',
                            'base_directory', 'file'),
                      UniqueConstraint('base_directory', 'rel_path'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    file = Column('file_id',
                  ForeignKey(File.id),
                  key='file',
                  nullable=True)

    base_directory = Column('base_directory_id',
                            ForeignKey(BaseDirectory.id),
                            key='base_directory',
                            nullable=False)

    isdir = Column(types.Boolean,
                   nullable=False,
                   default=False,
                   server_default='0')

    rel_path = Column(types.Text,
                      nullable=False)

    attrs = Column(types.Text,
                   nullable=False,
                   default='',
                   server_default='')


    @classmethod
    @memoized
    def file_local_view(cls):
        r"""
        Get an expression that combines each file_local with all the data
        up to base directories.

        >>> str(FileLocal.file_local_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            file_local.file_id, file_local.isdir, file_local.rel_path,
            base_directory.path, dataset.uuid, dataset.sync, dataset.merged,
            dataset.group_id, dataset.time_completed, dataset.time_started
        \nFROM file_local
        JOIN base_directory ON base_directory.id = file_local.base_directory_id
        JOIN dataset ON dataset.id = base_directory.dataset_id'

        @todo: fix if need more...

        @rtype: sqlalchemy.sql.expression.Executable
        """
        return select([file_locals.c.file,
                       file_locals.c.isdir,
                       file_locals.c.rel_path,
                       base_directories.c.path,
                       datasets.c.uuid,
                       datasets.c.sync,
                       datasets.c.merged,
                       datasets.c.group,
                       datasets.c.time_completed,
                       datasets.c.time_started],
                      from_obj=file_locals.join(base_directories)
                                          .join(datasets))


    @classmethod
    def get_available_on_dt(cls, base_dt, user_group_uuid):
        r"""
        Given a user group UUID (C{user_group_uuid}) and some baseline
        datetime (C{base_dt}), returns all the files
        (actually, their rel_paths and time_completeds)
        of the latest states of the files available for that user group
        on {base_dt}.

        >>> str(FileLocal.get_available_on_dt(
        ...         base_dt=datetime.utcnow(),
        ...         user_group_uuid=uuid.uuid4())) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            file_local.rel_path,
            max(dataset.time_completed) AS mtc
        \nFROM
            file_local
            JOIN base_directory
                ON base_directory.id = file_local.base_directory_id
            JOIN dataset
                ON dataset.id = base_directory.dataset_id
            JOIN "group" ON "group".id = dataset.group_id
        \nWHERE
            dataset.time_completed <= :time_completed_1 AND
            "group".uuid = :uuid_1
        GROUP BY file_local.rel_path'

        @param base_dt: baseline "time completed" of a dataset.
        """
        return select([file_locals.c.rel_path,
                       func.max(datasets.c.time_completed).label('mtc')],
                      ((datasets.c.time_completed <= base_dt) &
                       (user_groups.c.uuid == user_group_uuid)),
                      file_locals
                          .join(base_directories)
                          .join(datasets)
                          .join(user_groups))\
                   .group_by(file_locals.c.rel_path)


    @classmethod
    def clone_file_locals(cls,
                          file_locals_sel, base_dt, paths, user_group_uuid):
        """
        During cloning some files (on restoring process), returns the query
        that contains the FileLocal data generally ready for insertion,
        bound to the new (not yet created) base directories.

        @param paths: the iterable of paths used for cloning.
        @type paths: col.Iterable

        @returns: the sequence of file_locals

            The expression with 4 columns:
            file,
            base directory path,
            isdir flag and
            rel_path,
            with all the files from file_locals which are older than
            the given C{base_dt}, their rel_paths are in C{path_cond} and
            user_group.uuid is C{user_group_uuid}.
        """
        file_locals_subq = file_locals_sel.alias()

        path_cond = sqla_false()

        # Creating SQL whereclause 'OR' statement:
        #  - if path not ends with '/', we need to find this entry;
        #  - else we need to find all entries, that are like path.
        for path in paths:
            if path[-1] == '/':
                path_cond |= file_locals_subq.c.rel_path.like(
                                 u'{}%'.format(path))
                _path_to_compare = path[:-1]
            else:
                _path_to_compare = path

            path_cond |= file_locals_subq.c.rel_path == _path_to_compare

        return select([file_locals.c.file.label('file'),
                       base_directories.c.path,
                       file_locals.c.isdir,
                       file_locals.c.rel_path],
                      ((file_locals_subq.c.mtc <= base_dt) & path_cond &
                       (user_groups.c.uuid == user_group_uuid)),
                      file_locals
                          .join(base_directories)
                          .join(datasets)
                          .join(user_groups)
                          .join(file_locals_subq,
                                ((file_locals_subq.c.rel_path ==
                                      file_locals.c.rel_path) &
                                 (file_locals_subq.c.mtc ==
                                      datasets.c.time_completed))))


    @classmethod
    def get_file_locals_for_insert(cls, file_locals_sel, basedir_sel):
        """
        @returns: the data ready to insert into file_locals on create a dataset
            by cloning the existing data, provided that the file_local records
            to clone are retrievable via C{file_locals_sel} expression,
            and base directories to bind are already created and available via
            C{bd_sel}.
        """
        file_locals_subq = file_locals_sel.alias()
        basedir_subq = basedir_sel.alias()

        return select([file_locals_subq.c.file.label('file'),
                       basedir_subq.c.id.label('base_directory'),
                       file_locals_subq.c.isdir,
                       file_locals_subq.c.rel_path],
                      from_obj=file_locals_subq
                                   .join(basedir_subq,
                                         basedir_subq.c.path ==
                                             file_locals_subq.c.path))


    @classmethod
    def get_latest_uniq_rel_paths_by_group_expr(
            cls, latest_time_started=None, is_time_completed_specified=True,
            is_merged=None):
        r"""
        >>> str(FileLocal.get_latest_uniq_rel_paths_by_group_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT file_local.rel_path, dataset.group_id,
            max(dataset.time_started) AS ds_mts
        \nFROM file_local
        JOIN base_directory
            ON base_directory.id = file_local.base_directory_id
        JOIN dataset
            ON dataset.id = base_directory.dataset_id
        \nWHERE true AND dataset.time_completed IS NOT NULL
        GROUP BY file_local.rel_path, dataset.group_id'

        >>> str(FileLocal.get_latest_uniq_rel_paths_by_group_expr(
        ...         latest_time_started=datetime(2013, 01, 24, 12, 00))) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT file_local.rel_path, dataset.group_id,
            max(dataset.time_started) AS ds_mts
        \nFROM file_local
        JOIN base_directory
            ON base_directory.id = file_local.base_directory_id
        JOIN dataset
            ON dataset.id = base_directory.dataset_id
        \nWHERE
            true AND
            dataset.time_completed IS NOT NULL AND
            dataset.time_started <= :time_started_1
        GROUP BY file_local.rel_path, dataset.group_id'

        >>> str(FileLocal.get_latest_uniq_rel_paths_by_group_expr(
        ...         latest_time_started=datetime(2013, 01, 24, 12, 00),
        ...         is_time_completed_specified=False)) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT file_local.rel_path, dataset.group_id,
            max(dataset.time_started) AS ds_mts
        \nFROM file_local
        JOIN base_directory
            ON base_directory.id = file_local.base_directory_id
        JOIN dataset
            ON dataset.id = base_directory.dataset_id
        \nWHERE true AND dataset.time_started <= :time_started_1
        GROUP BY file_local.rel_path, dataset.group_id'

        @param latest_time_started: if not C{None}, used as an additional filter
            condition.
        @type latest_time_started: datetime, NoneType
        @type is_time_completed_specified: bool

        @return: unique file_locals with rel_path, dataset group_id and last
            time_started

        @rtype: sqlalchemy.sql.expression.Select
        """
        where = sqla_true()
        if is_time_completed_specified:
            where &= (datasets.c.time_completed != None)
        if latest_time_started is not None:
            where &= (datasets.c.time_started <= latest_time_started)
        if is_merged is not None:
            where &= (datasets.c.merged == is_merged)
        return select([file_locals.c.rel_path,
                       datasets.c.group,
                       func.max(datasets.c.time_started).label('ds_mts')],
                      where,
                      from_obj=file_locals
                                   .join(base_directories)
                                   .join(datasets))\
                   .group_by(file_locals.c.rel_path,
                             datasets.c.group)


    @classmethod
    @memoized
    def time_changes_for_file_by_username_rel_path_expr(cls):
        r"""
        The query accepts three bound parameters: C{username}, C{rel_path}
        and C{is_merged}.

        >>> str(FileLocal.time_changes_for_file_by_username_rel_path_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT anon_1.path,
                "user".name,
                anon_1.uuid,
                anon_1.time_started,
                anon_2.size
        \nFROM
            (SELECT file_local.file_id AS file_id,
                    file_local.isdir AS isdir,
                    file_local.rel_path AS rel_path,
                    base_directory.path AS path,
                    dataset.uuid AS uuid,
                    dataset.sync AS sync,
                    dataset.merged AS merged,
                    dataset.group_id AS group_id,
                    dataset.time_completed AS time_completed,
                    dataset.time_started AS time_started
            \nFROM file_local
            JOIN base_directory
                ON base_directory.id = file_local.base_directory_id
            JOIN dataset
                ON dataset.id = base_directory.dataset_id) AS anon_1
        LEFT OUTER JOIN
            (SELECT file.id AS id,
                    file.group_id AS group_id,
                    file.crc32 AS crc32,
                    file.uuid AS uuid,
                    file.fingerprint AS fingerprint,
                    (72057594037927936 * get_byte(file.fingerprint, 0)
                     + 281474976710656 * get_byte(file.fingerprint, 1)
                     + 1099511627776 * get_byte(file.fingerprint, 2)
                     + 4294967296 * get_byte(file.fingerprint, 3)
                     + 16777216 * get_byte(file.fingerprint, 4)
                     + 65536 * get_byte(file.fingerprint, 5)
                     + 256 * get_byte(file.fingerprint, 6)
                     + 1 * get_byte(file.fingerprint, 7)) AS size
            \nFROM file) AS anon_2
                ON anon_1.file_id = anon_2.id
        JOIN membership
            ON membership.group_id = anon_1.group_id
        JOIN "user"
            ON membership.user_id = "user".id
        \nWHERE anon_1.time_completed IS NOT NULL
                AND anon_1.rel_path = :rel_path
                AND anon_1.isdir = false
                AND "user".name = :username
                AND (:is_merged IS NULL
                     OR anon_1.merged = :is_merged)'

        @param rel_path: rel_path of the file.
        @type rel_path: basestring

        @return: base directory path and size of a file and datasets uuid
            with time_started where this file was changed (added or deleted)
        @rtype: sqlalchemy.sql.expression.Select
        """
        file_local_v = cls.file_local_view().alias()
        file_v = File.file_view().alias()

        where = ((file_local_v.c.time_completed != None) &
                 (file_local_v.c.rel_path == bindparam('rel_path')) &
                 (file_local_v.c.isdir == sqla_false()) &
                 (users.c.name == bindparam('username')) &
                 ((bindparam('is_merged') == None) |
                  (file_local_v.c.merged == bindparam('is_merged'))))
        return select([file_local_v.c.path,
                       users.c.name,
                       file_local_v.c.uuid,
                       file_local_v.c.time_started,
                       file_v.c.size],
                      where,
                      from_obj=file_local_v.join(file_v,
                                                 file_local_v.c.file ==
                                                     file_v.c.id,
                                                 isouter=True)
                                           .join(memberships,
                                                 memberships.c.group ==
                                                 file_local_v.c.group)
                                           .join(users,
                                                 memberships.c.user ==
                                                 users.c.user_id))


file_locals = files_local = FileLocal.__table__



class Chunk(Base):
    """A chunk."""

    __tablename__ = 'chunk'
    __table_args__ = (UniqueConstraint('hash', 'size'),
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    crc32 = Column(custom_types.CRC32,
                   CRC32_CHECK_CONSTRAINT,
                   nullable=False)

    uuid = Column(custom_types.UUID,
                  UUID_LENGTH_CHECK_CONSTRAINT,
                  nullable=False,
                  unique=True)

    # The hash of the chunk may probably be NULL/None, to reflect the situation
    # when the chunk hash failed at the moment of its initial upload,
    # hence rendering it probably invalid; though the chunk was still
    # uploaded because some of the files in it still may be considered valid.

    # Nevertheless, there should never exist a chunk with the same pair
    # of (chunk, size), provided that the chunk is not NULL, and this
    # is enforced on the database level.

    # TODO: updated: in fact, there should never exist a chunk
    # with the same tuple of (user group, chunk, size)
    hash = Column(ChunkHash,
                  CheckConstraint('length(hash) = 64'),
                  nullable=False)  # for now, everything works with NOT NULL

    size = Column(types.Integer,
                  CheckConstraint('size > 0'),
                  nullable=False)

    maxsize_code = Column(types.SmallInteger,
                          CheckConstraint('0 <= maxsize_code AND '
                                          'maxsize_code <= {:d}'
                                              .format(MAX_CHUNK_SIZE_CODE)),
                          nullable=False)


    @classmethod
    @memoized
    def delete_orphaned_chunks(cls):
        r"""
        Get an expression that deletes all the chunks no more referenced
        by any dataset.

        >>> str(Chunk.delete_orphaned_chunks()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'DELETE FROM chunk
        WHERE
            chunk.id NOT IN
                (SELECT chunk.id
                \nFROM
                    chunk
                        JOIN block ON chunk.id = block.chunk_id
                        JOIN file ON file.id = block.file_id
                        JOIN file_local ON file.id = file_local.file_id
                        JOIN base_directory
                            ON base_directory.id = file_local.base_directory_id
                        JOIN dataset
                            ON dataset.id = base_directory.dataset_id)'
        """
        select_all_chunk_ids = \
            select([chunks.c.id],
                   from_obj=chunks.join(blocks)
                                  .join(files)
                                  .join(file_locals)
                                  .join(base_directories)
                                  .join(datasets))
        return chunks.delete(not_(chunks.c.id.in_(select_all_chunk_ids)))


    @classmethod
    @memoized
    def select_chunk_by_hash_size_expr(cls):
        r"""Get an expression that finds the chunk by its hash and size.

        Such an expression accepts two bound parameters, C{hash} and C{size}.

        >>> str(Chunk.select_chunk_by_hash_size_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT chunk.uuid
        \nFROM chunk
        \nWHERE chunk.hash = :hash AND chunk.size = :size'
        """
        return select([chunks.c.uuid],
                      ((chunks.c.hash == bindparam('hash')) &
                       (chunks.c.size == bindparam('size'))))


    @classmethod
    def select_duplicates_expr(cls, chunks_):
        r"""
        Get an expression that finds the already existing chunks
        duplicating the passed ones on hash/size tuple.

        Such an expression accepts many bound parameters, three per each
        passed chunk; they should be named like C{v_N_orig_uuid},
        C{v_N_hash} and C{v_N_size}, where N is the index of the chunk in the
        sequence, starting from 0.

        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID
        >>> chunks = [
        ...     ChunkInfo(crc32=0x07FD7A5B,
        ...               uuid=ChunkUUID('5b237ceb300d4c88b4c06331cb14b5b4'),
        ...               maxsize_code=1,
        ...               hash='abcdefgh' * 8,
        ...               size=2097152),
        ...     ChunkInfo(crc32=0x7E5CE7AD,
        ...               uuid=ChunkUUID('940f071152d742fbbf4c818580f432dc'),
        ...               maxsize_code=0,
        ...               hash='01234567' * 8,
        ...               size=143941),
        ...     ChunkInfo(crc32=0xDCC847D8,
        ...               uuid=ChunkUUID('a5b605f26ea549f38658d217b7e8e784'),
        ...               maxsize_code=1,
        ...               hash='76543210' * 8,
        ...               size=2097152)
        ... ]
        >>> str(Chunk.select_duplicates_expr(chunks)) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT arg_chunks.orig_uuid, chunk.uuid
        \nFROM
            (SELECT
                :v_0_orig_uuid AS orig_uuid,
                :v_0_hash AS hash,
                :v_0_size AS size
            UNION SELECT
                :v_1_orig_uuid AS orig_uuid,
                :v_1_hash AS hash,
                :v_1_size AS size
            UNION SELECT
                :v_2_orig_uuid AS orig_uuid,
                :v_2_hash AS hash,
                :v_2_size AS size)
            AS arg_chunks
            JOIN chunk
                ON chunk.hash = arg_chunks.hash AND
                   chunk.size = arg_chunks.size'

        @param chunks_: the iterable over chunks to find; B{must} be non-empty!
        @type chunks_: col.Iterable
        """
        non_empty_chunks, chunks_ = inonempty(chunks_)
        assert non_empty_chunks

        # These queries contain the hardcoded values
        select_exprs = (select([bindparam('v_{:d}_orig_uuid'.format(i))
                                    .label('orig_uuid'),
                                bindparam('v_{:d}_hash'.format(i))
                                    .label('hash'),
                                bindparam('v_{:d}_size'.format(i))
                                    .label('size')])
                            for i, ch in enumerate(chunks_))

        union_expr = union(*select_exprs).alias('arg_chunks')

        # Now C{union_expr} contains the VALUES
        # (orig_uuid, hash, size), bound to the parameters
        # from C{bound_all_params}.

        # Let's create the multi-query that gets all the matches
        # from the DB.
        return select([union_expr.c.orig_uuid,
                       chunks.c.uuid],
                      from_obj=union_expr.join(chunks,
                                               (union_expr.c.hash
                                                    == chunks.c.hash) &
                                               (union_expr.c.size
                                                    == chunks.c.size)))


    @classmethod
    def select_duplicates_bindparam_values(cls, chunks_):
        r"""
        Get the bindparam values dictionary for the expression
        calculated by C{select_duplicates_expr()}.

        >>> from common.chunks import ChunkInfo
        >>> from common.typed_uuids import ChunkUUID
        >>> chunks = [
        ...     ChunkInfo(crc32=0x07FD7A5B,
        ...               uuid=ChunkUUID('5b237ceb300d4c88b4c06331cb14b5b4'),
        ...               maxsize_code=1,
        ...               hash='abcdefgh' * 8,
        ...               size=2097152),
        ...     ChunkInfo(crc32=0x7E5CE7AD,
        ...               uuid=ChunkUUID('940f071152d742fbbf4c818580f432dc'),
        ...               maxsize_code=0,
        ...               hash='01234567' * 8,
        ...               size=143941),
        ...     ChunkInfo(crc32=0xDCC847D8,
        ...               uuid=ChunkUUID('a5b605f26ea549f38658d217b7e8e784'),
        ...               maxsize_code=1,
        ...               hash='76543210' * 8,
        ...               size=2097152)
        ... ]
        >>> str(Chunk.select_duplicates_bindparam_values(chunks))\
        ...     # doctest:+NORMALIZE_WHITESPACE,+ELLIPSIS
        "{'v_0_orig_uuid': ChunkUUID('5b237ceb-300d-4c88-b4c0-6331cb14b5b4'),
          'v_1_size': 143941, 'v_2_size': 2097152,
          'v_1_orig_uuid': ChunkUUID('940f0711-52d7-42fb-bf4c-818580f432dc'),
          'v_2_orig_uuid': ChunkUUID('a5b605f2-6ea5-49f3-8658-d217b7e8e784'),
          'v_0_hash': <read-only buffer for 0x..., size -1,
                                                   offset 0 at 0x...>,
          'v_2_hash': <read-only buffer for 0x..., size -1,
                                                   offset 0 at 0x...>,
          'v_0_size': 2097152,
          'v_1_hash': <read-only buffer for 0x..., size -1,
                                                   offset 0 at 0x...>}"

        @param chunks_: the iterable over chunks to find; B{must} be non-empty!
        @type chunks_: col.Iterable
        """
        chunks_ = list(chunks_)  # will be iterated multiple times
        assert chunks_

        # Let's create the bound params; they will end up as a mapping,
        # but the sources (_bound_hash_params and _bound_size_params)
        # will contain the iterables over the key-value tuples.
        _bound_orig_uuid_params = (('v_{:d}_orig_uuid'.format(i), c.uuid)
                                       for i, c in enumerate(chunks_))
        _bound_hash_params = (('v_{:d}_hash'.format(i), buffer(c.hash))
                                  for i, c in enumerate(chunks_))
        _bound_size_params = (('v_{:d}_size'.format(i), c.size())
                                  for i, c in enumerate(chunks_))
        return {k: v
                    for k, v in chain(_bound_orig_uuid_params,
                                      _bound_hash_params,
                                      _bound_size_params)}


chunks = Chunk.__table__



class ChunkPerHost(Base):
    """
    Chunk is present on a host.
    """

    __tablename__ = 'chunk_per_host'
    __table_args__ = (UniqueConstraint('chunk', 'host'),
                      Index('chunk_per_host_fk_host',
                            'host'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    chunk = Column('chunk_id',
                   ForeignKey(Chunk.id),
                   key='chunk',
                   nullable=False)

    host = Column('host_id',
                  ForeignKey(Host.host_id),
                  key='host',
                  nullable=False)


    @classmethod
    @memoized
    def chunk_per_host_view(cls):
        r"""
        Get an expression for all the chunks (plus their UUIDs) stored on the
        hosts (plus their UUIDs).

        >>> str(ChunkPerHost.chunk_per_host_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            chunk_per_host.id,
            host_view.id AS host_id,
            host_view.uuid AS host_uuid,
            chunk.id AS chunk_id,
            chunk.crc32 AS chunk_crc32,
            chunk.uuid AS chunk_uuid,
            chunk.hash AS chunk_hash,
            coalesce(chunk.size, 0) AS chunk_size,
            chunk.maxsize_code AS chunk_maxsize_code
        \nFROM
            (SELECT
                inhabitant.id AS id,
                inhabitant.uuid AS uuid,
                inhabitant.urls AS urls,
                host.name AS name,
                host.user_id AS user_id
            \nFROM
                host
                JOIN inhabitant ON inhabitant.id = host.id)
            AS host_view
            JOIN chunk_per_host ON host_view.id = chunk_per_host.host_id
            JOIN chunk ON chunk_per_host.chunk_id = chunk.id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        host_var = Host.host_view().alias('host_view')
        return select([chunks_per_host.c.id,
                       host_var.c.id.label('host_id'),
                       host_var.c.uuid.label('host_uuid'),
                       chunks.c.id.label('chunk_id'),
                       chunks.c.crc32.label('chunk_crc32'),
                       chunks.c.uuid.label('chunk_uuid'),
                       chunks.c.hash.label('chunk_hash'),
                       func.coalesce(chunks.c.size, literal_column('0'))
                           .label('chunk_size'),
                       chunks.c.maxsize_code.label('chunk_maxsize_code')],
                      from_obj=host_var.join(chunks_per_host,
                                             host_var.c.id ==
                                                 chunks_per_host.c.host)
                                       .join(chunks,
                                             chunks_per_host.c.chunk ==
                                                 chunks.c.id))


    @classmethod
    @memoized
    def used_space_stat_view(cls):
        r"""
        Get an expression that combines each host with the total size of all
        the chunks stored on this host.

        >>> str(ChunkPerHost.used_space_stat_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            host.id AS host_id,
            coalesce(sum(chunk_per_host_view.chunk_size), 0)
                AS used_size
        \nFROM
            host
            JOIN
                (SELECT
                    chunk_per_host.id AS id,
                    host_view.id AS host_id,
                    host_view.uuid AS host_uuid,
                    chunk.id AS chunk_id,
                    chunk.crc32 AS chunk_crc32,
                    chunk.uuid AS chunk_uuid,
                    chunk.hash AS chunk_hash,
                    coalesce(chunk.size, 0) AS chunk_size,
                    chunk.maxsize_code AS chunk_maxsize_code
                \nFROM
                    (SELECT
                        inhabitant.id AS id,
                        inhabitant.uuid AS uuid,
                        inhabitant.urls AS urls,
                        host.name AS name,
                        host.user_id AS user_id
                    \nFROM
                        host
                        JOIN inhabitant ON inhabitant.id = host.id)
                    AS host_view
                    JOIN chunk_per_host
                        ON host_view.id = chunk_per_host.host_id
                    JOIN chunk ON chunk_per_host.chunk_id = chunk.id)
                AS chunk_per_host_view
                    ON chunk_per_host_view.host_id = host.id
        GROUP BY host.id'

        @rtype: sqlalchemy.sql.expression.Executable

        @todo: probably, may be optimized; see the todo inside the code.
        """
        # TODO someday: investigate do we need to join chunk_per_host_view,
        # or maybe we can just join chunks_per_host directly
        chunk_per_host_var = \
            cls.chunk_per_host_view().alias('chunk_per_host_view')

        return select([hosts.c.host_id.label('host_id'),
                       func.coalesce(func.sum(chunk_per_host_var.c.chunk_size),
                                     literal_column('0'))
                           .label('used_size')],
                      from_obj=hosts.join(chunk_per_host_var,
                                          chunk_per_host_var.c.host_id ==
                                              hosts.c.host_id)) \
                   .group_by(hosts.c.host_id)


    @classmethod
    @memoized
    def space_stat_view(cls):
        r"""
        Get an expression that combines each host with the total size of all
        the chunks stored on this host, and the maximum that the host permitted
        to store.

        >>> str(ChunkPerHost.space_stat_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        "SELECT
            inhabitant.id AS host_id,
            inhabitant.uuid AS host_uuid,
            max_storage_space_view.max_size AS max_size,
            used_space_stat_view.used_size AS used_size
        \nFROM
            inhabitant
            JOIN host ON inhabitant.id = host.id
            JOIN
                (SELECT
                    setting_view.host_id AS host_id,
                    to_number(concat('0', setting_view.value),
                              'S999999999999999D99')
                        * 1048576 AS max_size
                \nFROM
                    (SELECT
                        setting.id AS id,
                        setting_name.id AS setting_name_id,
                        inhabitant.id AS host_id,
                        inhabitant.uuid AS host_uuid,
                        setting_name.name AS name,
                        setting.update_time AS update_time,
                        setting.value AS value
                    \nFROM
                        setting_name
                        JOIN setting
                            ON setting.setting_name_id = setting_name.id
                        JOIN inhabitant ON setting.host_id = inhabitant.id
                        JOIN host ON host.id = inhabitant.id)
                    AS setting_view
                \nWHERE setting_view.name = 'max storage size')
                AS max_storage_space_view
                ON max_storage_space_view.host_id = host.id
            JOIN
                (SELECT
                    host.id AS host_id,
                    coalesce(sum(chunk_per_host_view.chunk_size), 0)
                        AS used_size
                \nFROM
                    host
                    JOIN
                        (SELECT
                            chunk_per_host.id AS id,
                            host_view.id AS host_id,
                            host_view.uuid AS host_uuid,
                            chunk.id AS chunk_id,
                            chunk.crc32 AS chunk_crc32,
                            chunk.uuid AS chunk_uuid,
                            chunk.hash AS chunk_hash,
                            coalesce(chunk.size, 0) AS chunk_size,
                            chunk.maxsize_code AS chunk_maxsize_code
                        \nFROM
                            (SELECT
                                inhabitant.id AS id,
                                inhabitant.uuid AS uuid,
                                inhabitant.urls AS urls,
                                host.name AS name,
                                host.user_id AS user_id
                            \nFROM
                                host
                                JOIN inhabitant ON inhabitant.id = host.id)
                            AS host_view
                            JOIN chunk_per_host
                                ON host_view.id = chunk_per_host.host_id
                            JOIN chunk ON chunk_per_host.chunk_id = chunk.id)
                        AS chunk_per_host_view
                        ON chunk_per_host_view.host_id = host.id
                GROUP BY host.id)
                AS used_space_stat_view
                ON used_space_stat_view.host_id = host.id"

        @rtype: sqlalchemy.sql.expression.Select
        """
        max_storage_space_var = \
            Setting.max_storage_space_view().alias('max_storage_space_view')
        used_space_stat_var = \
            cls.used_space_stat_view().alias('used_space_stat_view')

        return select([inhabitants.c.id.label('host_id'),
                       inhabitants.c.uuid.label('host_uuid'),
                       max_storage_space_var.c.max_size.label('max_size'),
                       used_space_stat_var.c.used_size.label('used_size')],
                      from_obj=inhabitants.join(hosts)
                                          .join(max_storage_space_var,
                                                max_storage_space_var.c.host_id
                                                    == hosts.c.host_id)
                                          .join(used_space_stat_var,
                                                used_space_stat_var.c.host_id
                                                    == hosts.c.host_id))


    @classmethod
    @memoized
    def select_chunk_uuids_by_host_uuid_expr(cls):
        r"""
        Get the query that receives the UUIDs of the chunks stored
        on the host. The query accepts a single bound parameter C{host_uuid}.

        >>> str(ChunkPerHost.select_chunk_uuids_by_host_uuid_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT chunk.uuid
        \nFROM chunk
            JOIN chunk_per_host ON chunk.id = chunk_per_host.chunk_id
            JOIN host ON host.id = chunk_per_host.host_id
            JOIN inhabitant ON inhabitant.id = host.id
        \nWHERE inhabitant.uuid = :host_uuid'

        @rtype: sqlalchemy.sql.expression.Select
        """
        return select([chunks.c.uuid],
                      inhabitants.c.uuid == bindparam('host_uuid'),
                      from_obj=chunks.join(chunks_per_host)
                                     .join(hosts)
                                     .join(inhabitants))


chunks_per_host = ChunkPerHost.__table__



class Block(Base):
    """A block (a region in a chunk mapped to a region in a file).

    @todo: possibly, some unique constraint should be added to the database,
        like, UNIQUE(chunk, file), so that the repeated attempts to write
        do not duplicate the data. The duplicated data should be cleaned
        as well.
    """

    __tablename__ = 'block'
    __table_args__ = (Index('block_fk_chunk',
                            'chunk', 'file'),
                      Index('block_fk_file',
                            'file'),
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    chunk = Column('chunk_id',
                   ForeignKey(Chunk.id),
                   key='chunk',
                   nullable=False)

    file = Column('file_id',
                  ForeignKey(File.id),
                  key='file',
                  nullable=False)

    offset_in_file = Column(types.BigInteger,
                            CheckConstraint('offset_in_file >= 0'),
                            nullable=False)

    offset_in_chunk = Column(types.Integer,
                             CheckConstraint('offset_in_chunk >= 0'),
                             nullable=False)

    size = Column(types.Integer,
                  CheckConstraint('size > 0'),
                  nullable=False)


blocks = Block.__table__


if __debug__:
    __test__ = {
        'File.file_view.sqlite':
            r"""
            >>> str(File.file_view(dialect='sqlite'))  \
            ... # doctest:+NORMALIZE_WHITESPACE
            "SELECT
                file.id, file.group_id, file.crc32, file.uuid,
                file.fingerprint,
                (1152921504606846976 *
                     (CASE substr(file.fingerprint, 1, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 72057594037927936 *
                     (CASE substr(file.fingerprint, 2, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 4503599627370496 *
                     (CASE substr(file.fingerprint, 3, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 281474976710656 *
                     (CASE substr(file.fingerprint, 4, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 17592186044416 *
                     (CASE substr(file.fingerprint, 5, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 1099511627776 *
                     (CASE substr(file.fingerprint, 6, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 68719476736 *
                     (CASE substr(file.fingerprint, 7, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 4294967296 *
                     (CASE substr(file.fingerprint, 8, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 268435456 *
                     (CASE substr(file.fingerprint, 9, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 16777216 *
                     (CASE substr(file.fingerprint, 10, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 1048576 *
                     (CASE substr(file.fingerprint, 11, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 65536 *
                     (CASE substr(file.fingerprint, 12, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 4096 *
                     (CASE substr(file.fingerprint, 13, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 256 *
                     (CASE substr(file.fingerprint, 14, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 16 *
                     (CASE substr(file.fingerprint, 15, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END) +
                 1 *
                     (CASE substr(file.fingerprint, 16, 1)
                      WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                      WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
                      WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8
                      WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11
                      WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14
                      ELSE 15 END))
            AS size \nFROM file"
            """
    }

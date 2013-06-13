#!/usr/bin/python
"""
Host-specific SQLAlchemy models.
"""

from datetime import datetime

from sqlalchemy import (
    func, types, CheckConstraint, Column, Index, UniqueConstraint)

from common.db.models import *  # pylint:disable=W0401



#
# Models
#

class BaseDirectoryAtHost(Base):
    """
    (Host-specific) Base directory in the dataset.
    """

    __tablename__ = 'base_directory_at_host'

    __table_args__ = (UniqueConstraint('group', 'path'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    group = Column('group_id',
                   ForeignKey(UserGroup.id),
                   key='group',
                   nullable=False)

    path = Column(types.Text,
                  nullable=False)


base_directories_at_host = BaseDirectoryAtHost.__table__



class FileLocalAtHost(Base):
    """
    (Host-specific) Local file on the filesystem.
    """

    __tablename__ = 'file_local_at_host'

    __table_args__ = (UniqueConstraint('base_directory_at_host',
                                       'rel_dir_path',
                                       'rel_file_path'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    # FK index is a subset of
    # rel_path_unique_for_base_directory_at_host constraint
    base_directory_at_host = Column('base_directory_at_host_id',
                                    ForeignKey(BaseDirectoryAtHost.id),
                                    key='base_directory_at_host',
                                    nullable=False)

    rel_dir_path = Column(types.Text,
                          nullable=False)

    rel_file_path = Column(types.Text,
                           nullable=False)


files_local_at_host = FileLocalAtHost.__table__



class FileLocalStateAtHost(Base):
    """
    (Host-specific) A state in time of some local file on the filesystem.
    """

    __tablename__ = 'file_local_state_at_host'

    __table_args__ = (UniqueConstraint('file_local_at_host',
                                       'time_changed'),
                      # FK index
                      Index('file_local_state_at_host_fk_file_local',
                            'file_local'),
                      # The index to quickly get the latest changes
                      # (for a specific local file), and optionally filter
                      # whether they are backed up already.
                      Index('file_local_state_at_host_file_lch_ch_index',
                            'file_local_at_host',
                            'time_changed',
                            'file_local'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    # FK index is a subset of
    # time_changed_unique_for_file_local_at_host constraint
    file_local_at_host = Column('file_local_at_host_id',
                                ForeignKey(FileLocalAtHost.id),
                                key='file_local_at_host',
                                nullable=False)

    # FK is explicit
    file_local = Column('file_local_id',
                        ForeignKey(FileLocal.id),
                        key='file_local',
                        nullable=True)

    time_changed = Column(types.DateTime,
                          nullable=False,
                          default=datetime.utcnow,
                          server_default=func.current_timestamp())

    isdir = Column(types.Boolean,
                   nullable=False,
                   default=False,
                   server_default='0')

    size = Column(types.BigInteger,
                  CheckConstraint('size IS NULL or size >= 0'),
                  nullable=True)

    @classmethod
    @memoized
    def file_local_state_at_host_view(cls):
        r"""
        >>> str(FileLocalStateAtHost.file_local_state_at_host_view()) \
        ...     # doctest: +NORMALIZE_WHITESPACE
        'SELECT
            f_l_s_ah.id,
            f_l_s_ah.time_changed,
            f_l_s_ah.isdir,
            f_l_s_ah.size,
            base_directory_at_host.id AS base_dir_ah_id,
            base_directory_at_host.group_id AS base_dir_ah_group_id,
            base_directory_at_host.path AS base_dir_ah_path,
            f_l_ah.id AS file_local_ah_id,
            f_l_ah.rel_dir_path AS file_local_ah_rel_dir_path,
            f_l_ah.rel_file_path AS file_local_ah_rel_file_path,
            f_l_ah.id AS file_local_id
        \nFROM base_directory_at_host
            JOIN file_local_at_host AS f_l_ah
                ON base_directory_at_host.id = f_l_ah.base_directory_at_host_id
            JOIN file_local_state_at_host AS f_l_s_ah
                ON f_l_ah.id = f_l_s_ah.file_local_at_host_id
            LEFT OUTER JOIN file_local
                ON file_local.id = f_l_s_ah.file_local_id'


        @rtype: sqlalchemy.sql.expression.Executable
        """


        f_l_s_ah = file_local_states_at_host.alias('f_l_s_ah')
        f_l_ah = files_local_at_host.alias('f_l_ah')
        return select([f_l_s_ah.c.id,
                       f_l_s_ah.c.time_changed,
                       f_l_s_ah.c.isdir,
                       f_l_s_ah.c.size,
                       (base_directories_at_host.c.id)
                           .label('base_dir_ah_id'),
                       (base_directories_at_host.c.group)
                           .label('base_dir_ah_group_id'),
                       (base_directories_at_host.c.path)
                           .label('base_dir_ah_path'),
                       (f_l_ah.c.id)
                            .label('file_local_ah_id'),
                       (f_l_ah.c.rel_dir_path)
                            .label('file_local_ah_rel_dir_path'),
                       (f_l_ah.c.rel_file_path)
                            .label('file_local_ah_rel_file_path'),
                       (f_l_ah.c.id).label('file_local_id')],
                      from_obj=base_directories_at_host
                                    .join(f_l_ah)
                                    .join(f_l_s_ah)
                                    .outerjoin(files_local))


file_local_states_at_host = FileLocalStateAtHost.__table__



class DummyChunk(Chunk):
    """
    Dummy chunk.
    """

    __tablename__ = 'dummy_chunk'
    __table_args__ = ({'sqlite_autoincrement': True},)

    dummy_chunk_id = Column('id',
                            ForeignKey(Chunk.id),
                            key='dummy_chunk_id',
                            primary_key=True)


dummy_chunks = DummyChunk.__table__



class UploadedChunk(Chunk):
    """
    Uploaded chunk.
    """

    __tablename__ = 'uploaded_chunk'
    __table_args__ = ({'sqlite_autoincrement': True},)

    uploaded_chunk_id = Column('id',
                               ForeignKey(Chunk.id),
                               key='uploaded_chunk_id',
                               primary_key=True)


uploaded_chunks = UploadedChunk.__table__



class RestoreChunk(Chunk):
    """
    Restore chunk.
    """

    __tablename__ = 'restore_chunk'
    __table_args__ = ({'sqlite_autoincrement': True},)

    restore_chunk_id = Column('id',
                              ForeignKey(Chunk.id),
                              key='restore_chunk_id',
                              primary_key=True)

    since = Column(types.DateTime,
                   nullable=False,
                   default=datetime.utcnow,
                   server_default=func.current_timestamp())


restore_chunks = RestoreChunk.__table__



class ExpectedChunk(Chunk):
    """
    Expected chunk.
    """

    __tablename__ = 'expected_chunk'
    __table_args__ = (UniqueConstraint('host', 'chunk'),
                      Index('expected_chunk_fk_chunk',
                            'chunk'),
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    chunk_id = Column('chunk_id',
                      ForeignKey(Chunk.id),
                      key='chunk',
                      nullable=False)

    host = Column('host_id',
                  ForeignKey(Host.host_id),
                  key='host',
                  nullable=False)

    reason_restore = Column(types.Boolean,
                            nullable=False)

    since = Column(types.DateTime,
                   nullable=False,
                   default=datetime.utcnow,
                   server_default=func.current_timestamp())


expected_chunks = ExpectedChunk.__table__



class MyDataset(Dataset):
    """
    The dataset created on this host.
    """

    __tablename__ = 'my_dataset'

    __table_args__ = ({'sqlite_autoincrement': True},)

    my_dataset_id = Column('id',
                           ForeignKey(Dataset.id),
                           key='my_dataset_id',
                           primary_key=True)

    paused = Column(types.Boolean,
                    nullable=False,
                    default=False,
                    server_default='0')


my_datasets = MyDataset.__table__



class MyHost(Host):
    """
    The host itself, when looking from the host.
    """

    __tablename__ = 'my_host'

    __table_args__ = ({'sqlite_autoincrement': True},)

    my_host_id = Column('id',
                        ForeignKey(Host.host_id),
                        key='my_host_id',
                        primary_key=True)


my_hosts = MyHost.__table__

#!/usr/bin/python
"""
Node-specific SQLAlchemy models.
"""

from datetime import datetime

import sqlalchemy
from sqlalchemy import (
    select, func, types, CheckConstraint, Column, Index, literal_column,
    UniqueConstraint
)
from sqlalchemy.sql.expression import bindparam


from common.utils import memoized
from common.db.models import *  # pylint:disable=W0401



#
# Models
#

class Host_At_Node(Host):
    """
    (Node-specific) Host data, specific to the node.
    """

    __tablename__ = 'host_at_node'

    __table_args__ = ({'sqlite_autoincrement': True},)

    host_at_node_id = Column('id',
                             ForeignKey(Host.host_id),
                             key='host_at_node_id',
                             primary_key=True)

    last_seen = Column(types.DateTime,
                       nullable=True)


    @classmethod
    @memoized
    def host_at_node_view(cls):
        r"""
        Get an expression for all the hosts, as stored on the node with the
        extra information.

        >>> str(Host_At_Node.host_at_node_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            inhabitant.id,
            "user".id AS user_id,
            "user".name AS user_name,
            "user".digest AS user_digest,
            inhabitant.uuid,
            inhabitant.urls,
            host.name,
            host_at_node.last_seen
        \nFROM
            inhabitant
            JOIN host ON inhabitant.id = host.id
            JOIN host_at_node ON host.id = host_at_node.id
            JOIN "user" ON "user".id = host.user_id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        return select([inhabitants.c.id,
                       users.c.user_id.label('user_id'),
                       users.c.name.label('user_name'),
                       users.c.digest.label('user_digest'),
                       inhabitants.c.uuid,
                       inhabitants.c.urls,
                       hosts.c.name,
                       hosts_at_node.c.last_seen],
                      from_obj=inhabitants.join(hosts)
                                          .join(hosts_at_node)
                                          .join(users))


hosts_at_node = Host_At_Node.__table__



class Trusted_Host_At_Node(Host_At_Node):
    """
    (Node-specific) Trusted host data, specific to the node.
    """

    __tablename__ = 'trusted_host_at_node'

    __table_args__ = (Index('trusted_host_at_node_idx_id_for_storage',
                            'trusted_host_at_node_id', 'for_storage'),
                      Index('trusted_host_at_node_idx_id_for_restore',
                            'trusted_host_at_node_id', 'for_restore'),
                      {'sqlite_autoincrement': True})

    trusted_host_at_node_id = Column('id',
                                     ForeignKey(Host_At_Node.host_at_node_id),
                                     key='trusted_host_at_node_id',
                                     primary_key=True)

    for_storage = Column(types.Boolean,
                         nullable=False)

    for_restore = Column(types.Boolean,
                         nullable=False)


trusted_hosts_at_node = Trusted_Host_At_Node.__table__



class DatasetOnHost(Base):
    """
    (Node-specific) A dataset fully synchronized to a host.
    """

    __tablename__ = 'dataset_on_host'

    __table_args__ = (UniqueConstraint('dataset', 'host'),
                      Index('dataset_on_host_fk_host',
                            'host'),
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    dataset = Column('dataset_id',
                     ForeignKey(Dataset.id),
                     key='dataset',
                     nullable=False)

    host = Column('host_id',
                  ForeignKey(Host.host_id),
                  key='host',
                  nullable=False)

    time = Column(types.DateTime,
                  nullable=False,
                  default=datetime.utcnow,
                  server_default=func.current_timestamp())


    @classmethod
    @memoized
    def select_host_uuids_lacking_sync_dataset_expr(cls):
        r"""
        Get the query that generates the UUIDs of the hosts that lack
        some dataset (provided that it is a sync dataset).
        The query accepts a single bound parameter C{ds_uuid}.

        >>> str(DatasetOnHost.select_host_uuids_lacking_sync_dataset_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT DISTINCT other_inhabitant.uuid AS uuid
        \nFROM
            dataset
            JOIN membership AS other_membership
                ON dataset.group_id = other_membership.group_id
            JOIN user_at_node AS other_user_at_node
                ON other_user_at_node.id = other_membership.user_id
            JOIN host AS other_host
                ON other_host.user_id = other_user_at_node.id
            JOIN inhabitant AS other_inhabitant
                ON other_inhabitant.id = other_host.id
            LEFT OUTER JOIN dataset_on_host AS ds_on_other_host
                ON ds_on_other_host.dataset_id = dataset.id AND
                   ds_on_other_host.host_id = other_host.id
        \nWHERE
            dataset.uuid = :ds_uuid AND
            dataset.sync AND
            ds_on_other_host.id IS NULL AND
            NOT other_user_at_node.suspended'

        @rtype: sqlalchemy.sql.expression.Select
        """
        other_membership = memberships.alias('other_membership')
        other_user_at_node = users_at_node.alias('other_user_at_node')
        other_host = hosts.alias('other_host')
        other_inhabitant = inhabitants.alias('other_inhabitant')
        ds_on_other_host = datasets_on_host.alias('ds_on_other_host')

        return select(columns=[other_inhabitant.c.uuid.label('uuid')],
                      from_obj=datasets.join(other_membership,
                                             datasets.c.group ==
                                                 other_membership.c.group)
                                       .join(other_user_at_node,
                                             other_user_at_node.c
                                                               .user_at_node_id
                                                 == other_membership.c.user)
                                       .join(other_host,
                                             other_host.c.user ==
                                                 other_user_at_node
                                                     .c.user_at_node_id)
                                       .join(other_inhabitant)
                                       .join(ds_on_other_host,
                                             (ds_on_other_host.c.dataset ==
                                                  datasets.c.id) &
                                             (ds_on_other_host.c.host ==
                                                  other_host.c.host_id),
                                             isouter=True),
                      whereclause=(datasets.c.uuid == bindparam('ds_uuid')) &
                                  datasets.c.sync &
                                  # Keep "== None" instead of "is None",
                                  # cause it is parsed by SQLAlchemy!
                                  (ds_on_other_host.c.id == None) &
                                  (~other_user_at_node.c.suspended),
                      distinct=True)


    @classmethod
    @memoized
    def select_ds_uuids_groups_synced_to_host_expr(cls):
        r"""
        Get the query that provides the datasets (in fact, only their UUIDs
        and group UUIDs) that are present at some host.

        The query accepts a single bound parameter C{host_uuid}.

        >>> str(DatasetOnHost.select_ds_uuids_groups_synced_to_host_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT DISTINCT
            dataset.uuid AS ds_uuid,
            "group".uuid AS ugroup_uuid
        \nFROM
            inhabitant AS my_inhabitant
            JOIN host AS my_host
                ON my_inhabitant.id = my_host.id
            JOIN membership AS i_belong_to
                ON my_host.user_id = i_belong_to.user_id
            JOIN "group"
                ON "group".id = i_belong_to.group_id
            JOIN dataset
                ON "group".id = dataset.group_id
            JOIN dataset_on_host
                ON dataset_on_host.host_id = my_host.id AND
                   dataset_on_host.dataset_id = dataset.id
        \nWHERE
            my_inhabitant.uuid = :host_uuid AND
            dataset.sync AND
            dataset.time_completed IS NOT NULL'

        @rtype: sqlalchemy.sql.expression.Select
        """
        my_host = hosts.alias('my_host')
        my_inhabitant = inhabitants.alias('my_inhabitant')
        i_belong_to = memberships.alias('i_belong_to')

        return select(columns=[datasets.c.uuid.label('ds_uuid'),
                               user_groups.c.uuid.label('ugroup_uuid')],
                      from_obj=my_inhabitant
                                   .join(my_host)
                                   .join(i_belong_to,
                                         my_host.c.user == i_belong_to.c.user)
                                   .join(user_groups)
                                   .join(datasets)
                                   .join(datasets_on_host,
                                         (datasets_on_host.c.host ==
                                              my_host.c.host_id) &
                                         (datasets_on_host.c.dataset ==
                                              datasets.c.id)),
                      whereclause=(my_inhabitant.c.uuid ==
                                       bindparam('host_uuid')) &
                                  datasets.c.sync &
                                  # Keep "!= None" instead of "is not None",
                                  # cause it is parsed by SQLAlchemy!
                                  (datasets.c.time_completed != None),
                      distinct=True)


    @classmethod
    @memoized
    def select_datasets_not_synced_to_host_expr(cls):
        r"""
        Get the query that provides the datasets
        (actually, all the information for their building) that are
        missing at some host.

        The query accepts a single bound parameter C{host_uuid}.

        >>> str(DatasetOnHost.select_datasets_not_synced_to_host_expr()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT DISTINCT
            dataset.uuid AS ds_uuid,
            dataset.time_started AS ds_time_started,
            dataset.time_completed AS ds_time_completed,
            "group".uuid AS ugroup_uuid
        \nFROM
            inhabitant AS my_inhabitant
            JOIN host AS my_host
                ON my_inhabitant.id = my_host.id
            JOIN membership AS i_belong_to
                ON my_host.user_id = i_belong_to.user_id
            JOIN "group"
                ON "group".id = i_belong_to.group_id
            JOIN dataset
                ON "group".id = dataset.group_id
            LEFT OUTER JOIN dataset_on_host
                ON dataset_on_host.host_id = my_host.id AND
                   dataset_on_host.dataset_id = dataset.id
        \nWHERE
            my_inhabitant.uuid = :host_uuid AND
            dataset_on_host.id IS NULL AND
            dataset.sync AND
            dataset.time_completed IS NOT NULL'

        @rtype: sqlalchemy.sql.expression.Select
        """
        my_host = hosts.alias('my_host')
        my_inhabitant = inhabitants.alias('my_inhabitant')
        i_belong_to = memberships.alias('i_belong_to')

        return select(columns=[
                          datasets.c.uuid.label('ds_uuid'),
                          datasets.c.time_started.label('ds_time_started'),
                          datasets.c.time_completed.label('ds_time_completed'),
                          user_groups.c.uuid.label('ugroup_uuid')
                      ],
                      from_obj=my_inhabitant
                                   .join(my_host)
                                   .join(i_belong_to,
                                         my_host.c.user == i_belong_to.c.user)
                                   .join(user_groups)
                                   .join(datasets)
                                   .join(datasets_on_host,
                                         (datasets_on_host.c.host ==
                                              my_host.c.host_id) &
                                         (datasets_on_host.c.dataset ==
                                              datasets.c.id),
                                         isouter=True),
                      whereclause=(my_inhabitant.c.uuid ==
                                       bindparam('host_uuid')) &
                                  # Keep "== None" instead of "is None",
                                  # cause it is parsed by SQLAlchemy!
                                  (datasets_on_host.c.id == None) &
                                  datasets.c.sync &
                                  # Keep "!= None" instead of "is not None",
                                  # cause it is parsed by SQLAlchemy!
                                  (datasets.c.time_completed != None),
                      distinct=True)


datasets_on_host = DatasetOnHost.__table__



class ChunkTrafficStat(Base):
    """
    (Node-specific) The statistics of chunk traffic.
    """

    __tablename__ = 'chunk_traffic_stat'

    __table_args__ = (
                      # TODO: find out which indices are useful,
                      #       depending on the usage
                      {'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    # FK index is a subset of composite indices
    src_host = Column('src_host_id',
                      ForeignKey(Host.host_id),
                      key='src_host',
                      nullable=False)

    # FK index is a subset of composite indices
    dst_host = Column('dst_host_id',
                      ForeignKey(Host.host_id),
                      key='dst_host',
                      nullable=False)

    # FK index is a subset of composite indices
    chunk = Column('chunk_id',
                   ForeignKey(Chunk.id),
                   key='chunk',
                   nullable=False)

    time_completed = Column(types.DateTime,
                            nullable=False,
                            default=datetime.utcnow,
                            server_default=func.current_timestamp())

    bps = Column(types.Float,
                 CheckConstraint('bps > 0'),
                 nullable=False)


chunk_traffic_stats = ChunkTrafficStat.__table__



class StatDay(Base):
    """
    (Node-specific) A unique day (useful for statistics).
    """

    __tablename__ = 'stat_day'

    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    date = Column(types.Date,
                  nullable=False,
                  unique=True)


stat_days = StatDay.__table__



class StatTimeQuant(Base):
    """
    (Node-specific) A time quant (useful for statistics).
    """

    __tablename__ = 'stat_time_quant'

    __table_args__ = ({'sqlite_autoincrement': True},)

    id = Column(types.Integer,
                primary_key=True)

    time = Column(types.Time,
                  nullable=False,
                  unique=True)


stat_time_quants = StatTimeQuant.__table__



class PresenceStat(Base):
    """
    (Node-specific) The statistics of host online presence.
    """

    __tablename__ = 'presence_stat'

    __table_args__ = (UniqueConstraint('host',
                                       'stat_time_quant',
                                       'stat_day'),
                      # TODO: find out which extra indices are useful,
                      #       depending on the usage
                      Index('presence_stat_fk_host',
                            'host'),
                      {'sqlite_autoincrement': True})

    id = Column(types.Integer,
                primary_key=True)

    # FK index is a subset of composite indice
    host = Column('host_id',
                  ForeignKey(Host.host_id),
                  key='host',
                  nullable=False)

    # FK index is not a subset of any index! Should it be?
    stat_day = Column('stat_day_id',
                      ForeignKey(StatDay.id),
                      key='stat_day',
                      nullable=False)

    # FK index is not a subset of any index! Should it be?
    # It was, previously, a subset of index over
    # (stat_time_quant_id, host_id, stat_day_id).
    stat_time_quant = Column('stat_time_quant_id',
                             ForeignKey(StatTimeQuant.id),
                             key='stat_time_quant',
                             nullable=False)


    @classmethod
    @memoized
    def presence_stat_view(cls):
        r"""
        Get an expression that presents the presence statistic of each host
        in a convenient to use form.

        >>> str(PresenceStat.presence_stat_view()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            presence_stat.host_id,
            stat_day.date + stat_time_quant.time AS datetime
        \nFROM
            presence_stat
            JOIN stat_day ON stat_day.id = presence_stat.stat_day_id
            JOIN stat_time_quant
                ON stat_time_quant.id = presence_stat.stat_time_quant_id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        return select([presence_stats.c.host,
                       (stat_days.c.date + stat_time_quants.c.time)
                           .label('datetime')],
                      from_obj=presence_stats.join(stat_days)
                                             .join(stat_time_quants))


    @classmethod
    @memoized
    def users_were_online_last(cls):
        r"""
        Get an expression that combines every user with the time when it was
        online the last time.

        >>> str(PresenceStat.users_were_online_last()) \
        ...     # doctest:+NORMALIZE_WHITESPACE
        'SELECT
            "user".id,
            "user".name,
            max(presence_stat.datetime) AS last
        \nFROM
            "user"
            LEFT OUTER JOIN host ON "user".id = host.user_id
            LEFT OUTER JOIN
                (SELECT
                    presence_stat.host_id AS host_id,
                    stat_day.date + stat_time_quant.time AS datetime
                \nFROM
                    presence_stat
                    JOIN stat_day ON stat_day.id = presence_stat.stat_day_id
                    JOIN stat_time_quant
                        ON stat_time_quant.id =
                            presence_stat.stat_time_quant_id)
                AS presence_stat
                ON presence_stat.host_id = host.id
        GROUP BY "user".id'

        @rtype: sqlalchemy.sql.expression.Executable
        """
        presence_stat_var = cls.presence_stat_view().alias('presence_stat')
        return select([users.c.user_id,
                       users.c.name,
                       func.max(presence_stat_var.c.datetime)
                           .label('last')],
                      from_obj=users.join(hosts, isouter=True)
                                    .join(presence_stat_var,
                                          presence_stat_var.c.host ==
                                              hosts.c.host_id,
                                          isouter=True))\
                   .group_by(users.c.user_id)


presence_stats = PresenceStat.__table__



class User_At_Node(User):
    """
    (Node-specific) User data, specific to the node.
    """
    __tablename__ = 'user_at_node'
    __table_args__ = ({'sqlite_autoincrement': True},)

    user_at_node_id = Column('id',
                             ForeignKey(User.user_id),
                             key='user_at_node_id',
                             primary_key=True)

    suspended = Column(types.Boolean,
                       nullable=False,
                       default=False,
                       server_default='0')


users_at_node = User_At_Node.__table__

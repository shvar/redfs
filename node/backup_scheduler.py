#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Backup scheduler.

@note: obsolete.
"""

#
# Imports
#

from __future__ import absolute_import
import bisect
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from threading import RLock
from uuid import UUID

import pytz

from twisted.application.internet import TimerService

from contrib.dbc import contract_epydoc

from common.db import Queries
from common.server import ServerProcess
from common.utils import exceptions_logged, gen_uuid

from trusted import db
from trusted.db import TrustedQueries

from .timezones import is_naive_dt, utcnow



#
# Constants
#

logger = logging.getLogger(__name__)

BACKUP_SCHEDULES_CHECK_PERIOD = timedelta(seconds=1)



#
# Classes
#

class BackupSchedule(object):
    """The schedules, which can be sorted by the next-backup-time.

    @note: schedule support is disabled, so this class is not used anymore.
    """
    __slots__ = ('host_uuid', 'uuid', 'name', 'period', 'paths',
                 'next_backup_datetime')


    @exceptions_logged(logger)
    @contract_epydoc
    def __init__(self,
                 host_uuid, uuid, name, period, next_backup_datetime,
                 tz_info, paths):
        """Constructor.

        @type host_uuid: UUID
        @type uuid: UUID
        @type name: basestring
        @precondition: period.lower() in ('once', 'daily', 'weekly', 'monthly')
                       # period
        @type next_backup_datetime: datetime
        @precondition: is_naive_dt(next_backup_datetime) # next_backup_datetime
        @type tz_info: tzinfo
        @type paths: dict
        """
        self.host_uuid = host_uuid
        self.uuid = uuid
        self.name = name
        self.period = period.lower()
        self.next_backup_datetime = tz_info.localize(next_backup_datetime)
        self.paths = paths

        if __debug__:
            # Validate paths
            for per_path_setting in paths.itervalues():
                # path_settings is a dict with all setting for every path
                assert set(per_path_setting.iterkeys()) <= set(('f+', 'f-')), \
                       repr(per_path_setting)


    @property
    @contract_epydoc
    def next_backup_datetime_utc(self):
        """Represent the next_backup_time as UTC time.

        @postcondition: not is_naive_dt(result)
        @postcondition: result.tzinfo is pytz.utc
        """
        assert not is_naive_dt(self.next_backup_datetime), \
               repr(self.next_backup_datetime)

        return self.next_backup_datetime.astimezone(pytz.utc)


    @exceptions_logged(logger)
    @contract_epydoc
    def copy(self):
        """Return the copy of this schedule as a new schedule.

        @rtype: BackupSchedule
        """
        return BackupSchedule(host_uuid=self.host_uuid,
                              uuid=self.uuid,
                              name=self.name,
                              period=self.period,
                              next_backup_datetime=self.next_backup_datetime
                                                       .replace(tzinfo=None),
                              tz_info=self.next_backup_datetime.tzinfo,
                              paths=self.paths)


    def __repr__(self):
        return u'<BackupSchedule {} ({!r}) on {}, for host {}, {}>' \
                   .format(self.uuid,
                           self.name,
                           self.next_backup_datetime,
                           self.host_uuid,
                           self.period)


    def __cmp__(self, other):
        """
        If UUIDs match, the schedules match as well.
        Otherwise, they are ordered by the next backup UTC time first,
        then by UUID.
        """
        if self.uuid == other.uuid:
            assert self.host_uuid == other.host_uuid, \
                   (self.host_uuid, other.host_uuid)
            return 0
        else:
            return cmp(self.next_backup_datetime_utc,
                       other.next_backup_datetime_utc) or \
                   cmp(self.uuid,
                       other.uuid)


    @classmethod
    @contract_epydoc
    def from_dict(cls, host_uuid, tz_info, data_dict):
        """Create the instance from the dict-like representation.

        @type host_uuid: UUID
        @type tz_info: tzinfo
        @type data_dict: dict

        @rtype: BackupSchedule
        """
        return cls(host_uuid=host_uuid,
                   uuid=data_dict['uuid'],
                   name=data_dict['name'],
                   period=data_dict['period'],
                   next_backup_datetime=data_dict['next_backup_datetime'],
                   tz_info=tz_info,
                   paths=data_dict['paths'])


    @contract_epydoc
    def to_dict(self, json=True):
        """Convert into the portable dict-like representation.

        @param json: Whether the items are JSON-safe. TODO::: remove

        @rtype: dict
        """
        return {
            'uuid': self.uuid,
            'name': self.name,
            'period': self.period,
            'next_backup_datetime': self.next_backup_datetime
                                        .replace(tzinfo=None),
            'paths': self.paths
        }


    @classmethod
    @exceptions_logged(logger)
    @contract_epydoc
    def _dummy_schedule_for_time(cls, dt):
        """
        Create a dummy schedule item, with only the datetime field sensible,
        for various sorting purposes.

        @param dt: Datetime for the schedule item, assumed naive
                   (but may be non-naive as well).
        @type dt: datetime
        """
        naive_dt = dt if is_naive_dt(dt) \
                      else dt.astimezone(pytz.utc).replace(tzinfo=None)

        return cls(host_uuid=gen_uuid(),
                   uuid=gen_uuid(),
                   name='',
                   period='once',
                   next_backup_datetime=naive_dt,
                   tz_info=pytz.utc,
                   paths={})


    @exceptions_logged(logger)
    def advance_by_period(self):
        """
        Using the period stored in the schedule,
        advance forward to the next datetime, using the period as the step.

        @note: If the .next_backup_datetime field is None after this procedure,
               the schedule is complete and should not be fired anymore.

        @note: The time is advanced in the target's local timezone,
               rather than UTC.
        """
        assert self.next_backup_datetime is not None, \
               repr(self.next_backup_datetime)
        _now = utcnow()

        # For the new schedule, let's calculate the new datetime to fire...
        # ... maybe, do even multiple iterations.
        while (self.next_backup_datetime is not None and
               self.next_backup_datetime <= _now):
            if self.period == 'once':
                # Remove the schedule.
                # Mark schedule.next_backup_datetime as None
                self.next_backup_datetime = None

            elif self.period == 'daily':
                self.next_backup_datetime += timedelta(days=1)

            elif self.period == 'weekly':
                self.next_backup_datetime += timedelta(weeks=1)

            elif self.period == 'monthly':
                # Moving to the next month is not just adding
                # the month-long period;
                # we need to keep the date, if possible, so we'll do it
                # via replacing.
                old_dt = self.next_backup_datetime
                old_year = old_dt.year
                old_month = old_dt.month

                # ... but replacing the month may need to increment
                # the year as well, so replace the year.
                if old_month == 12:
                    new_year = old_year + 1
                    new_month = 1
                else:
                    new_year = old_year
                    new_month = old_month + 1

                # ... but there is a weird case of the impossible date,
                # such as February 30. In this case, we can do nothing but
                # move the day backwards, to 29, or to 28.
                # But note, that's irreversible!
                new_day = old_dt.day
                while True:
                    try:
                        self.next_backup_datetime = \
                            old_dt.replace(year=new_year,
                                           month=new_month,
                                           day=new_day)
                    except ValueError:
                        # Hmm, impossible date, as we've suspected.
                        # Try with the day
                        new_day -= 1
                    else:
                        # That's a possible date, so let's stick with it.
                        break

                    # Fool-proof check, to don't loop indefinitely:
                    if new_day <= 0:
                        raise Exception('For some weird reason, '
                                            "this datetime doesn't advance "
                                            'to the next {} period: {}'
                                            .format(self.period, old_dt))

            else:
                raise NotImplementedError(u'Period {}'.format(self.period))



class BackupScheduler(object):
    """The container for the backup schedules.

    Internally, it keeps the list of schedules in always-sorted state;
    they are sorted by the expected time of fire (then by UUID;
    but two schedules with the same UUID always match).

    @todo: The scheduler must be dynamically updated:
           1. if some "backup scheduled" settings are changed;
           2. if some "timezone" setting is changed.

    @note: schedule support is disabled, so this class is not used anymore.
    """
    __slots__ = ('server_process', 'lock',
                 '__schedules', '__schedules_by_host_uuid',
                 '__schedule_check_timer', '__last_reread_from_db')


    @exceptions_logged(logger)
    def __init__(self, server_process):
        assert isinstance(server_process, ServerProcess), \
               repr(server_process)

        self.server_process = server_process
        self.lock = RLock()

        self.__schedule_check_timer = \
            TimerService(BACKUP_SCHEDULES_CHECK_PERIOD.total_seconds(),
                         self.__on_schedule_check_timer)

        self.reread_cache()


    def __repr__(self):
        return u'<BackupScheduler: {} schedule(s)>'\
                   .format(len(self.__schedules))


    @property
    def app(self):
        """
        @rtype: NodeApp
        """
        return self.server_process.app


    @exceptions_logged(logger)
    def __reset(self):
        self.__schedules = []
        self.__schedules_by_host_uuid = defaultdict(set)


    @exceptions_logged(logger)
    def start(self):
        self.__schedule_check_timer.startService()


    @exceptions_logged(logger)
    def stop(self):
        """
        @todo: It is nowhere stopped at the moment, should it be?
        """
        self.__schedule_check_timer.stopService()


    @contract_epydoc
    def add(self, schedule):
        """
        @type schedule: BackupSchedule
        """
        with self.lock:
            assert schedule not in self.__schedules, repr(schedule)
            bisect.insort(self.__schedules, schedule)
            self.__schedules_by_host_uuid[schedule.host_uuid].add(schedule)
            assert self.__schedules == sorted(self.__schedules), 'Not sorted!'


    @contract_epydoc
    def remove(self, schedule):
        """
        @type schedule: BackupSchedule
        """
        with self.lock:
            assert schedule in self.__schedules, repr(schedule)
            index = bisect.bisect(self.__schedules, schedule) - 1
            assert schedule == self.__schedules[index], \
                   (index, schedule, self.__schedules)
            del self.__schedules[index]
            self.__schedules_by_host_uuid[schedule.host_uuid].remove(schedule)
            assert self.__schedules == sorted(self.__schedules), 'Not sorted!'


    @contract_epydoc
    def get_schedules_by_host_uuid(self, host_uuid):
        """
        @type host_uuid: UUID
        @rtype: frozenset
        """
        return frozenset(self.__schedules_by_host_uuid[host_uuid])


    @contract_epydoc
    def get_schedules_older_than(self, dt):
        """
        @param dt: The time, the schedules older than, are returned.
        @type dt: datetime

        @precondition: not is_naive_dt(dt) # repr(dt)

        @returns: The schedules which are older than the "dt".
        @rtype: list
        @postcondition: consists_of(result, BackupSchedule)
        """
        with self.lock:
            i = bisect.bisect(self.__schedules,
                              BackupSchedule._dummy_schedule_for_time(dt))
            return self.__schedules[:i]


    @contract_epydoc
    def reread_cache_for_host(self, host_uuid):
        """
        Reread all the schedules for a single host from the DB.
        It is assumed they've just been updated, so they should not be
        sent back.

        @param host_uuid: UUID of the host which schedules need to be updated.
        @type host_uuid: UUID
        """
        logger.debug('Rereading the schedules on %r for host %r',
                     self.server_process.me, host_uuid)

        with self.lock:
            old_schedules = self.get_schedules_by_host_uuid(host_uuid)
            logger.debug('Removing schedules for host %s: %r',
                         host_uuid, old_schedules)
            for schedule in old_schedules:
                self.remove(schedule)

            with db.RDB() as rdbw:
                new_schedules = \
                    Queries.Settings.get_setting(
                        host_uuid=host_uuid,
                        setting_name=Queries.Settings.BACKUP_SCHEDULE,
                        rdbw=rdbw)

                tz_name = Queries.Settings.get_setting(
                              host_uuid=host_uuid,
                              setting_name=Queries.Settings.TIMEZONE,
                              rdbw=rdbw)

            if tz_name:
                try:
                    tz_info = pytz.timezone(tz_name)
                except:
                    logger.error('Cannot parse timezone %r for host %r, '
                                     'fallback to UTC',
                                 tz_name, host_uuid)
                    tz_info = pytz.utc
            else:
                tz_info = pytz.utc

            logger.debug('Adding new schedules for host %s: %r',
                         host_uuid, new_schedules)
            for schedule in new_schedules:
                self.add(BackupSchedule.from_dict(host_uuid,
                                                  tz_info,
                                                  schedule))


    def reread_cache(self):
        """
        Reread the cache/info from the database.
        """
        self.__last_reread_from_db = utcnow()
        logger.debug('Rereading the schedules on %r', self.server_process.me)

        with self.lock:
            self.__reset()
            for host_uuid, (schedules, tz_name) \
                    in TrustedQueries.TrustedSettings.get_all_schedules() \
                                                     .iteritems():
                #
                if tz_name:
                    try:
                        tz_info = pytz.timezone(tz_name)
                    except:
                        logger.error('Cannot parse timezone %r for host %r, '
                                         'fallback to UTC',
                                     tz_name, host_uuid)
                        tz_info = pytz.utc
                else:
                    tz_info = pytz.utc

                for schedule in schedules:
                    self.add(BackupSchedule.from_dict(host_uuid,
                                                      tz_info,
                                                      schedule))

        logger.debug('Reread %i schedules on %r',
                     len(self.__schedules), self.server_process.me)


    @exceptions_logged(logger)
    def __on_schedule_check_timer(self):
        """
        Called whenever the schedule check timer is fired.
        """
        pass


    @exceptions_logged(logger)
    def __check_schedules(self):

        _now = utcnow()

        with self.lock:
            #
            # First, do we need to reread the schedules from the DB.
            #
            assert isinstance(self.__last_reread_from_db, datetime), \
                   repr(self.__last_reread_from_db)
            maxdelta = timedelta(seconds=BACKUP_SCHEDULES_REREAD_PERIOD
                                             .total_seconds())
            if _now - self.__last_reread_from_db > maxdelta:
                self.reread_cache()

            #
            # Now, we can check the schedules.
            #
            if self.__schedules:
                logger.debug('Checking for suspect schedules '
                                 'at %s among %i schedules...',
                             _now, len(self.__schedules))
            suspect_schedules = self.get_schedules_older_than(_now)
            if suspect_schedules:
                logger.debug('On node %r, the following (%i) schedules '
                                 'have passed their time:\n%s',
                             self.server_process.me, len(suspect_schedules),
                             '\n'.join(repr(s)
                                           for s in suspect_schedules))

                # But what hosts are actually alive at the moment?
                alive_host_uuids = \
                    [h.uuid
                         for h in self.app.known_hosts.alive_peers()]
                if alive_host_uuids:
                    logger.debug('Alive hosts at the moment are: %r',
                                 alive_host_uuids)
                # The schedule will fire a backup only if both its time is out,
                # and its host is alive.
                process_schedules = {sch
                                         for sch in suspect_schedules
                                         if sch.host_uuid in alive_host_uuids}

                if process_schedules:
                    processed_host_uuids = set()
                    logger.debug('The following (%i) schedules will fire:\n%s',
                                 len(suspect_schedules),
                                 '\n'.join(repr(sch)
                                               for sch in process_schedules))

                    # Loop over schedules, and run the backup transactions.
                    for schedule in process_schedules:
                        logger.debug('Firing a backup for schedule %r',
                                         schedule)

                        # TODO: Add "if user is suspended" check when
                        # TODO: BackupScheduler will be uncommented.
                        raise NotImplementedError
                        self.start_scheduled_backup(schedule)

                        new_schedule = schedule.copy()
                        new_schedule.advance_by_period()
                        logger.debug('%r advanced to %r',
                                     schedule, new_schedule)

                        # Remove old schedule; add new if needed.
                        self.remove(schedule)
                        if new_schedule.next_backup_datetime is not None:
                            self.add(new_schedule)

                        processed_host_uuids.add(new_schedule.host_uuid)

                    # We've done with the backup transactions.
                    # Would be cool to update the settings.
                    for host_uuid in processed_host_uuids:
                        schedules = self.get_schedules_by_host_uuid(
                                        host_uuid)
                        logger.debug('Updating the schedules '
                                         'for host %s:\n%r',
                                     host_uuid, schedules)
                        with db.RDB() as rdbw:
                            TrustedQueries.TrustedSettings.set_setting(
                                rdbw=rdbw,
                                host_uuid=host_uuid,
                                setting_name=Queries.Settings.BACKUP_SCHEDULE,
                                setting_value=[s.to_dict()
                                                   for s in schedules],
                                setting_time=_now.replace(tzinfo=None))
                        self.send_updated_schedules_to_host(host_uuid)


    def start_scheduled_backup(self, schedule):
        """Given a schedule, start an appropriate backup transaction."""
        assert isinstance(schedule, BackupSchedule), repr(schedule)

        _manager = self.server_process.tr_manager

        b_tr = _manager.create_new_transaction(
                    name='BACKUP',
                    src=self.server_process.me,
                    dst=self.app.known_hosts[schedule.host_uuid],
                    parent=None,
                    # BACKUP-specific
                    schedule_uuid=schedule.uuid,
                    schedule_name=schedule.name,
                    schedule_paths=schedule.paths)


    @contract_epydoc
    def send_updated_schedules_to_host(self, host_uuid):
        """
        Given an UUID of the host,
        start an appropriate "UPDATE_CONFIGURATION" transaction.

        @type host_uuid: UUID
        """
        _setting_name = Queries.Settings.BACKUP_SCHEDULE

        with db.RDB() as rdbw:
            settings = {_setting_name: Queries.Settings.get_setting(
                                           host_uuid=host_uuid,
                                           setting_name=_setting_name,
                                           direct=True,
                                           with_time=True,
                                           rdbw=rdbw)}

        uc_tr = self.server_process.tr_manager.create_new_transaction(
                    name='UPDATE_CONFIGURATION',
                    src=self.server_process.me,
                    dst=self.app.known_hosts[host_uuid],
                    parent=None,
                    # UPDATE_CONFIGURATION-specific
                    settings=settings)

#!/usr/bin/python

from __future__ import absolute_import
import logging
from uuid import uuid4

from twisted.application.internet import TimerService
from twisted.internet import reactor, threads

from common.utils import exceptions_logged


logger = logging.getLogger(__name__)
logger_status_backup = logging.getLogger('status.backup')
logger_status_backup_progress = logging.getLogger('status.backup.progress')
logger_status_restore = logging.getLogger('status.restore')
logger_status_restore_progress = logging.getLogger('status.restore.progress')



def _start_dummy_data_transfer_ventilator():
    #from random import choice

    def restore_started(uuid):
        logger_status_restore.info('dummy restore uuid: %s, status: %s',
                                   uuid, 'started',
                                   extra={'result': 'started',
                                          'ds_uuid': uuid,
                                          'tr_uuid': None,
                                          'target_dir': None})

    def restore_running(uuid):
        logger_status_restore.info('dummy restore uuid: %s, status: %s',
                                   uuid, 'running',
                                   extra={'result': 'running',
                                           'ds_uuid': uuid,
                                           'tr_uuid': None,
                                           'target_dir': None})

    def restore_finished(uuid):
        logger_status_restore.info('dummy restore uuid: %s, status: %s',
                                   uuid, 'finished',
                                   extra={'result': 'success',
                                          'ds_uuid': uuid,
                                          'tr_uuid': None,
                                          'target_dir': None})

    def restore_failed(uuid):
        logger_status_restore.info('dummy restore uuid: %s, status: %s',
                                   uuid, 'failed',
                                   extra={'result': 'fail',
                                          'ds_uuid': uuid,
                                          'tr_uuid': None,
                                          'target_dir': None})

    def restore_progress(uuid, transfered, total,
                         transfered_bytes, total_bytes):
        logger_status_restore_progress.info('dummy restore progresses with'
                                          + ' different transfered and total'
                                          + ' uuid: %s'
                                          + ' transfered: %s'
                                          + ' total %s',
                                            uuid, transfered, total,
                                            extra={'progresses': [
                                                {'uuid': uuid,
                                                'num': transfered,
                                                'of': total,
                                                'num_bytes': transfered_bytes,
                                                'of_bytes': total_bytes}
                                                ]})

    def backup_started(uuid):
        logger_status_backup.info('dummy backup uuid: %s, status: %s',
                                  uuid, 'started',
                                  extra={'status': 'started',
                                         'ds_uuid': uuid,
                                         'result_str': '',
                                         'relaunch_in_mins': None})

    def backup_running(uuid):
        logger_status_backup.info('dummy backup uuid: %s, status: %s',
                                  uuid, 'running',
                                  extra={'status': 'running',
                                         'ds_uuid': uuid,
                                         'result_str': '',
                                         'relaunch_in_mins': None})

    def backup_finished(uuid):
        logger_status_backup.info('dummy backup uuid: %s, status: %s',
                                  uuid, 'finished',
                                  extra={'status': 'ok',
                                         'ds_uuid': uuid,
                                         'result_str': '',
                                         'relaunch_in_mins': None})

    def backup_failed(uuid):
        logger_status_backup.info('dummy backup uuid: %s, status: %s',
                                  uuid, 'failed',
                                  extra={'status': 'fail',
                                        'ds_uuid': uuid,
                                        'result_str': '',
                                        'relaunch_in_mins': None})

    def backup_progress(uuid, transfered, total,
                        transfered_bytes, total_bytes):
        logger_status_backup_progress.info('dummy backup progress'
                                         + ' uuid: %s'
                                         + ' transfered: %s'
                                         + ' total %s',
                                           uuid, transfered, total,
                                           extra={'ds_uuid': uuid,
                                                  'num': transfered,
                                                  'of': total,
                                                  'num_bytes': transfered_bytes,
                                                  'of_bytes': total_bytes})

    def run_good_case():
        backup_uuid = uuid4()
        restore_uuid = uuid4()

        restore_total = 1000
        backup_total = 1000

        backup_started(backup_uuid)
        restore_started(restore_uuid)

        @exceptions_logged(logger)
        def _backup_progress(uuid, mult, total):
            backup_running(uuid)
            backup_progress(uuid, int(mult * total), total,
                            100000 * int(mult * total), 100000 * total)

        @exceptions_logged(logger)
        def _restore_progress(uuid, mult, total):
            restore_progress(uuid, int(mult * total), total,
                             100000 * int(mult * total), 100000 * total)
            restore_running(uuid)

        for i in xrange(1, 11):
            reactor.callLater(2 * i, _backup_progress,
                              backup_uuid, i * 10. / 100.0, backup_total)

            reactor.callLater(2 * i, _restore_progress,
                              restore_uuid, i * 10. / 100.0, restore_total)

        reactor.callLater(30, backup_finished, backup_uuid)
        reactor.callLater(30, restore_failed, restore_uuid)


    def run_bad_case():
        backup_uuid = uuid4()
        restore_uuid = uuid4()

        restore_total = 1000
        backup_total = 1000

        backup_started(backup_uuid)
        restore_started(restore_uuid)

        def _backup_progress(uuid, mult, total):
            backup_running(uuid)
            backup_progress(uuid, int(mult * total), total,
                            100000 * int(mult * total), 100000 * total)

        def _restore_progress(uuid, mult, total):
            restore_progress(uuid, int(mult * total), total,
                             100000 * int(mult * total), 100000 * total)
            restore_running(uuid)

        for i in xrange(1, 11):
            reactor.callLater(2 * i, _backup_progress, backup_uuid,
                              i * 10. / 100.0, backup_total)

            reactor.callLater(2 * i, _restore_progress, restore_uuid,
                              i * 9. / 100.0, restore_total)

        reactor.callLater(30, backup_finished, backup_uuid)
        reactor.callLater(30, restore_failed, restore_uuid)

    @exceptions_logged(logger)
    def run_case():
        if run_case.next == 'good':
            run_case.next = 'bad'
            run_good_case()
        else:
            run_case.next = 'good'
            run_bad_case()
    run_case.next = 'good'


    t = TimerService(50, run_case)
    t.startService()

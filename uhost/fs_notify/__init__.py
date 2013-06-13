#!/usr/bin/python
"""
The (more-or-less) universal mechanism to watch the FS for changes, no matter
of the OS.
"""

from __future__ import absolute_import
import sys


# For debugging purposes, let's make this file executable via something like
#   PYTHONPATH="." python uhost/fs_notify/__init__.py dir_to_watch


# Select FS notification engine depending on the reactor.
# You may import the resulting FSNotifyManager
if __name__ != '__main__':
    from .qt4_notify import Qt4FSNotifyManager as FSNotifyManager


if __debug__:
    def on_event_received(event):
        from datetime import datetime
        # print('{!r}'.format(event))
        # if hasattr(event, 'to_path'):
        #     print('from_path: p{!r}, f{!r}'.format(event.from_path.isdir(),
        #                                            event.from_path.isfile()))
        #     print('  to_path: p{!r}, f{!r}'.format(event.from_path.isdir(),
        #                                            event.from_path.isfile()))
        # else:
        #     print('path: p{!r}, f{!r}'.format(event.path.isdir(),
        #                                       event.path.isfile()))
        # print('')
        print '{!s}: {!r},'.format(datetime.utcnow(),
                                    event)

    # def create_test_file(directory):
    #     import random, string, os
    #     from datetime import datetime
    #     blocks_num = 8
    #     block_size = 1024*1024
    #     try:
    #         os.remove(os.path.join(directory, 'testfile'))
    #     except OSError:
    #         pass
    #     with open(os.path.join(directory, 'testfile'), 'wb') as f:
    #         for block_num in xrange(blocks_num):
    #             s = ''.join([random.choice(string.printable)
    #                  for i in xrange(block_size)])
    #             f.write(s)
    #             print '{!s}: writing block {!r} done.'.format(datetime.utcnow(),
    #                                                           block_num)
    #         print '{!s}: flushing file'.format(datetime.utcnow())
    #         f.flush()
    #         os.fsync(f)
    #     print '{!s}: write to file done'.format(datetime.utcnow())



    def main():
        from twisted.internet import reactor
        from common.twisted_utils import callLaterInThread
        print reactor
        if len(sys.argv) < 2:
            print('Pass the directory to watch!')
        else:
            _dir = sys.argv[1]
            manager = FSNotifyManager()
            print('Watching {!r}'.format(_dir))
            manager.watch(_dir, on_event_received)
            #callLaterInThread(1, create_test_file, _dir)
            reactor.run()  # pylint:disable=E1101


    if __name__ == '__main__':
        import sys
        sys.path.insert(0, '.')
        from common.system import install_reactor; 'OPTIONAL'  # snakefood
        import logging
        logging.basicConfig(level=logging.INFO)
        from common.logger import configure_logging
        configure_logging('./', 'test_notify')
        logger = logging.getLogger(__name__)
        install_reactor(force_reactor_name='qt4')
        from uhost import fs_notify; 'OPTIONAL'  # snakefood
        fs_notify.main()


__all__ = ('enable',)

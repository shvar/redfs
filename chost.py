#!/usr/bin/python
"""Host loader."""
import sys

if sys.platform == 'win32':
    sys.path.extend(['libs', 'libs/modules.dat'])
elif sys.platform.startswith('linux'):
    sys.path.extend(['/usr/share/pyshared/calathi'])

if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1:4] == ['-c',
                                                'from multiprocessing.forking import main; main()',
                                                '--multiprocessing-fork']:
        from multiprocessing.forking import main; main()
    else:
        import uhostcli
        uhostcli.main()


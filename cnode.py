#!/usr/bin/python
"""Node loader."""
import sys

if sys.platform.startswith('linux'):
    sys.path.extend(['/usr/share/pyshared/calathi'])

if __name__ == '__main__':
    import nodecli
    nodecli.main()

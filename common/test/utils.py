#!/usr/bin/python
"""
Various functions useful for testing.
"""

#
# Imports
#

import contextlib
import tempfile
import itertools
import os
import sys
from collections import namedtuple
from random import Random
from struct import pack
from uuid import UUID

from common import version
from common.utils import open_wb

from .lorem import LOREM



#
# Functions
#

GET_SAMPLE_DATA_RANDOM = Random(42)


class DataVariants(object):
    __slots__ = tuple()

    LOREM = 0
    PSEUDORANDOM = 1


def get_sample_data(variant):
    """
    @rtype: basestring
    """
    if variant == DataVariants.LOREM:
        return LOREM
    elif variant == DataVariants.PSEUDORANDOM:
        # 8 bits of pseudorandomness.
        global GET_SAMPLE_DATA_RANDOM
        return pack('!B', GET_SAMPLE_DATA_RANDOM.getrandbits(8))
    else:
        raise Exception('Unsupported variant {!r}'.format(variant))


def _test_create_tempfile(variant=0, repeat=1):
    """
    Create a temporary file with some artificial data,
    which is removed automatically when it becomes unused.

    @param variant: Which variant of the text to use.
    @param repeat: How many times to repeat the text.

    @returns: A file-like structure, which .name field may be used
              to refer to actual file.
    """
    assert repeat >= 1

    fh = tempfile.NamedTemporaryFile(suffix=version.project_name)

    fh.write(get_sample_data(variant) * repeat)
    fh.flush()
    os.fsync(fh.fileno())
    return fh


def _test_create_permfile(path, variant=0, repeat=1):
    """
    Create a file with some artificial data,
    which is NOT removed automatically when it becomes unused.
    It is the caller duty to remove the file when it becomes unused.

    @param variant: Which variant of the text to use.
    @param repeat: How many times to repeat the text.
    """
    assert repeat >= 1

    with open_wb(path) as fh:
        fh.write(get_sample_data(variant) * repeat)


TestDirVariant = \
    namedtuple('TestDirVariant', ('dir_depth',
                                  'min_subdirs', 'max_subdirs',
                                  'min_files', 'max_files',
                                  'min_size_code', 'max_size_code'))


_TEST_DIR_VARIANTS = [
    # 0
    TestDirVariant(dir_depth=3,
                   min_subdirs=1, max_subdirs=4,
                   min_files=1, max_files=16,
                   min_size_code=8, max_size_code=26),
    # 1
    TestDirVariant(dir_depth=4,
                   min_subdirs=1, max_subdirs=10,
                   min_files=1, max_files=16,
                   min_size_code=8, max_size_code=16),
    # 2
    TestDirVariant(dir_depth=3,
                   min_subdirs=0, max_subdirs=1,
                   min_files=1, max_files=4,
                   min_size_code=29, max_size_code=30),
    # 3
    TestDirVariant(dir_depth=4,
                   min_subdirs=8, max_subdirs=32,
                   min_files=8, max_files=32,
                   min_size_code=0, max_size_code=4),
]


def __create_dir(path, variant_index, rec=0):
    """Create some directory and fill it with pseudorandom data."""
    global GET_SAMPLE_DATA_RANDOM
    __r = GET_SAMPLE_DATA_RANDOM

    if not os.path.isdir(path):
        os.makedirs(path)

    if (isinstance(variant_index, int) and
        0 <= variant_index <= len(_TEST_DIR_VARIANTS) - 1):
        variant = _TEST_DIR_VARIANTS[variant_index]
    else:
        raise Exception('Only variants 0-{:d} are supported!'
                            .format(len(_TEST_DIR_VARIANTS) - 1))

    if rec < variant.dir_depth:
        # Create subdirectories
        subdirs = [u'dir-{:08x}'.format(i)
                       for i in __r.sample(xrange(variant.max_subdirs),
                                           __r.randint(variant.min_subdirs,
                                                       variant.max_subdirs))]

        if subdirs:
            for sd in __r.sample(subdirs,
                                 __r.randrange(len(subdirs))):
                __create_dir(os.path.join(path, sd), variant_index, rec + 1)

    # Create files
    subfiles = \
        [u'file-{:08x}'.format(i)
             for i in __r.sample(xrange(variant.max_files),
                                 __r.randint(variant.min_files,
                                             variant.max_files))]
    for file in subfiles:
        # The file size is randomly distributed over all the binary powers
        size_power = __r.randint(variant.min_size_code,
                                 variant.max_size_code)
        size = __r.randrange(2 ** size_power)

        # Create and fill the file
        with open(os.path.join(path, file), 'w+b') as fh:
            for i in itertools.count():
                text = '\n[Section {:d}]\n{:s}'.format(i, LOREM)
                text_len = len(text)
                fh.write(text[:min(text_len, size)])
                size -= text_len
                if size < 0:
                    break


def _test_create_permdir(base_path, variant=0):
    """
    Create a directory with some artificial data,
    which is NOT removed automatically when it becomes unused.
    It is the caller duty to remove the file when it becomes unused.

    @param variant: Which variant of the directory data to use.
    """
    __create_dir(base_path, variant)


__GEN_UUID_RNGS = {}


def gen_uuid(option):
    """
    A special version of gen_uuid that generates UUIDs in a pseudorandom way,
    but repeatably for the same option

    @rtype: UUID

    >>> gen_uuid('dummy strange value 1')
    >>> gen_uuid('dummy strange value 1')
    >>> gen_uuid('dummy strange value 1')
    >>> gen_uuid('dummy strange value 2')
    >>> gen_uuid('dummy strange value 2')
    """
    global __GEN_UUID_RNGS
    rng = __GEN_UUID_RNGS.setdefault(option, Random(option))
    return UUID(int=rng.getrandbits(128))


@contextlib.contextmanager
def spoof_as_secondary_thread():
    """
    Temporarily (during the context) alter the behaviour of
    the C{in_main_thread()} for it to always return C{False}.

    @note: obviously, NEVER call it outside unit tests!
    """
    import common.utils as common_utils
    assert common_utils._FORCE_SPOOF_AS_SECONDARY_THREAD is False
    assert 'doctest' in sys.modules, sys.modules.keys()
    assert 'unittest' in sys.modules, sys.modules.keys()

    common_utils._FORCE_SPOOF_AS_SECONDARY_THREAD = True
    yield
    common_utils._FORCE_SPOOF_AS_SECONDARY_THREAD = False



#
# Main
#

if __name__ == '__main__':
    pass

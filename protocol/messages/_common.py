#!/usr/bin/python
"""Common code for all protocol messages."""

import bz2
import json
import logging
from functools import wraps
from types import GeneratorType

from contrib.dbc import contract_epydoc

from common.http_transport import MessageProcessingException



#
# Constants
#

logger = logging.getLogger(__name__)
logger_bzip2 = logging.getLogger(__name__ + '.bzip2')

BZ2_COMPRESSLEVEL = 5



#
# Functions
#

def bzip2(f):
    """
    The decorator that can be put on any function returning
    basestring (in this context, likely C{get_body()} method
    of any AbstractMessage subclass), to transparently compress its output
    using BZ2 algorithm.

    @note: the decorated function is stored in C{._without_bzip2} field.

    @note: make sure to never call it from the main thread!
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        result_iter = f(*args, **kwargs)
        assert isinstance(result_iter, GeneratorType), repr(result_iter)

        compressor = bz2.BZ2Compressor(BZ2_COMPRESSLEVEL)

        size_unpacked = 0
        size_packed = 0

        # Compress the input data in an iterative way.
        for chunk in result_iter:
            size_unpacked += len(chunk)
            compressed = compressor.compress(chunk)
            size_packed += len(compressed)
            if compressed:
                yield compressed

        # Finalize
        compressed = compressor.flush()
        size_packed += len(compressed)
        if compressed:
            yield compressed

        logger_bzip2.debug('Compressed %i/%i (%.2f%%)',
                           size_packed,
                           size_unpacked,
                           100.0 * size_packed / size_unpacked
                               if size_unpacked
                               else float('inf'))

    wrapper.func_name = '{}|bzip2'.format(f.func_name)
    wrapper._without_bzip2 = f

    return wrapper


def bunzip2(f):
    """
    The decorator that can be put on any class method accepting
    a single basestring argument (in this context, likely
    C{init_from_body()} method of any AbstractMessage subclass),
    to transparently decompress the input using BZ2 algorithm.

    @note: the decorated function is stored in C{._without_bunzip2} field.

    @note: make sure to never call it from the main thread!

    @todo: convert into the coroutine-style consumer.
    """
    @wraps(f)
    @contract_epydoc
    def wrapper(self, compressed_data):
        """
        @type compressed_data: basestring
        """
        if __debug__:
            size_packed = len(compressed_data)
            logger_bzip2.debug('Decompressing %i bytes...', size_packed)

        decompressed = bz2.decompress(compressed_data)

        if __debug__:
            size_unpacked = len(decompressed)
            logger_bzip2.debug(
                'Decompressed %i/%i (%.2f%%)',
                size_packed,
                size_unpacked,
                100.0 * size_packed / size_unpacked if size_unpacked
                                                    else float('inf'))

        return f(self, decompressed)

    wrapper.func_name = '{}|bunzip2'.format(f.func_name)
    wrapper._without_bunzip2 = f

    return wrapper


def json_dumps(f):
    """
    The decorator that can be put on any function
    returning any serializeable data,
    (in this context, likely C{get_body()} method of any
    AbstractMessage subclass), to transparently encode its output
    using json.dumps.
    In __debug__ mode, it is possible that C{sort_keys = True} will be enabled.

    @note: the decorated function is stored in C{._without_json_dumps} field.

    @note: make sure to never call it from the main thread!

    @todo: implement lazy JSON generation somehow.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result_dict = f(*args, **kwargs)
            assert isinstance(result_dict, dict), repr(result_dict)

            # For now, it is not a proper generator (GeneratorType) yet
            # (and it always assumes the result is a dict on top-level),
            # but we must implement it at some point.
            try:
                res = json.dumps(result_dict,
                                 check_circular=False,
                                 sort_keys=__debug__)
            except Exception:
                logger.exception('Cannot dump JSON from %r', result_dict)
                raise
            yield res
        except Exception:
            logger.exception('Json dumping problem in wrapped %r(*%r, **%r)',
                             f, args, kwargs)
            raise

    wrapper.func_name = '{}|json_dumps'.format(f.func_name)
    wrapper._without_json_dumps = f

    return wrapper


def json_loads(f):
    """
    The decorator that can be put on any class method
    accepting a single basestring argument
    (in this context, likely C{init_from_body()} method of any
    AbstractMessage subclass), to transparently decode its input
    using json.loads.

    @note: the decorated function is stored in C{._without_json_loads} field.

    @note: make sure to never call it from the main thread!

    @todo: convert into the coroutine-style consumer.
    """
    @wraps(f)
    @contract_epydoc
    def wrapper(self, json_encoded):
        """
        @type json_encoded: basestring
        """
        if not json_encoded:
            raise MessageProcessingException('Empty body received!')
        try:
            return f(self, json.loads(json_encoded))
        except Exception:
            logger.exception('Json loading problem in wrapped %r(%r, %r)',
                             f, self, json_encoded)
            raise

    wrapper.func_name = '{}|json_loads'.format(f.func_name)
    wrapper._without_json_loads = f

    return wrapper

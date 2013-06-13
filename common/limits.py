#!/usr/bin/python
"""
Various limitations imposed on the system.
None of them should ever be changed!

@warning: NEVER change any of the values defined in this module dynamically.

@var MAX_FILE_SIZE: Maximum file size for the files supported by the system.
@type MAX_FILE_SIZE: int

@cvar SMALL_FILE_WATERMARK_CODE: The file size (in MiB) as power of 2,
    all the files smaller than it are considered "small" (i.e. they will get
    into the "small file chunk stream").
    B{WARNING}: altering this value B{may} decrease the chunk match
    probability between the different users.
@type SMALL_FILE_WATERMARK_CODE: int

@cvar SMALL_FILE_WATERMARK: Similar to SMALL_FILE_WATERMARK_CODE,
                            but measured in bytes.
@type SMALL_FILE_WATERMARK: int

@cvar MAX_CHUNK_SIZE_CODE: Maximum chunk size (in MiB) as power of 2.
    B{WARNING}: altering this value B{will} decrease the chunk match
    probability between the different users.
    Current value is 4, meaning that the maximum chunk size is 16 MiB.
@type MAX_CHUNK_SIZE_CODE: int

@cvar MAX_CHUNK_SIZE: Similar to MAX_CHUNK_SIZE_CODE, but measured in bytes.
@type MAX_CHUNK_SIZE: int

@cvar SMALLSTREAM_CHUNK_SIZE_CODE: The chunk size (in MiB) for the chunks
    in the "small chunks" chunk stream.
    Altering this value will B{not} significantly decrease the chunk match
    probability between the different users.
    Current value is 2, meaning that the chunk size is 4 MiB.
@type SMALLSTREAM_CHUNK_SIZE_CODE: int

@cvar SMALLSTREAM_CHUNK_SIZE: Similar to SMALLSTREAM_CHUNK_SIZE_CODE
                              but measured in bytes.
@type SMALLSTREAM_CHUNK_SIZE: int
"""

from __future__ import absolute_import

from .utils import size_code_to_size


# Maximum 8 bytes to store the file size
MAX_FILE_SIZE = 2 ** (8 * 8)

# 1 MiB
SMALL_FILE_WATERMARK_CODE = 0
SMALL_FILE_WATERMARK = size_code_to_size(SMALL_FILE_WATERMARK_CODE)

# 64 MiB
MAX_CHUNK_SIZE_CODE = 6
MAX_CHUNK_SIZE = size_code_to_size(MAX_CHUNK_SIZE_CODE)

# 4 MiB
SMALLSTREAM_CHUNK_SIZE_CODE = 2
SMALLSTREAM_CHUNK_SIZE = size_code_to_size(SMALLSTREAM_CHUNK_SIZE_CODE)

SUPPORTED_CHUNK_SIZE_CODES = frozenset(xrange(MAX_CHUNK_SIZE_CODE + 1))
SUPPORTED_CHUNK_SIZES = frozenset(size_code_to_size(i)
                                      for i in xrange(MAX_CHUNK_SIZE_CODE + 1))

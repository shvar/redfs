#!/usr/bin/python
"""
Various cryptographic functions.
"""

#
# Imports
#

from __future__ import absolute_import
import binascii
import collections as col
import logging
import numbers
import os
import operator
from abc import ABCMeta, abstractmethod
from binascii import hexlify, unhexlify
from hashlib import sha224, sha384, sha512  # pylint:disable=E0611
from itertools import imap, izip_longest
from functools import reduce
from struct import pack, unpack
from types import NoneType
from uuid import UUID

from contrib.dbc import contract_epydoc

import twisted.cred._digest as digest_auth

try:
    from M2Crypto.EVP import Cipher as m2crypto_Cipher
except ImportError:
    m2crypto_Cipher = None

try:
    from Crypto.Cipher import AES as pyCrypto_AES
except ImportError:
    pyCrypto_AES = None

from . import limits, version, settings
from .utils import POWERS_OF_TWO_SET

if __debug__:
    from .test.utils import DataVariants, _test_create_tempfile



#
# Constants
#

logger = logging.getLogger(__name__)
logger_fs_problem = logging.getLogger('status.fs_problem')

SYSTEM_KEY = 'R\xe3I\x8b(\xdc\xf5ot\xa1\xbb\xd6\xbbv\x04i6D\x9f\\L\x1a' \
             '\xb5\x9c\x7f\x84\xfc\xfc\t2\xb7\xbc'

AES_KEY_SIZE = 32  # 32 bytes, 256 bits
AES_IV_SIZE = 16  # AES.block_size, 16 bytes, 128 bits
AES_BLOCK_SIZE = 16  # AES.block_size

MAX_HASH = int('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
               'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', 16)

__test__ = {}  # will be updated below



#
# Functions, defined depending on the module availability
#

__TEST_SAMPLE_AES_KEY = 'abcdefghABCDEFGHabcdefghABCDEFGH'
__TEST_SAMPLE_AES_IV = '1234567887654321'
__TEST_SAMPLE_AES_PLAIN_TEXT = 'abcdefgh12345678'
__TEST_SAMPLE_AES_ENCRYPTED_TEXT = \
    '["\'\x97\x07\xb6\xb6\xfa\x1f\x8a\x05\xfb\x9dy7\n'


# python_m2crypto is optional
if m2crypto_Cipher is not None:

    def _aes_encrypt_m2crypto(key, iv, data):
        r"""
        >>> _aes_encrypt_m2crypto(__TEST_SAMPLE_AES_KEY,
        ...                       __TEST_SAMPLE_AES_IV,
        ...                       __TEST_SAMPLE_AES_PLAIN_TEXT) == \
        ...     __TEST_SAMPLE_AES_ENCRYPTED_TEXT
        True
        """
        c = m2crypto_Cipher(alg='aes_256_cbc',
                            key=key,
                            iv=iv,
                            op=1,
                            padding=False)
        r1 = c.update(data)
        return r1 + c.final()


    def _aes_decrypt_m2crypto(key, iv, data):
        r"""
        >>> _aes_decrypt_m2crypto(__TEST_SAMPLE_AES_KEY,
        ...                       __TEST_SAMPLE_AES_IV,
        ...                       __TEST_SAMPLE_AES_ENCRYPTED_TEXT) == \
        ...     __TEST_SAMPLE_AES_PLAIN_TEXT
        True
        """
        c = m2crypto_Cipher(alg='aes_256_cbc',
                            key=key,
                            iv=iv,
                            op=0,
                            padding=False)
        r1 = c.update(data)
        return r1 + c.final()


    __test__.update({
        '_aes_m2crypto':
            r"""
            >>> e_m2crypto = _aes_encrypt_m2crypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  __TEST_SAMPLE_AES_PLAIN_TEXT_2)
            >>> len(e_m2crypto)
            545664
            >>> hexlify(e_m2crypto)[:64]
            'f64febaf432c81b2a247e875acc87d1a494dd8ad96346799f7eae9d1011cab8c'
            >>> hexlify(e_m2crypto)[-64:]
            'd60c2b3f9bd4d2bdadf2ee67e80f56832ed040aeb3fcbd44146ac6935cc359b7'
            >>> _aes_decrypt_m2crypto(__TEST_SAMPLE_AES_KEY,
            ...                       __TEST_SAMPLE_AES_IV,
            ...                       e_m2crypto) == \
            ...     __TEST_SAMPLE_AES_PLAIN_TEXT_2
            True
            """,
    })


# python_crypto is optional
if pyCrypto_AES is not None:

    def _aes_encrypt_pycrypto(key, iv, data):
        r"""
        >>> _aes_encrypt_pycrypto(__TEST_SAMPLE_AES_KEY,
        ...                       __TEST_SAMPLE_AES_IV,
        ...                       __TEST_SAMPLE_AES_PLAIN_TEXT) == \
        ...     __TEST_SAMPLE_AES_ENCRYPTED_TEXT
        True
        """
        return pyCrypto_AES.new(key,
                                pyCrypto_AES.MODE_CBC,
                                iv).encrypt(data)


    def _aes_decrypt_pycrypto(key, iv, data):
        r"""
        >>> _aes_decrypt_pycrypto(__TEST_SAMPLE_AES_KEY,
        ...                       __TEST_SAMPLE_AES_IV,
        ...                       __TEST_SAMPLE_AES_ENCRYPTED_TEXT) == \
        ...     __TEST_SAMPLE_AES_PLAIN_TEXT
        True
        """
        return pyCrypto_AES.new(key,
                                pyCrypto_AES.MODE_CBC,
                                iv).decrypt(data)


    __test__.update({
        '_aes_pycrypto':
            r"""
            >>> e_pycrypto = _aes_encrypt_pycrypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  __TEST_SAMPLE_AES_PLAIN_TEXT_2)
            >>> len(e_pycrypto)
            545664
            >>> hexlify(e_pycrypto)[:64]
            'f64febaf432c81b2a247e875acc87d1a494dd8ad96346799f7eae9d1011cab8c'
            >>> hexlify(e_pycrypto)[-64:]
            'd60c2b3f9bd4d2bdadf2ee67e80f56832ed040aeb3fcbd44146ac6935cc359b7'
            >>> _aes_decrypt_pycrypto(__TEST_SAMPLE_AES_KEY,
            ...                       __TEST_SAMPLE_AES_IV,
            ...                       e_pycrypto) == \
            ...     __TEST_SAMPLE_AES_PLAIN_TEXT_2
            True
            """,
    })


# If both are installed, we have some extra unit tests
if m2crypto_Cipher is not None and pyCrypto_AES is not None:
    __test__.update({
        '_aes_encrypt':
            r"""
            >>> e_m2crypto = _aes_encrypt_m2crypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  __TEST_SAMPLE_AES_PLAIN_TEXT_2)
            >>> e_pycrypto = _aes_encrypt_pycrypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  __TEST_SAMPLE_AES_PLAIN_TEXT_2)
            >>> e_m2crypto == e_pycrypto
            True
            """,
        '_aes_decrypt':
            r"""
            >>> e_m2crypto = _aes_encrypt_m2crypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  __TEST_SAMPLE_AES_PLAIN_TEXT_2)
            >>> d_m2crypto = _aes_decrypt_m2crypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  e_m2crypto)
            >>> d_pycrypto = _aes_decrypt_pycrypto(
            ...                  __TEST_SAMPLE_AES_KEY,
            ...                  __TEST_SAMPLE_AES_IV,
            ...                  e_m2crypto)
            >>> d_m2crypto == __TEST_SAMPLE_AES_PLAIN_TEXT_2
            True
            >>> d_m2crypto == d_pycrypto
            True
            """,
    })


#
# Now define the encrypt/decrypt functions which are available unconditionally
#
if m2crypto_Cipher is not None:
    _aes_encrypt = _aes_encrypt_m2crypto
    _aes_decrypt = _aes_decrypt_m2crypto
elif pyCrypto_AES is not None:
    _aes_encrypt = _aes_encrypt_pycrypto
    _aes_decrypt = _aes_decrypt_pycrypto
else:
    raise ImportError('Neither M2Crypto nor PyCrypto are available!')


def co_crc32():
    r"""Coroutine-style CRC32 calculation.

    >>> crc32_gen = co_crc32(); crc32_gen.next()
    0
    >>> for i in xrange(500):
    ...     cur_crc32 = crc32_gen.send('<{:d}>'.format(i))
    ...     if i == 250:
    ...         print(cur_crc32)
    1426233064
    >>> cur_crc32
    4208741286

    >>> # For comparison
    >>> binascii.crc32(''.join('<{:d}>'.format(i) for i in xrange(251))) \
    ...     & 0xFFFFFFFF
    1426233064
    >>> binascii.crc32(''.join('<{:d}>'.format(i) for i in xrange(500))) \
    ...     & 0xFFFFFFFF
    4208741286

    @returns: a generator which accepts (via C{.send()}) the blocks of the data
              and streams out the CRC32's of the stream after each next block.
    """
    _crc32_func = binascii.crc32  # minor optimization
    current_crc32 = 0
    while True:
        input_ = yield current_crc32
        current_crc32 = _crc32_func(input_, current_crc32) & 0xFFFFFFFF


def co_sha512():
    r"""Coroutine-style SHA512 calculation.

    >>> sha512_gen = co_sha512(); _next = sha512_gen.next()
    >>> callable(_next)
    True
    >>> _next() # doctest:+ELLIPSIS
    "\xcf\x83\xe15...\xa582z\xf9'\xda>"
    >>> for i in xrange(500):
    ...     cur_sha512_func = sha512_gen.send('<{:d}>'.format(i))
    ...     if i == 250:
    ...         print(callable(cur_sha512_func),
    ...               hexlify(cur_sha512_func())) # doctest:+ELLIPSIS
    (True, '8e89e975c354dfdbdd805c1347001c...bd26f2ca9a293704595c1c3586c75fd')
    >>> callable(cur_sha512_func)
    True
    >>> hexlify(cur_sha512_func()) # doctest:+ELLIPSIS
    '93d03d723dab05c097c0a721a9f7e7c9...dcaeda2365741499ef10ed0a9f83ac802'

    >>> # For comparison
    >>> hexlify(sha512(''.join('<{:d}>'.format(i)
    ...                    for i in xrange(251))).digest())
    ...  # doctest:+ELLIPSIS
    '8e89e975c354dfdbdd805c1347001cfa...15bd26f2ca9a293704595c1c3586c75fd'
    >>> hexlify(sha512(''.join('<{:d}>'.format(i)
    ...                            for i in xrange(500))).digest())
    ...  # doctest:+ELLIPSIS
    '93d03d723dab05c097c0a721a9f7e7c9...dcaeda2365741499ef10ed0a9f83ac802'

    @returns: a generator which accepts (via C{.send()}) the blocks of the data
        and streams out the callables, which can generate SHA512 of the stream.
    """
    d_sha512 = sha512()
    while True:
        input_ = yield d_sha512.digest
        d_sha512.update(input_)


def crc32(input):
    r"""Eager CRC32 calculation.

    >>> crc32(''.join('<{:d}>'.format(i) for i in xrange(251)))
    1426233064
    >>> binascii.crc32(''.join('<{:d}>'.format(i) for i in xrange(251))) \
    ...     & 0xFFFFFFFF
    1426233064

    >>> crc32(''.join('<{:d}>'.format(i) for i in xrange(500)))
    4208741286
    >>> binascii.crc32(''.join('<{:d}>'.format(i) for i in xrange(500))) \
    ...     & 0xFFFFFFFF
    4208741286
    """
    return binascii.crc32(input) & 0xFFFFFFFF


def co_fingerprint(cryptographer):
    r"""Coroutine-style Fingerprint calculation.

    When using it, make sure to send 2048-bytes-aligned
    (originally, 16384-bytes-aligned) large blocks rather than short pieces,
    otherwise it will not be calculated properly.

    >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))

    >>> fp_gen = co_fingerprint(cr); _next = fp_gen.next()
    >>> callable(_next)
    True
    >>> _next()
    <Fingerprint: 00...f927da3e, 0 bytes>
    >>> for i in xrange(10):
    ...     # will be aligned to 16KiB
    ...     cur_fp_func = fp_gen.send('<{:d}>'.format(i))
    ...     if i == 5:
    ...         print(callable(cur_fp_func), repr(cur_fp_func()))
    (True, '<Fingerprint: 00...d014921d, 18 bytes>')

    >>> callable(cur_fp_func)
    True
    >>> cur_fp_func()
    <Fingerprint: 00...afe8d2d7, 30 bytes>

    >>> # For comparison
    >>> h = _test_create_tempfile()

    >>> fp_compare1 = Fingerprint.for_filename(h.name, cr)  # reference
    >>> fp_compare1
    <Fingerprint: 00...51e727da, 1745 bytes>

    >>> fp_gen2 = co_fingerprint(cr); _next = fp_gen2.next()
    >>> callable(_next)
    True
    >>> _next()
    <Fingerprint: 00...f927da3e, 0 bytes>
    >>> with open(h.name) as fh:
    ...     block = True
    ...     while block:
    ...         block = fh.read(16384)
    ...         fp_compare2 = fp_gen2.send(block)
    >>> callable(fp_compare2)
    True
    >>> fp_compare2()
    <Fingerprint: 00...51e727da, 1745 bytes>

    >>> fp_compare1 == fp_compare2()
    True

    >>> # What if we read in non-16KiB blocks?

    >>> # 2048 bytes: still good
    >>> fp_gen_2048 = co_fingerprint(cr); _next = fp_gen_2048.next()
    >>> callable(_next)
    True
    >>> _next()
    <Fingerprint: 00...f927da3e, 0 bytes>
    >>> with open(h.name) as fh:
    ...     block = True
    ...     while block:
    ...         block = fh.read(2048)
    ...         fp_compare_2048 = fp_gen_2048.send(block)
    >>> callable(fp_compare_2048)
    True
    >>> fp_compare1 == fp_compare_2048()
    True

    >>> # 17 bytes: BAD!
    >>> fp_gen_17 = co_fingerprint(cr); _next = fp_gen_17.next()
    >>> callable(_next)
    True
    >>> _next()
    <Fingerprint: 00...f927da3e, 0 bytes>
    >>> with open(h.name) as fh:
    ...     block = True
    ...     while block:
    ...         block = fh.read(17)
    ...         fp_compare_17 = fp_gen_17.send(block)
    >>> callable(fp_compare_17)
    True
    >>> fp_compare1 == fp_compare_17()
    False

    >>> # But even if reading by 17 bytes, we can reshape it
    >>> from common.itertools_ex import co_reshape_bytestream
    >>> fp_gen_17_not_rshpd = co_fingerprint(cr)
    >>> fp_gen_17_rshpd = co_reshape_bytestream(fp_gen_17_not_rshpd, 16384)
    >>> _next = fp_gen_17_rshpd.next()
    >>> callable(_next)
    True
    >>> _next()
    <Fingerprint: 00...f927da3e, 0 bytes>
    >>> with open(h.name) as fh:
    ...     block = True
    ...     while block:
    ...         block = fh.read(17)
    ...         fp_compare_17_rshpd = fp_gen_17_rshpd.send(block)
    >>> callable(fp_compare_17_rshpd)
    True
    >>> fp_compare1 == fp_compare_17_rshpd()
    True

    @type cryptographer: Cryptographer

    @returns: a generator which accepts (via C{.send()}) the blocks of the data
              and streams out the Fingerprint's of the stream
              after each next block.
    """
    sha512_gen = co_sha512()
    sha512_gen.next()
    cur_hash_func = sha512_gen.send('')

    _size = 0
    cur_digest_func = lambda: Fingerprint(size=_size, hash=cur_hash_func())
    while True:
        input_ = yield cur_digest_func
        for _16kib_block in Fingerprint._encrypt_by_16kib_blocks(
                                input_, cryptographer):
            # Actually, cur_hash_func is still the same, but assignment
            # is left here for clarity
            cur_hash_func = sha512_gen.send(_16kib_block)
        _size += len(input_)



#
# Classes
#

# We can don't mention it is inherited from collections.Hashable
# (that's an ABC, finally!), but better put it here explicitly
# for clarity.
class Fingerprint(col.Hashable):
    """A file fingerprint. 72 bytes (576 bits) long.

    @cvar NULL: An "empty" fingerprint.
    @type NULL: Fingerprint

    @ivar size: File size
    @type size: int

    @ivar hash: File hash (stored in packed string form).
    @type hash: str

    @invariant: len(self.hash) == 64
    @invariant: len(self.packed) == 72
    """
    __slots__ = ('size', 'hash')

    NULL = None


    # no @contract_epydoc anymore cause it is resource-consuming
    def __init__(self, size, hash):
        """Constructor.

        @param size: File size
        @type size: numbers.Integral

        @param hash: File SHA512 hash, either in unpacked or packed form.
        @type hash: (basestring, long)

        @precondition: 0 <= size <= limits.MAX_FILE_SIZE
        """
        if size > limits.MAX_FILE_SIZE:
            raise Exception('The file size of {:d} is larger than {}, '
                                'what is maximum supported by the system!'
                                .format(size, limits.MAX_FILE_SIZE))

        self.size = size

        # Autodetect hash type
        if isinstance(hash, basestring):
            l = len(hash)
            if l == 64:
                # Packed string
                self.hash = hash
            elif l == 128:
                # Unpacked string
                self.hash = unhexlify(hash)
            else:
                raise Exception('Wrong hash: {!r}'.format(hash))
        elif isinstance(hash, long):
            assert 0 <= hash <= MAX_HASH, repr(hash)
            # Long integer
            self.hash = unhexlify('{:0128x}'.format(hash))
        else:
            raise Exception('Hash must be either str or int, '
                                'rather that {!r}({})'
                                .format(hash, type(hash)))


    @contract_epydoc
    def __cmp__(self, other):
        r"""Compare two fingerprints.

        Special exception is the comparison of a C{Fingerprint} with C{None};
        in this case, C{Fingeprint} (and actually, just everything else)
        is considered larger than C{None}.

        >>> Fingerprint(1723, 12345L) > Fingerprint(1722, 12346L)
        True
        >>> Fingerprint(1723, 12345L) == Fingerprint(1723, 12345L)
        True
        >>> Fingerprint(1723, 12345L) > Fingerprint(1723, 12346L)
        True
        >>> Fingerprint(1723, 12345L) > None
        True
        >>> None < Fingerprint(1723, 12345L)
        True

        @type other: Fingerprint, NoneType
        """
        # Sizes are sorted in straight order;
        # if they are equal, hashes are sorted in reverse order.
        return 1 if other is None \
                 else cmp(self.size, other.size) or \
                      cmp(other.hash, self.hash)


    def __hash__(self):
        """
        hash() function support.
        """
        return hash((self.size, self.hash))


    def __repr__(self):
        hash_str = str(self)
        return u'<Fingerprint: {}...{}, {:d} bytes>' \
                   .format(hash_str[:2], hash_str[-8:], self.size)


    @contract_epydoc
    def __str__(self):
        r"""The textual representation of the fingerprint.

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> fp_str = str(Fingerprint.for_filename(h.name, cr))
        >>> fp_str  # doctest:+ELLIPSIS
        '00000000000006d140ac74e4c3a6a6e4cef032...d703113f8841534d51e727da'
        >>> fp_str.startswith('00000000000006d1')
        True
        >>> len(fp_str)
        144

        @postcondition: len(result) == 144
        """
        return hexlify(self.packed)


    @classmethod
    @contract_epydoc
    def from_string(cls, fp_string):
        r"""
        Given a textual representation of the fingerprint,
        return the appropriate object.

        >>> from base64 import b64encode
        >>> h = _test_create_tempfile()
        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))

        >>> fp1 = Fingerprint.for_filename(h.name, cr)

        >>> in_str = ('00000000000006d140ac74e4c3a6a6e4cef0'
        ...           '328566a407352cdb34a15e1aafacfa75b732'
        ...           'f09c2669de2c39aeaf6a620dc141a105b9e4'
        ...           '8fb6464a075cd703113f8841534d51e727da')
        >>> in_str == str(fp1)
        True

        >>> fp2 = Fingerprint.from_string(in_str)
        >>> str(fp2) == in_str
        True

        >>> fp2 == fp1
        True

        @type fp_string: basestring
        @precondition: len(fp_string) == 144

        @return: The fingerprint object for the string.
        @rtype: Fingerprint
        """
        return cls(size=int(fp_string[:16], 16),
                   hash=fp_string[16:])


    @property
    @contract_epydoc
    def as_buffer(self):
        r"""
        The fingerprint in the form of packed buffer.

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> buf = Fingerprint.for_filename(h.name, cr).as_buffer
        >>> type(buf)
        <type 'buffer'>
        >>> len(buf)
        72
        >>> str(buf) # doctest:+ELLIPSIS
        "\x00\x00\x00\x00\x00\x00\x06\xd1@\xact\xe4...\x11?\x88ASMQ\xe7'\xda"

        @return: A buffer-type object containing the fingerprint.
        @rtype: buffer
        @postcondition: len(result) == 72
        """
        return buffer(self.packed)


    @classmethod
    @contract_epydoc
    def from_buffer(cls, buf):
        r"""
        Given the packed buffer with fingerprint,
        create an appropriate Fingerprint object.

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> fp1 = Fingerprint.for_filename(h.name, cr); fp1
        <Fingerprint: 00...51e727da, 1745 bytes>
        >>> buf = fp1.as_buffer
        >>> type(buf)
        <type 'buffer'>
        >>> fp2 = Fingerprint.from_buffer(buf)
        >>> fp2
        <Fingerprint: 00...51e727da, 1745 bytes>
        >>> fp1 == fp2
        True

        @precondition: len(buf) == 72
        @param buf: The buffer with the packed fingerprint.
        @type buf: buffer

        @return: A Fingerprint object
        @rtype: Fingerprint
        """
        return cls(unpack('!Q', buf[:8])[0], buf[8:])


    @contract_epydoc
    def sqlite3_adapt(self):
        r"""Adapter for SQLite3 DBABI module.

        Use as::

          sqlite3.register_adapter(Fingerprint, Fingerprint.sqlite3_adapt)

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> fp_buf = Fingerprint.for_filename(h.name, cr).sqlite3_adapt()
        >>> type(fp_buf)
        <type 'buffer'>
        >>> len(fp_buf)
        72
        >>> hexlify(fp_buf) # doctest:+ELLIPSIS
        '00000000000006d140ac74e...8841534d51e727da'

        @rtype: buffer
        """
        return self.as_buffer


    @classmethod
    @contract_epydoc
    def sqlite3_convert(cls, value):
        r"""Converted for SQLite3 DBABI module.

        Use as::

        sqlite3.register_converter('FINGERPRINT', Fingerprint.sqlite3_convert)

        Normally, the FINGERPRINT should be stored as a BLOB in the sqlite3
        database i.e. returned as a buffer object; but when the conversion
        function is registered, the buffer will be returned.

        >>> in_str = ('00000000000006d1420cf2d3bc04f9cccb63'
        ...           '9750620f83746ecca08a4061c5682aaeef39'
        ...           '7b1e23893f9a5a32ec05db57f1538272dfea'
        ...           'dd27c2232cd49882e4be8b12899dfea9c8d4')

        >>> fp = Fingerprint.sqlite3_convert(unhexlify(in_str))
        >>> fp  # doctest:+ELLIPSIS
        <read-only buffer for 0x..., size -1, offset 0 at 0x...>
        >>> hexlify(fp)  # doctest:+ELLIPSIS
        '00000000000006d1420cf2d...8b12899dfea9c8d4'

        @type value: basestring

        @rtype: buffer
        """
        return buffer(value)


    @property
    @contract_epydoc
    def packed(self):
        r"""The fingerprint in the packed form.

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> result = Fingerprint.for_filename(h.name, cr)
        >>> len(result.packed)
        72

        @returns: File fingerprint (in packed string form, precisely
                  72 bytes long).
        @rtype: str
        @postcondition: len(result) == 72
        """
        return pack('!Q', self.size) + self.hash


    @classmethod
    @contract_epydoc
    def for_filename(cls, filename, cryptographer):
        r"""Given a filename, get the fingerprint for the appropriate file.

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> h = _test_create_tempfile()

        >>> Fingerprint.for_filename(h.name, cr)
        <Fingerprint: 00...51e727da, 1745 bytes>

        @param filename: The name of the file to get the fingerprint for.
        @type filename: basestring

        @param cryptographer: C{Cryptographer} object to use to encrypt
            the file 16Ki-blockwise before calculating the Fingerprint.
        @type cryptographer: Cryptographer

        @returns: file fingerprint.
        @rtype: Fingerprint
        """
        return cls(size=os.path.getsize(filename),
                   hash=Fingerprint._get_file_hash_packed(filename,
                                                          cryptographer))


    @classmethod
    def _encrypt_by_16kib_blocks(cls, data, cryptographer):
        """
        For fingerprint calculation, we need to take the file, split it by
        16KiB blocks (that is, having 1024 AES blocks each)
        (aligning the block to 16KiB by 0x00 bytes if it is shorter)
        and encrypt them independently.
        This allows both dependency of the fingerprint
        from the user/group encryption keys (by being encrypted),
        and CBC streaming benefits (turning into kind-of-ECB
        with 16KiB-size blocks) without moving away from simple encrypt/decrypt
        API.

        @param data: a string with the incoming data

        @returns: the iterator over 16KiB-long blocks of original data,
            each encrypted by cryptographer.
        """
        l = len(data)
        _16kib = 16 * 1024
        how_many_bytes_to_align = (-l) % _16kib
        align_bytes = '\x00' * how_many_bytes_to_align

        aligned_data = data + align_bytes
        aligned_len = l + how_many_bytes_to_align
        assert len(aligned_data) == aligned_len, (aligned_len, data)
        assert len(aligned_data) % _16kib == 0, repr(data)

        for i in xrange(0, aligned_len, _16kib):
            new_block = aligned_data[i:i + _16kib]
            yield cryptographer.encrypt(new_block, 0)


    @classmethod
    @contract_epydoc
    def _get_file_hash_packed(cls, filename, cryptographer):
        r"""
        Given the filename, calculate SHA512 hash for it and return in
        the packed form.

        >>> from base64 import b64encode

        >>> cr = Cryptographer(''.join(chr(i) for i in xrange(AES_KEY_SIZE)))
        >>> cr2 = Cryptographer('ABCDEFGH12345678ABCDEFGH12345678')

        >>> h = _test_create_tempfile()
        >>> res = Fingerprint._get_file_hash_packed(h.name, cr)
        >>> b64encode(res) # doctest:+ELLIPSIS
        'QKx05MOmpuTO8DKFZqQHNSzbNKFeGq+s+nW3...NcDET+IQVNNUecn2g=='

        >>> res2 = Fingerprint._get_file_hash_packed(h.name, cr2)
        >>> b64encode(res2) # doctest:+ELLIPSIS
        'wOM7/32aLo8HX+I3cCpX1YxOL0ydknJG4hwI...751s1JiV/JUi+1WyA=='

        >>> # Different cryptographers cause different hashes
        >>> res != res2
        True

        @param filename: The filename of a file to calculate SHA512 hash for.
        @type filename: basestring

        @param cryptographer: C{Cryptographer} object to use to encrypt
            the file 16Ki-blockwise before calculating the fingerprint.
        @type cryptographer: Cryptographer

        @returns: File SHA512 hash (in packed string form, precisely
                  64 bytes long).
                  If the file is unreadable, returns zero-filled string.
        @rtype: str
        @postcondition: len(result) == 64
        """
        # Hash part is in little-endian, for development convenience.
        try:
            d_sha512 = sha512()

            with open(filename, 'rb') as f:
                eof = False
                while not eof:
                    block = f.read(0x100000)  # 1 Mb blocks for speed

                    eof = not(len(block))
                    if not eof:
                        for _16kib_block in cls._encrypt_by_16kib_blocks(
                                                block, cryptographer):
                            d_sha512.update(_16kib_block)
            result = d_sha512.digest()

        except Exception as e:
            result = '\x00' * 64
            logger_fs_problem.warning('Cannot read file contents %r: %r',
                                      filename, e,
                                      extra={'path': filename,
                                             '_type': 'read file',
                                             'exc': e})

        return result



Fingerprint.NULL = Fingerprint(0, '0' * 128)


class AbstractCryptoKeyGenerator(object):
    """
    An abstract class that is capable to generate IV and encryption key
    depending on some data UUID.

    For example, given an UUID of chunk, it generates an appropriate
    IV and key for it.

    Various implementations may differ on methods of generation:
    some implementation may generate IV/key upon the keyboard input,
    some implementation may generate IV/key from the previously generated
    key file (key book).
    """
    __metaclass__ = ABCMeta


    @abstractmethod
    def generate_IV(self, crc32):
        """Generate the initialization vector.

        @note: Abstract method, thus no @contract_epydoc.

        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The IV for the data.
        @rtype: str
        @postcondition: len(result) == AES_IV_SIZE
        """
        pass


    @abstractmethod
    def generate_key(self, crc32):
        """Generate the encryption key vector.

        @note: Abstract method, thus no @contract_epydoc.

        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The encryption key for the data.
        @rtype: str
        @postcondition: len(result) == AES_KEY_SIZE
        """
        pass



class KeyboardInputKeyGenerator(AbstractCryptoKeyGenerator):
    r"""Key/IV generator based on the keyboard input.

    >>> gen = KeyboardInputKeyGenerator('love god sex')
    >>> iv = gen.generate_IV(0xFE00FFFF)
    >>> isinstance(iv, str) and len(iv) == AES_IV_SIZE
    True

    >>> key = gen.generate_key(0xFE00FFFF)
    >>> isinstance(key, str) and len(key) == AES_KEY_SIZE
    True
    """

    __slots__ = ('__password',)


    @contract_epydoc
    def __init__(self, password):
        """Constructor.

        @param password: The password which was entered from the keyboard.
        @type password: basestring
        """
        self.__password = password


    @contract_epydoc
    def generate_IV(self, crc32):
        """
        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The IV for the data.
        @rtype: str
        @postcondition: len(result) == AES_IV_SIZE
        """
        super(KeyboardInputKeyGenerator, self).generate_IV(crc32)
        return (sha384(self.__password).digest())[-AES_IV_SIZE:]


    @contract_epydoc
    def generate_key(self, crc32):
        """
        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The encryption key for the data.
        @rtype: str
        @postcondition: len(result) == AES_KEY_SIZE
        """
        super(KeyboardInputKeyGenerator, self).generate_key(crc32)
        return (sha384(self.__password).digest())[:AES_KEY_SIZE]



class KeybookKeyGenerator(AbstractCryptoKeyGenerator):
    r"""Key/IV generator based on the keybook.

    @todo: implement properly!

    @todo: also, fix tests in C{_test_cryptographer} below.

    # Prepare temporary file
    >>> # 16384 bytes - 512 keys
    >>> h = _test_create_tempfile(variant=DataVariants.PSEUDORANDOM,
    ...                           repeat=16384)
    >>> KeybookKeyGenerator.validate_file(h.name)
    True

    >>> # gen = KeybookKeyGenerator(h.name)
    >>> # iv = gen.generate_IV(0xFE00FFFF)
    >>> # isinstance(iv, str) and len(iv) == AES_IV_SIZE

    >>> # key = gen.generate_key(0xFE00FFFF)
    >>> # isinstance(key, str) and len(key) == AES_KEY_SIZE
    """

    class IncorrectFileException(BaseException):
        pass


    __slots__ = ('__keybook_file_path',)


    @contract_epydoc
    def __init__(self, keybook_file_path):
        """Constructor.

        @param keybook_file_path: The path to the keybook file.
        @type keybook_file_path: basestring
        """
        self.__keybook_file_path = keybook_file_path


    @contract_epydoc
    def generate_IV(self, crc32):
        """
        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The IV for the data.
        @rtype: basestring
        @postcondition: len(result) == AES_IV_SIZE
        """
        raise NotImplementedError()

        super(KeybookKeyGenerator, self).generate_IV(crc32)
        return (sha224(crc32.bytes).digest())[-AES_IV_SIZE:]


    @contract_epydoc
    def generate_key(self, crc32):
        """
        @param crc32: the CRC32 of the data to be encrypted.
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @return: The encryption key for the data.
        @rtype: str
        @postcondition: len(result) == AES_KEY_SIZE
        """
        raise NotImplementedError()
        super(KeybookKeyGenerator, self).generate_key(crc32)

        # We need to use the UUID number representation as the index
        # in the keybook, but using uuid itself is unsafe, cause
        # its trailing parts are usually the same.
        # So take the hash from it.
        uuid_as_crypto_safe_number = long(sha512(crc32.bytes).hexdigest(), 16)

        key_index = uuid_as_crypto_safe_number % \
                    (os.path.getsize(self.__keybook_file_path) / AES_KEY_SIZE)

        with open(self.__keybook_file_path, 'rb') as fh:
            fh.seek(AES_KEY_SIZE * key_index)
            result = fh.read(AES_KEY_SIZE)
            return result


    @staticmethod
    @contract_epydoc
    def validate_file(keybook_file_path):
        r"""
        Given a path to the keybook file, ensure the file is a valid keybook.

        >>> h_bad1 = _test_create_tempfile(variant=DataVariants.LOREM)
        >>> KeybookKeyGenerator.validate_file(h_bad1.name)
        False

        >>> h_bad2 = _test_create_tempfile(variant=DataVariants.PSEUDORANDOM,
        ...                                repeat=32 * 7)
        >>> KeybookKeyGenerator.validate_file(h_bad2.name)
        False

        >>> h_good = _test_create_tempfile(variant=DataVariants.PSEUDORANDOM,
        ...                                repeat=32 * 16)
        >>> KeybookKeyGenerator.validate_file(h_good.name)
        True

        @type keybook_file_path: basestring
        @rtype: bool
        """
        if os.path.isfile(keybook_file_path):
            size = os.path.getsize(keybook_file_path)
            if size and size % AES_KEY_SIZE == 0:
                keys_count = size / AES_KEY_SIZE
                return keys_count in POWERS_OF_TWO_SET

        return False



class Cryptographer(object):
    """
    The class that encapsulates the logic needed to encrypt or decrypt any data
    using one of the key generators.
    """
    __slots__ = ('__group_key', '__key_generator')


    # no @contract_epydoc anymore cause it is resource-consuming
    def __init__(self, group_key, key_generator=None):
        """Constructor.

        @param group_key: the encryption key for the user group;
            or C{None} if the group is not encrypted.
        @type group_key: str, NoneType

        @param: the C{AbstractCryptoKeyGenerator} object capable to generate
            the user keys.
        @type key_generator: NoneType, AbstractCryptoKeyGenerator

        @precondition: group_key is None or len(group_key) == AES_KEY_SIZE
        """
        self.__group_key = group_key
        self.__key_generator = key_generator


    # no @contract_epydoc anymore cause it is resource-consuming
    def _per_user_encryption_key(self, crc32):
        r"""Calculate per-user encryption key.

        >>> gen = KeyboardInputKeyGenerator('love god sex')
        >>> group_key = ''.join(chr(i) for i in xrange(AES_KEY_SIZE))
        >>> # Create cryptographers
        >>> cry = Cryptographer(group_key, gen)
        >>> cry._per_user_encryption_key(0xF3210000) == \
        ... '\xe5\t\xde\x004\xf8Fr\x05\xb7\xc4\xb2\xccg\x9b\xd56B\x10\xa9c!4' \
        ... 'G\x86\xdfW\xf1\xff!yu'
        True
        >>> # Note that per-user encryption key does not depend on CRC32
        >>> cry._per_user_encryption_key(0xF3210000) == \
        ... cry._per_user_encryption_key(0x12345678) == \
        ... cry._per_user_encryption_key(0xFEDCBA98)
        True

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        @rtype: str
        @postcondition: len(result) == AES_KEY_SIZE
        """
        return '\x00' * AES_KEY_SIZE if self.__key_generator is None \
                                     else self.__key_generator \
                                              .generate_key(crc32)


    # no @contract_epydoc anymore cause it is resource-consuming
    def key(self, crc32):
        """
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @rtype: str
        @postcondition: len(result) == AES_KEY_SIZE
        """
        result = xor_strings(pack('!L', crc32) * 4,
                             SYSTEM_KEY)

        if self.__group_key is not None:
            result = xor_strings(result, self.__group_key)

        _per_user_key = self._per_user_encryption_key(crc32)
        if _per_user_key is not None:
            result = xor_strings(result, _per_user_key)

        return result


    # no @contract_epydoc anymore cause it is resource-consuming
    def iv(self, crc32):
        """
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @rtype: str
        @postcondition: len(result) == AES_IV_SIZE
        """
        return '\x00' * AES_IV_SIZE \
                   if self.__key_generator is None \
                   else self.__key_generator.generate_IV(crc32)


    # no @contract_epydoc anymore cause it is resource-consuming
    def encrypt(self, data, crc32):
        """Perform the UUID-aware encryption of some data.

        @type data: str
        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF

        @rtype: str
        """
        return _aes_encrypt(self.key(crc32), self.iv(crc32), data)


    # no @contract_epydoc anymore cause it is resource-consuming
    def decrypt(self, data, crc32):
        """Perform the UUID-aware decryption of some data.

        @type crc32: int
        @precondition: 0 <= crc32 <= 0xFFFFFFFF
        """
        return _aes_decrypt(self.key(crc32), self.iv(crc32), data)



#
# Functions
#

@contract_epydoc
def generate_digest(username, password,
                    realm=version.project_internal_codename):
    r"""
    Generate the htdigest-similar digest for the username, password
    and the realm (version.project_name by default).

    The username is considered case-insensitive, so, for the purposes of both
    backward-compatibility and the insensitivity, it is forcibly converted
    to lowercase.

    >>> _realm = settings.HTTP_AUTH_REALM_NODE

    >>> generate_digest('alpha', 'alphapwd', _realm)
    '44b1c5ba49573a5bf4931a4e877aba187385b560'

    >>> generate_digest('AlpHa', 'alphapwd', _realm)
    '44b1c5ba49573a5bf4931a4e877aba187385b560'

    >>> # Logins are case-insensitive, passwords are not
    >>> (generate_digest('AlpHa', 'alphapwd', _realm) == \
    ...      generate_digest('alpha', 'alphapwd', _realm),
    ...  generate_digest('alpha', 'alphapwd', _realm) == \
    ...      generate_digest('alpha', 'AlphaPwd', _realm))
    (True, False)

    @type username: str
    @type password: basestring
    @type realm: str

    @returns: The digest password, equal to one as if the similar arguments
              are passed to htdigest command line tool.
    @rtype: str
    """
    return digest_auth.calcHA1(pszAlg='sha',
                               pszUserName=username.lower(),
                               pszRealm=realm,
                               pszPassword=password,
                               pszNonce=None, pszCNonce=None)


@contract_epydoc
def gen_rand_key(key_security_exp=0, cb=None):
    r"""
    Create the random key with the security exponent size of
    C{key_security_exp}.

    >>> len(gen_rand_key(8))
    8192

    @param key_security_exp: The security exponent of the key; default is 0.
    @type key_security_exp: int

    @param cb: Callback which is regularly called
               to notify about the key generation progress. Not used yet!
    @precondition: cb is None or callable(cb)

    @postcondition: len(result) == 32 * (2 ** key_security_exp)
    @rtype: buffer
    """
    results = []

    for i in xrange(2 ** key_security_exp):
        results.append(os.urandom(AES_KEY_SIZE))

    return buffer(''.join(results))


@contract_epydoc
def gen_empty_key(key_security_exp=0):
    r"""
    Create an empty key with the security exponent size of C{key_security_exp}.

    >>> len(gen_empty_key(8))
    8192

    >>> str(gen_empty_key(1)) == '\x00' * 64
    True

    @param key_security_exp: The security exponent of the key; default is 0.
    @type key_security_exp: int

    @postcondition: len(result) == 32 * (2 ** key_security_exp)
    @rtype: buffer
    """
    return buffer('\x00' * (AES_KEY_SIZE * 2 ** key_security_exp))


# @contract_epydoc - removed as resource-consuming
def xor_strings(*strings):
    r"""Given several strings, XOR them together.

    >>> xor_strings('abcdef', 'ABCDEF', '567890')
    '\x15\x16\x17\x18\x19\x10'
    >>> xor_strings('abcdef', 'ABCDEFG', '56789') == '\x15\x16\x17\x18\x19 G'
    True

    @rtype: str
    """
    return ''.join(imap(chr, (reduce(operator.xor, imap(ord, i))
                                  for i in izip_longest(fillvalue='\x00',
                                                        *strings))))



if __debug__:

    from random import Random
    _rng = Random(42)
    __TEST_SAMPLE_AES_PLAIN_TEXT_2 = ''.join(chr(_rng.randint(0, 255))
                                                     for i in xrange(545661)) \
                                     + '\x00\x00\x00'

    __test__ = {
        '_test_fingerprints':
            """
            >>> cr = Cryptographer(''.join(chr(i)
            ...                        for i in xrange(AES_KEY_SIZE)))

            # Prepare temporary file
            >>> cr = Cryptographer(''.join(chr(i)
            ...                        for i in xrange(AES_KEY_SIZE)))
            >>> h = _test_create_tempfile()

            >>> result = Fingerprint.for_filename(h.name, cr)
            >>> result
            <Fingerprint: 00...51e727da, 1745 bytes>

            # Calculate hashes.
            >>> hash1 = Fingerprint._get_file_hash_packed(h.name, cr)
            >>> hash2 = '%02x' * 64 % unpack('!64B', hash1)
            >>> hash3 = int(hash2, 16)

            >>> hash2 # doctest:+ELLIPSIS
            '40ac74e4c3a6a6e4...8841534d51e727da'

            >>> hash3 # doctest:+ELLIPSIS
            33872343491099851...5630485734500314L

            # Prepare fingerprints
            >>> fp1 = Fingerprint(1745, hash1)
            >>> fp2 = Fingerprint(1745, hash2)
            >>> fp3 = Fingerprint(1745, hash3)

            >>> result == fp1 == fp2 == fp3
            True

            >>> result.size
            1745
            """,

        '_test_cryptographer_encrypted_group':
            r"""
            >>> #
            >>> # Prepare key generators
            >>> #
            >>> h = _test_create_tempfile(variant=DataVariants.PSEUDORANDOM,
            ...                           repeat=16384)
            >>> # gen1 = KeybookKeyGenerator(h.name)
            >>> gen2 = KeyboardInputKeyGenerator('love god sex')
            >>> group_key = ''.join(chr(i) for i in xrange(AES_KEY_SIZE))

            >>> #
            >>> # Create cryptographers
            >>> #
            >>> # cry1 = Cryptographer(group_key, gen1)
            >>> cry2 = Cryptographer(group_key, gen2)

            >>> sample_text = 'alpha beta gamma'
            >>> sample_crc32_1 = 0xFE00FFFF
            >>> sample_crc32_2 = 0x00000001

            >>> from base64 import b64encode
            >>> # b64encode(cry1._per_user_encryption_key(sample_crc32_1))

            >>> # For keyboard input generator, per-user encryption keys
            >>> # depend on the password only
            >>> b64encode(cry2._per_user_encryption_key(sample_crc32_1))
            '5QneADT4RnIFt8SyzGeb1TZCEKljITRHht9X8f8heXU='
            >>> b64encode(cry2._per_user_encryption_key(sample_crc32_2))
            '5QneADT4RnIFt8SyzGeb1TZCEKljITRHht9X8f8heXU='

            >>> # b64encode(cry1.key(sample_crc32_1))
            >>> # b64encode(cry1.iv(sample_crc32_1))

            >>> b64encode(cry2.key(sample_crc32_1))
            'Setqd+YhSuWHH4qQhRxuTBAXneY7LpfM4UKxFuoO0NY='
            >>> b64encode(cry2.iv(sample_crc32_1))
            'NXs5UThCh9QQLETlcNsk5A=='
            >>> b64encode(cry2.key(sample_crc32_2))
            't+uViRghtRt5H3VuexyRshAXneY7LpfM4UKxFuoO0NY='
            >>> b64encode(cry2.iv(sample_crc32_2))
            'NXs5UThCh9QQLETlcNsk5A=='

            >>> #
            >>> # Test encryption by keybook-based cryptographer
            >>> #
            >>> # e1 = cry1.encrypt(sample_text, sample_crc32_1)
            >>> # e1
            >>> # cry1.decrypt(e1, sample_crc32_1) == sample_text

            >>> # cry1.decrypt(e1, sample_crc32_1)

            >>> #
            >>> # Test encryption by password-based cryptographer
            >>> #
            >>> e2 = cry2.encrypt(sample_text, sample_crc32_1)
            >>> e2
            '\xa7\xcd\xc4\xbb\xe7\xef\xe1\xd9<IhZ!!\xees'
            >>> # cry2.decrypt(e2, sample_crc32_1) == sample_text
            True

            >>> cry2.decrypt(e2, sample_crc32_1)
            'alpha beta gamma'
            """,

        '_test_cryptographer_non_encrypted_group':
            r"""
            >>> #
            >>> # Prepare key generators
            >>> #
            >>> h = _test_create_tempfile(variant=DataVariants.PSEUDORANDOM,
            ...                           repeat=16384)
            >>> # gen1 = KeybookKeyGenerator(h.name)
            >>> gen2 = KeyboardInputKeyGenerator('love god sex')

            >>> #
            >>> # Create cryptographers
            >>> #
            >>> # cry1 = Cryptographer(None, gen1)
            >>> cry2 = Cryptographer(None, gen2)

            >>> sample_text = 'alpha beta gamma'
            >>> sample_crc32_1 = 0xFE00FFFF
            >>> sample_crc32_2 = 0x00000001

            >>> from base64 import b64encode
            >>> # b64encode(cry1._per_user_encryption_key(sample_crc32_1))

            >>> # For keyboard input generator, per-user encryption keys
            >>> # depend on the password only
            >>> b64encode(cry2._per_user_encryption_key(sample_crc32_1))
            '5QneADT4RnIFt8SyzGeb1TZCEKljITRHht9X8f8heXU='
            >>> b64encode(cry2._per_user_encryption_key(sample_crc32_2))
            '5QneADT4RnIFt8SyzGeb1TZCEKljITRHht9X8f8heXU='

            >>> # b64encode(cry1.key(sample_crc32_1))
            >>> # b64encode(cry1.iv(sample_crc32_1))

            >>> b64encode(cry2.key(sample_crc32_1))
            'SepodOIkTOKPFoCbiRFgQwAGj/UvO4Hb+VurDfYTzsk='
            >>> b64encode(cry2.iv(sample_crc32_1))
            'NXs5UThCh9QQLETlcNsk5A=='
            >>> b64encode(cry2.key(sample_crc32_2))
            't+qXihwksxxxFn9ldxGfvQAGj/UvO4Hb+VurDfYTzsk='
            >>> b64encode(cry2.iv(sample_crc32_2))
            'NXs5UThCh9QQLETlcNsk5A=='

            >>> #
            >>> # Test encryption by keybook-based cryptographer
            >>> #
            >>> # e1 = cry1.encrypt(sample_text, sample_crc32_1)
            >>> # e1
            >>> # cry1.decrypt(e1, sample_crc32_1) == sample_text

            >>> # cry1.decrypt(e1, sample_crc32_1)

            >>> #
            >>> # Test encryption by password-based cryptographer
            >>> #
            >>> e2 = cry2.encrypt(sample_text, sample_crc32_1)
            >>> e2
            '\x98\xd2\xc5\x1e\xe6eWf\x9d08\xa2\xa9f\xeb\x14'
            >>> # cry2.decrypt(e2, sample_crc32_1) == sample_text
            True

            >>> cry2.decrypt(e2, sample_crc32_1)
            'alpha beta gamma'
            """,
    }

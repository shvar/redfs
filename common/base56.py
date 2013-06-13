#!/usr/bin/python
"""
Tools to encode/decode a number using base56 encoding.
"""

#
# Imports
#

from __future__ import absolute_import
import math
import numbers
import random
import sys

from contrib.dbc import contract_epydoc



#
# Constants
#

ALPHABET = '23456789abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ'
ALPHABET_LENGTH = len(ALPHABET)

DIGITS_IN_CODE = 5
MIN_DIGITS_IN_CODE = 5
MAX_DIGITS_IN_CODE = 8  # to change on overflow

CODE_RE = r'^[%s]{%d,%d}$' % (ALPHABET, MIN_DIGITS_IN_CODE, MAX_DIGITS_IN_CODE)



#
# Functions
#

def base56encode(num):
    """Returns C{num} as a base56-encoded string.

    # Using Base56 alphabet
    >>> base56encode(97380888)
    'bUwAs'
    >>> base56encode(
    ...     27161640924711133174641892492099373013022880705846240439317365927L)
    'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
    >>> base56encode(0)
    ''
    >>> base56encode(-1)
    ''

    @param num: integer code for magnet-link
    @type num: numbers.Integral

    @return: magnet url-code
    @rtype: basestring
    """
    encode = ''

    if num < 0:
        return ''

    while num >= ALPHABET_LENGTH:
        mod = num % ALPHABET_LENGTH
        encode = ALPHABET[mod] + encode
        num /= ALPHABET_LENGTH

    if num:
        encode = ALPHABET[num] + encode

    return encode


@contract_epydoc
def base56decode(s):
    """Decodes the base56-encoded string s into an integer

    # Using Base56 alphabet
    >>> base56decode('neJPG')
    198925310
    >>> base56decode('zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')
    27161640924711133174641892492099373013022880705846240439317365927L
    >>> base56decode('222222222222222222222222222222222222')
    0L
    >>> base56decode('1')
    Traceback (most recent call last):
      ...
    ValueError: substring not found

    @param s: input magnet url-code
    @type s: basestring

    @return: integer code for magnet-link
    @rtype: numbers.Integral
    """
    decoded = 0
    multi = 1
    s = s[::-1]
    for char in s:
        decoded += multi * ALPHABET.index(char)
        multi *= ALPHABET_LENGTH

    return decoded


@contract_epydoc
def _gen_magnet_code_int(version=0,
                         code_length=DIGITS_IN_CODE,
                         rng=random._inst):
    r"""
    Generate integer code for magnet-link to encode with base56.
    Will add two zero bytes if version is defined. Default version is 0.

    >>> rng=random.Random(42)
    >>> _gen_magnet_code_int(code_length=5, rng=rng)
    171644825L

    >>> _gen_magnet_code_int(code_length=5, rng=rng)
    29885207L

    >>> _gen_magnet_code_int(code_length=6, rng=rng)
    4402387665L

    >>> (_gen_magnet_code_int(version=1, code_length=5) /
    ...     (2 ** int(math.ceil(math.log(ALPHABET_LENGTH ** 5, 2)) - 2))) == 1
    True

    >>> (_gen_magnet_code_int(version=2, code_length=6) /
    ...     (2 ** int(math.ceil(math.log(ALPHABET_LENGTH ** 6, 2)) - 2))) == 2
    True

    >>> _gen_magnet_code_int(version=3)
    0

    @param version: version of algorithm. Default is 0.
    @type version: int
    @precondition: 0 <= version <= 2

    @type code_length: numbers.Integral
    @precondition: MIN_DIGITS_IN_CODE <= code_length <= MAX_DIGITS_IN_CODE

    @type rng: random._random.Random

    @return: integer code for magnet-link
    @rtype: numbers.Integral
    @postcondition: 0 <= result <= ALPHABET_LENGTH ** code_length
    """
    if not 0 <= version <= 2:
        return 0

    # The number of bits in the total code, which contain the variable
    # information. Eg, for 5-symbols long codes, it is 28
    max_value = ALPHABET_LENGTH ** code_length
    bits_count = int(math.ceil(math.log(ALPHABET_LENGTH ** code_length, 2))
                     - 2)
    result_val = max_value + 1
    while result_val > max_value:
        result_val = (2 ** bits_count) * version + rng.getrandbits(bits_count)
    return result_val


@contract_epydoc
def gen_magnet_code(version=0, code_length=DIGITS_IN_CODE):
    """Generate string code for magnet-link.

    Will add two zero bytes if version is defined. Default version is 0.

    @param version: version of magnet-code.
    @type version: int

    @param code_length: the length of the code.
    @type code_length: numbers.Integral

    @return: string-code for magnet-link.
    @rtype: basestring
    @postcondition: len(result) == code_length
    """
    code = base56encode(_gen_magnet_code_int(version=version,
                                             code_length=code_length))
    while len(code) < code_length:
        code = ALPHABET[0] + code
    return code



if __debug__:
    if 'nose' in sys.modules:
        import unittest

        class Base56Tests(unittest.TestCase):

            def test_alphabet_length(self):
                self.assertEqual(56, len(ALPHABET))

            def test_sample1(self):
                self.assertEqual('Tgmc', base56encode(8650162))
                self.assertEqual(8650162, base56decode('Tgmc'))

            def test_sample2(self):
                self.assertEqual('jS', base56encode(1000))
                self.assertEqual(1000, base56decode('jS'))

            def test_encode_bad_scenarios(self):
                # Zero returns empty string
                self.assertEqual('', base56encode(0))
                # Negative numbers too
                self.assertEqual('', base56encode(-100))

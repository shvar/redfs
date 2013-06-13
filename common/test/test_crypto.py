#!/usr/bin/python
"""Test C{common.crypto} module."""

import random, unittest
from itertools import imap

from common import crypto
from common.utils import random_string


SAMPLE_AES_KEY = 'abcdefghABCDEFGHabcdefghABCDEFGH'
SAMPLE_AES_IV = '1234567887654321'
SAMPLE_AES_PLAIN_TEXT = 'abcdefgh12345678'
SAMPLE_AES_ENCRYPTED_TEXT = '["\'\x97\x07\xb6\xb6\xfa\x1f\x8a\x05\xfb\x9dy7\n'


RND_SAMPLE_AES_KEY = ''.join(imap(chr, (random.randint(0, 255)
                                 for i in xrange(crypto.AES_KEY_SIZE))))


# Only if python-m2crypto is installed.

if crypto.m2crypto_Cipher is not None:

    def test_aes_encrypt_m2crypto():
        r = crypto._aes_encrypt_m2crypto(SAMPLE_AES_KEY,
                                         SAMPLE_AES_IV,
                                         SAMPLE_AES_PLAIN_TEXT)
        assert r == SAMPLE_AES_ENCRYPTED_TEXT, repr(r)


    def test_aes_decrypt_m2crypto():
        r = crypto._aes_decrypt_m2crypto(SAMPLE_AES_KEY,
                                         SAMPLE_AES_IV,
                                         SAMPLE_AES_ENCRYPTED_TEXT)
        assert r == SAMPLE_AES_PLAIN_TEXT, repr(r)


# Only if python-crypto is installed.

if crypto.pyCrypto_AES is not None:

    def test_aes_encrypt_pycrypto():
        r = crypto._aes_encrypt_pycrypto(SAMPLE_AES_KEY,
                                         SAMPLE_AES_IV,
                                         SAMPLE_AES_PLAIN_TEXT)
        assert r == SAMPLE_AES_ENCRYPTED_TEXT, repr(r)


    def test_aes_decrypt_pycrypto():
        r = crypto._aes_decrypt_pycrypto(SAMPLE_AES_KEY,
                                         SAMPLE_AES_IV,
                                         SAMPLE_AES_ENCRYPTED_TEXT)
        assert r == SAMPLE_AES_PLAIN_TEXT, repr(r)


# But one of python-crypto and python-m2crypto MUST be installed.

def test_aes_encrypt():
    r = crypto._aes_encrypt(SAMPLE_AES_KEY,
                            SAMPLE_AES_IV,
                            SAMPLE_AES_PLAIN_TEXT)
    assert r == SAMPLE_AES_ENCRYPTED_TEXT, repr(r)


def test_aes_decrypt():
    r = crypto._aes_decrypt(SAMPLE_AES_KEY,
                            SAMPLE_AES_IV,
                            SAMPLE_AES_ENCRYPTED_TEXT)
    assert r == SAMPLE_AES_PLAIN_TEXT, repr(r)



#
# Classes
#

class TestModule(unittest.TestCase):

    __AES_METHODS = {}

    if crypto.m2crypto_Cipher is not None:
        __AES_METHODS['m2crypto'] = (crypto.m2crypto_Cipher,
                                     crypto._aes_encrypt_m2crypto,
                                     crypto._aes_decrypt_m2crypto)
    if crypto.pyCrypto_AES is not None:
        __AES_METHODS['pcrypto'] = (crypto.pyCrypto_AES,
                                    crypto._aes_encrypt_pycrypto,
                                    crypto._aes_decrypt_pycrypto)


    def setUp(self):
        self.longMessage = True


    def test_aes_encrypt_decrypt(self):
        cls = self.__class__

        key = random_string(crypto.AES_KEY_SIZE)
        iv = random_string(crypto.AES_IV_SIZE)
        plain_text = random_string(crypto.AES_BLOCK_SIZE
                                   * random.randint(0, 1023))

        # Encrypt the same plain text using all the available methods,
        # and ensure all the results match.
        results = {name: encrypt(key, iv, plain_text)
                       for name, (avail, encrypt, decrypt)
                           in cls.__AES_METHODS.iteritems()}

        encrypted_text = results.values()[0]
        self.assertEqual(len(encrypted_text),
                         len(plain_text))
        for k, v in results.iteritems():
            self.assertEqual(v, encrypted_text, msg = k)

        # Now decrypt the same encrypted text using all the available methods,
        # and ensure all the results match to the original plain text.
        results = {name: decrypt(key, iv, encrypted_text)
                       for name, (avail, encrypt, decrypt)
                           in cls.__AES_METHODS.iteritems()}
        for k, v in results.iteritems():
            self.assertEqual(v, plain_text, msg = k)

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Various simple datatypes used throughout the system.

This module is designed to be as high in the subordination pyramid as possible,
thus the types defined here should NOT contain any significant implementation
dependent on the other implementation modules (except probably
the serializing/deserializing code);  instead, any other modules
will import this module.

@todo: Move types like Chunk, etc to this module.
"""

#
# Imports
#

from __future__ import absolute_import

import collections as col
from datetime import datetime, timedelta
from functools import partial
from types import NoneType
from uuid import UUID

import logging

from contrib.dbc import contract_epydoc

from .abstractions import IJSONable, IPrintable
from .chunks import ChunkInfo
from .utils import (
    antisymmetric_comparison, coalesce, dt_to_ts, ts_to_dt, td_to_sec,
    sec_to_td, get_wider_locales, gen_uuid)


logger = logging.getLogger(__name__)

#
# Types and classes
#

AutoUpdateSettings = col.namedtuple('AutoUpdateSettings', ['update'])

TrustedHostCaps = col.namedtuple('TrustedHostCaps',
                                 ('storage', 'restore'))
"""
The capabilities of a Trusted Host.
"""



class AuthToken(IPrintable, IJSONable):
    """Authentication token for the password-less logging to the node."""
    __slots__ = ('ts', 'uuid')


    @contract_epydoc
    def __init__(self, ts, uuid, *args, **kwargs):
        r"""Constructor.

        >>> AuthToken(ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2')
        ... )  # doctest:+NORMALIZE_WHITESPACE
        AuthToken(ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
                  uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'))

        @type ts: datetime
        @type uuid: UUID
        """
        super(AuthToken, self).__init__(*args, **kwargs)
        self.ts = ts
        self.uuid = uuid


    def __str__(self):
        return u'ts={self.ts!r}, uuid={self.uuid!r}'.format(self=self)


    def to_json(self):
        r"""
        >>> AuthToken(ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...           uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2')
        ... ).to_json()  # doctest:+NORMALIZE_WHITESPACE
        {'ts': 1348242302.478074, 'uuid': '030f417bc1dd41d1837c847e7ab886a2'}
        """
        return {'ts': dt_to_ts(self.ts),
                'uuid': self.uuid.hex}


    @classmethod
    def from_json(cls, json_struct):
        r"""
        >>> AuthToken.from_json({'ts': 1348242302.478074,
        ...                      'uuid': '030f417bc1dd41d1837c847e7ab886a2'}
        ... )()  # doctest:+NORMALIZE_WHITESPACE
        AuthToken(ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
                  uuid=UUID('030f417b-c1dd-41d1-837c-847e7ab886a2'))

        """
        return partial(super(AuthToken, cls).from_json(json_struct),
                       ts=ts_to_dt(json_struct['ts']),
                       uuid=UUID(json_struct['uuid']))


    @classmethod
    def create(cls, uuid=None, ts=None):
        return cls(uuid=gen_uuid() if uuid is None else uuid,
                   ts=datetime.utcnow() if ts is None else ts)



class ProgressNotificationPerHost(IJSONable):
    """
    Host-specific information about progress notification (i.e. about the fact
    that some chunks were uploaded to a single host).
    """

    __slots__ = ('chunks', 'end_ts', 'duration')


    @contract_epydoc
    def __init__(self, chunks, end_ts=None, duration=None, *args, **kwargs):
        """Constructor.

        >>> # Minimal
        >>> ProgressNotificationPerHost(chunks=[])
        ProgressNotificationPerHost.from_json(chunks=[])

        >>> # Maximal
        >>> from common.typed_uuids import ChunkUUID
        >>> chunks = [
        ...     ChunkInfo(crc32=0x07FD7A5B,
        ...               uuid=ChunkUUID('5b237ceb300d4c88b4c06331cb14b5b4'),
        ...               maxsize_code=1,
        ...               hash='abcdefgh' * 8,
        ...               size=2097152),
        ...     ChunkInfo(crc32=0x7E5CE7AD,
        ...               uuid=ChunkUUID('940f071152d742fbbf4c818580f432dc'),
        ...               maxsize_code=0,
        ...               hash='01234567' * 8,
        ...               size=143941),
        ...     ChunkInfo(crc32=0xDCC847D8,
        ...               uuid=ChunkUUID('a5b605f26ea549f38658d217b7e8e784'),
        ...               maxsize_code=1,
        ...               hash='76543210' * 8,
        ...               size=2097152)
        ... ]

        >>> ProgressNotificationPerHost(
        ...     chunks=chunks,
        ...     end_ts=datetime(2012, 11, 5, 12, 12, 42, 8433),
        ...     duration=timedelta(5, 3, 29291)
        ... )  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ProgressNotificationPerHost.from_json(chunks=[{'crc32': 134052443,
                 'maxsize_code': 1,
                 'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo...Z2hhYmNkZWZnaA==',
                 'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
                 'size': 2097152},
                {'crc32': 2120017837,
                 'maxsize_code': 0,
                 'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3...NjcwMTIzNDU2Nw==',
                 'uuid': '940f071152d742fbbf4c818580f432dc',
                 'size': 143941},
                {'crc32': 3704113112,
                 'maxsize_code': 1,
                 'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw...MTA3NjU0MzIxMA==',
                 'uuid': 'a5b605f26ea549f38658d217b7e8e784',
                 'size': 2097152}],
            duration=432003.029291,
            end_ts=1352117562.008433)

        @param chunks: an iterable over Chunk objects.

        @param end_ts: the time of upload completion.
            May be C{None} if doesn't matter.
        @type end_ts: datetime, NoneType

        @param end_ts: the upload duration.
            May be C{None} if doesn't matter.
        @time duration: timedelta, NoneType
        """
        super(ProgressNotificationPerHost, self).__init__(*args, **kwargs)
        self.chunks = chunks
        self.end_ts = end_ts
        self.duration = duration


    def to_json(self):
        r"""Implement C{IJSONable} interface.

        >>> # Minimal
        >>> ProgressNotificationPerHost(chunks=[]).to_json()
        {'chunks': []}

        >>> # Maximal
        >>> from common.typed_uuids import ChunkUUID
        >>> chunks = [
        ...     ChunkInfo(crc32=0x07FD7A5B,
        ...               uuid=ChunkUUID('5b237ceb300d4c88b4c06331cb14b5b4'),
        ...               maxsize_code=1,
        ...               hash='abcdefgh' * 8,
        ...               size=2097152),
        ...     ChunkInfo(crc32=0x7E5CE7AD,
        ...               uuid=ChunkUUID('940f071152d742fbbf4c818580f432dc'),
        ...               maxsize_code=0,
        ...               hash='01234567' * 8,
        ...               size=143941),
        ...     ChunkInfo(crc32=0xDCC847D8,
        ...               uuid=ChunkUUID('a5b605f26ea549f38658d217b7e8e784'),
        ...               maxsize_code=1,
        ...               hash='76543210' * 8,
        ...               size=2097152)
        ... ]

        >>> ProgressNotificationPerHost(
        ...     chunks=chunks,
        ...     end_ts=datetime(2012, 11, 5, 12, 12, 42, 8433),
        ...     duration=timedelta(5, 3, 29291)
        ... ).to_json()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        {'chunks':
             [{'crc32': 134052443, 'maxsize_code': 1,
               'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdoYWJ...2hhYmNkZWZnaA==',
               'uuid': '5b237ceb300d4c88b4c06331cb14b5b4', 'size': 2097152},
              {'crc32': 2120017837, 'maxsize_code': 0,
               'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3MDE...jcwMTIzNDU2Nw==',
               'uuid': '940f071152d742fbbf4c818580f432dc', 'size': 143941},
              {'crc32': 3704113112, 'maxsize_code': 1,
               'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEwNzY...TA3NjU0MzIxMA==',
               'uuid': 'a5b605f26ea549f38658d217b7e8e784', 'size': 2097152}],
         'duration': 432003.029291,
         'end_ts': 1352117562.008433}
        """
        # Mandatory
        result = {'chunks': [c.to_json() for c in self.chunks]}

        # Optional
        if self.end_ts is not None:
            result['end_ts'] = dt_to_ts(self.end_ts)
        if self.duration is not None:
            result['duration'] = td_to_sec(self.duration)
        return result


    @classmethod
    def from_json(cls, data):
        r"""Implement C{IJSONable} interface.

        >>> # Minimal
        >>> ProgressNotificationPerHost.from_json({
        ...     'chunks': []
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ProgressNotificationPerHost.from_json(chunks=[])

        >>> # Maximal
        >>> ProgressNotificationPerHost.from_json({
        ...     'chunks': [
        ...         {'crc32': 134052443,
        ...          'maxsize_code': 1,
        ...          'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                  'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo'
        ...                  'YWJjZGVmZ2hhYmNkZWZnaA==',
        ...          'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
        ...          'size': 2097152},
        ...         {'crc32': 2120017837,
        ...          'maxsize_code': 0,
        ...          'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                  'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3'
        ...                  'MDEyMzQ1NjcwMTIzNDU2Nw==',
        ...          'uuid': '940f071152d742fbbf4c818580f432dc',
        ...          'size': 143941},
        ...         {'crc32': 3704113112,
        ...          'maxsize_code': 1,
        ...          'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                  'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw'
        ...                  'NzY1NDMyMTA3NjU0MzIxMA==',
        ...          'uuid': 'a5b605f26ea549f38658d217b7e8e784',
        ...          'size': 2097152}
        ...     ],
        ...     'duration': 432003.029291,
        ...     'end_ts': 1352117562.008433
        ... })()  # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
        ProgressNotificationPerHost.from_json(chunks=[{'crc32': 134052443,
                 'maxsize_code': 1,
                 'hash': 'YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdo...Z2hhYmNkZWZnaA==',
                 'uuid': '5b237ceb300d4c88b4c06331cb14b5b4',
                 'size': 2097152},
                {'crc32': 2120017837,
                 'maxsize_code': 0,
                 'hash': 'MDEyMzQ1NjcwMTIzNDU2NzAxMjM0NTY3...NjcwMTIzNDU2Nw==',
                 'uuid': '940f071152d742fbbf4c818580f432dc',
                 'size': 143941},
                {'crc32': 3704113112,
                 'maxsize_code': 1,
                 'hash': 'NzY1NDMyMTA3NjU0MzIxMDc2NTQzMjEw...MTA3NjU0MzIxMA==',
                 'uuid': 'a5b605f26ea549f38658d217b7e8e784',
                 'size': 2097152}],
            duration=432003.029291,
            end_ts=1352117562.008433)
        """
        return partial(super(ProgressNotificationPerHost, cls).from_json(data),
                       chunks=[ChunkInfo.from_json(c)()
                                   for c in data['chunks']],
                       end_ts=ts_to_dt(data['end_ts'])
                                  if 'end_ts' in data
                                  else None,
                       duration=sec_to_td(data['duration'])
                                    if 'duration' in data
                                    else None)



class ProgressNotificationPerHostWithDeferred(ProgressNotificationPerHost):
    """
    Host-specific information about progress notification (containing
    a C{.deferred} field for IoC).
    """

    __slots__ = ('deferred',)


    def __init__(self, deferred, *args, **kwargs):
        super(ProgressNotificationPerHostWithDeferred, self).__init__(*args,
                                                                      **kwargs)
        self.deferred = deferred



LoginPasswordTuple = \
    col.namedtuple('LoginPasswordTuple', ['login', 'password'])


@antisymmetric_comparison
class SMTPLogReportingSettings(IJSONable):
    """
    Can be compared with another similar class for equality/nonequality,
    and verified for boolean True-ness.

    Each default value affect three important places:
    1. __init__ arguments,
    2. __nonzero__ verification,
    3. to_json/from_json encoding must convert {} into
       False-evaluated object and vice versa.
    """
    __slots__ = ('recipient', 'server', 'starttls', 'loginpassword')


    @contract_epydoc
    def __init__(self,
                 recipient='', server='', starttls=False, loginpassword=None):
        """
        @type server: basestring
        @type starttls: bool
        @param loginpassword: A tuple of login and password, or None
                              if the login-based authentication
                              should not be used.
        @type loginpassword: LoginPasswordTuple, NoneType
        """
        self.recipient = recipient
        self.server = server
        self.starttls = starttls
        self.loginpassword = loginpassword


    def __eq__(self, other):
        return (isinstance(other, SMTPLogReportingSettings) and
                self.recipient == other.recipient and
                self.server == other.server and
                self.starttls == other.starttls and
                self.loginpassword == other.loginpassword)


    def __nonzero__(self):
        """
        Is True in boolean context if any of the fields
        changed from the default values.
        """
        return (self.recipient != '' or
                self.server != '' or
                self.starttls != False or
                self.loginpassword is not None)


    def to_json(self):
        result = {}
        if self.recipient:
            result['recipient'] = self.recipient
        if self.server:
            result['server'] = self.server
        if self.starttls:
            result['starttls'] = 1
        # Empty passwords are not permitted, I suppose.
        if (self.loginpassword and
            self.loginpassword.login and
            self.loginpassword.password):
            result['login'], result['password'] = self.loginpassword

        return result


    @classmethod
    def from_json(cls, json_struct):
        if 'login' in json_struct or 'password' in json_struct:
            lp = LoginPasswordTuple(json_struct.get('login', ''),
                                    json_struct.get('password', ''))
        else:
            lp = None

        return partial(cls,
                       recipient=json_struct.get('recipient', ''),
                       server=json_struct.get('server', ''),
                       starttls=bool(json_struct.get('starttls', False)),
                       loginpassword=lp)



@antisymmetric_comparison
class LogReportingSettings(IJSONable):
    """
    Can be compared with another similar class for equality/nonequality,
    and verified for boolean True-ness.

    Each default value affect three important places:
    1. __init__ arguments,
    2. __nonzero__ verification,
    3. to_json/from_json encoding must convert {} into
       False-evaluated object and vice versa.
    """
    __slots__ = ('method', 'smtp_settings')


    class Methods(object):
        """The namespace for supported log reporting methods."""

        _all = frozenset(('internal', 'smtp'))
        _default = 'internal'


        @classmethod
        def or_default(cls, method):
            """
            Given a what assumed to be a method, return itself,
            if it is among the allowed methods, or the default method.
            """
            return method if method in cls._all \
                          else cls._default


    @contract_epydoc
    def __init__(self, method=Methods._default, smtp_settings=None):
        self.method = method
        self.smtp_settings = coalesce(smtp_settings,
                                      SMTPLogReportingSettings())


    def __eq__(self, other):
        return (isinstance(other, LogReportingSettings) and
                self.method == other.method and
                self.smtp_settings == other.smtp_settings)


    def __nonzero__(self):
        """
        Is True in boolean context if any of the fields
        changed from the default values.
        """
        return (self.method != LogReportingSettings.Methods._default or
                bool(self.smtp_settings))


    def to_json(self):
        result = {'method': self.method}
        if self.smtp_settings:
            result['smtp_settings'] = self.smtp_settings.to_json()
        return result


    @classmethod
    @contract_epydoc
    def from_json(cls, json_struct):
        """
        @type json_struct: dict
        """
        if 'smtp_settings' in json_struct:
            smtp_settings = SMTPLogReportingSettings \
                                .from_json(json_struct['smtp_settings'])()
        else:
            smtp_settings = None

        return partial(cls,
                       method=json_struct.get('method',
                                              LogReportingSettings.Methods
                                                                  ._default),
                       smtp_settings=smtp_settings)



class LocalizedUserMessage(IPrintable, IJSONable):
    """
    A message from the system to the particular user,
    converted to the particular language.
    """
    __slots__ = ('username', 'key', 'ts', 'body', 'locale')


    def __init__(self, username, key, ts, body, locale):
        """Constructor.

        @type username: basestring
        @type key: basestring
        @type ts: datetime
        @type body: basestring
        @type locale: basestring
        """
        self.username = username
        self.key = key
        self.ts = ts
        self.body = body
        self.locale = locale


    def __str__(self):
        return u'username={self.username!r}, key={self.key!r}, ' \
                'ts={self.ts!r}, body={self.body!r}, ' \
                'locale={self.locale!r}'.format(self=self)


    def to_json(self):
        r"""
        >>> UserMessage(
        ...     username='johndoe', key='abcde',
        ...     ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...     body={'EN_US': 'You are banned, dude!',
        ...           'en_GB': 'You are banned, bloke!',
        ...           'C': 'banned'}).to_json() # doctest:+NORMALIZE_WHITESPACE
         {'username': 'johndoe',
          'body': {'c': 'banned',
                   'en_us': 'You are banned, dude!',
                   'en_gb': 'You are banned, bloke!'},
          'ts': 1348242302.478074,
          'key': 'abcde'}
        """
        return {'username': self.username,
                'key': self.key,
                'ts': dt_to_ts(self.ts),
                'body': self.body,
                'locale': self.locale}


    @classmethod
    def from_json(cls, json_struct):
        r"""
        >>> UserMessage.from_json({
        ...     'username': 'johndoe',
        ...     'body': {'c': 'banned',
        ...              'en_us': 'You are banned, dude!',
        ...              'en_gb': 'You are banned, bloke!'},
        ...     'ts': 1348242302.478074,
        ...     'key': 'abcde'
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        UserMessage(username='johndoe',
                    key='abcde',
                    ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
                    body={'c': 'banned',
                          'en_us': 'You are banned, dude!',
                          'en_gb': 'You are banned, bloke!'})
        """
        return partial(super(LocalizedUserMessage, cls).from_json(json_struct),
                       username=json_struct['username'],
                       key=json_struct['key'],
                       ts=ts_to_dt(json_struct['ts']),
                       body=json_struct['body'],
                       locale=json_struct['locale'])



class UserMessage(IPrintable, IJSONable):
    """A message from the system to the particular user."""
    __slots__ = ('username', 'key', 'ts', 'body')


    @contract_epydoc
    def __init__(self, username, key, ts, body,
                 *args, **kwargs):
        """Constructor.

        >>> UserMessage(
        ...     username='johndoe', key='abcde',
        ...     ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...     body={'EN_US': 'You are banned, dude!',
        ...           'en_US': 'You are banned, guy!'}
        ... ) # doctest:+NORMALIZE_WHITESPACE
        Traceback (most recent call last):
          ...
        AssertionError: {'EN_US': 'You are banned, dude!',
                         'en_US': 'You are banned, guy!'}

        @type username: basestring
        @type key: basestring
        @type ts: datetime
        @param body: a mapping from locale to the message translation
            (for that locale).
            Do NOT ever mix the cases of the same locale!
            The behaviour is not defined.
        @type body: col.Mapping
        """
        super(UserMessage, self).__init__(*args, **kwargs)
        self.username = username
        self.key = key
        self.ts = ts
        assert (len(set(body.iterkeys())) ==
                len({lang.lower() for lang in body.iterkeys()})), \
               repr(body)
        self.body = {k.lower(): v for k, v in body.iteritems()}


    def __str__(self):
        return u'username={self.username!r}, key={self.key!r}, ' \
                'ts={self.ts!r}, body={self.body!r}'.format(self=self)


    def to_json(self):
        r"""
        >>> UserMessage(
        ...     username='johndoe', key='abcde',
        ...     ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...     body={'EN_US': 'You are banned, dude!',
        ...           'en_GB': 'You are banned, bloke!',
        ...           'en': 'You are banned!',
        ...           'C': 'banned'}).to_json() # doctest:+NORMALIZE_WHITESPACE
        {'username': 'johndoe',
         'body': {'c': 'banned',
                  'en': 'You are banned!',
                  'en_us': 'You are banned, dude!',
                  'en_gb': 'You are banned, bloke!'},
         'ts': 1348242302.478074,
         'key': 'abcde'}
        """
        return {'username': self.username,
                'key': self.key,
                'ts': dt_to_ts(self.ts),
                'body': self.body}


    @classmethod
    def from_json(cls, json_struct):
        r"""
        >>> UserMessage.from_json({
        ...     'username': 'johndoe',
        ...     'body': {'C': 'banned',
        ...              'en': 'You are banned!',
        ...              'EN_US': 'You are banned, dude!',
        ...              'en_GB': 'You are banned, bloke!'},
        ...     'ts': 1348242302.478074,
        ...     'key': 'abcde'
        ... })()  # doctest:+NORMALIZE_WHITESPACE
        UserMessage(username='johndoe',
                    key='abcde',
                    ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
                    body={'c': 'banned',
                          'en': 'You are banned!',
                          'en_us': 'You are banned, dude!',
                          'en_gb': 'You are banned, bloke!'})
        """
        return partial(super(UserMessage, cls).from_json(json_struct),
                       username=json_struct['username'],
                       key=json_struct['key'],
                       ts=ts_to_dt(json_struct['ts']),
                       body=json_struct['body'])


    @contract_epydoc
    def _get_translated_body(self, locale, strict=True):
        """Translate a message body to the particular locale.

        >>> m = UserMessage(username='johndoe', key='abcde',
        ...                 ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...                 body={'EN_US': 'You are banned, dude!',
        ...                       'en_GB': 'You are banned, bloke!',
        ...                       'C': 'banned'})

        >>> # IS IT OK???
        >>> m._get_translated_body(
        ...     'En', strict=True)  # doctest:+NORMALIZE_WHITESPACE
        (None, None)

        >>> m._get_translated_body(
        ...     'EN', strict=False)  # doctest:+NORMALIZE_WHITESPACE
        ('c', 'banned')

        >>> m._get_translated_body(
        ...     'EN_gb', strict=False) # doctest:+NORMALIZE_WHITESPACE
        ('en_gb', 'You are banned, bloke!')

        @param locale: the name of locale to which we want to translate
            a message.
        @param strict: whether we want to use a precisely matching locale.

        @return: a tuple of actual locale that was found, and the proper
            translation. Both of them may be None if proper translation
            was not found.
        @rtype: tuple
        """
        locale = locale.lower()

        result = (None, None)

        # For non-strict, ['en_us', 'en', 'c'] - order is important,
        # sort from longest to shortest language name.
        locales = [locale] if strict \
                           else get_wider_locales(locale)

        for try_locale in locales:
            try:
                result = (try_locale, self.body[try_locale])
            except KeyError:
                continue
            else:
                break
        else:
            logger.error('The message lacks "c" localization: %s', self)

        return result


    @contract_epydoc
    def translate(self, locale, strict=True):
        """Translate a multi-locale message to a localized one.

        >>> m = UserMessage(username='johndoe', key='abcde',
        ...                 ts=datetime(2012, 9, 21, 15, 45, 2, 478074),
        ...                 body={'EN_US': 'You are banned, dude!',
        ...                       'en_GB': 'You are banned, bloke!',
        ...                       'C': 'banned'})

        >>> # IS IT OK???
        >>> m.translate('En', strict=True)  # doctest:+NORMALIZE_WHITESPACE
        LocalizedUserMessage(username='johndoe',
            key='abcde',
            ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
            body=None, locale=None)

        >>> m.translate('EN', strict=False)  # doctest:+NORMALIZE_WHITESPACE
        LocalizedUserMessage(username='johndoe',
            key='abcde',
            ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
            body='banned', locale='c')

        >>> m.translate('EN_gb', strict=False)  # doctest:+NORMALIZE_WHITESPACE
        LocalizedUserMessage(username='johndoe',
            key='abcde',
            ts=datetime.datetime(2012, 9, 21, 15, 45, 2, 478074),
            body='You are banned, bloke!', locale='en_gb')

        @rtype: LocalizedUserMessage
        """
        locale, body = self._get_translated_body(locale, strict)
        return LocalizedUserMessage(username=self.username, key=self.key,
                                    ts=self.ts, body=body, locale=locale)

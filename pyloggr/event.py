# encoding: utf-8
"""
The pyloggr.event module mainly provides the Event class.

Event provides an abstraction of a syslog event.
"""

from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import logging
from base64 import b64encode, b64decode, urlsafe_b64encode

import ujson
from arrow import Arrow
import arrow
import arrow.parser
import dateutil.parser
from datetime import datetime
from functools import total_ordering
from io import open

from future.utils import python_2_unicode_compatible, raise_from, viewvalues, viewitems
from future.builtins import str as past_unicode
from future.builtins import bytes as realbytes
# noinspection PyPackageRequirements,PyCompatibility
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import hmac as hmac_func
from cryptography.exceptions import InvalidSignature
from psycopg2.extras import Json
from spooky_hash import Hash128
from lockfile import LockFile, LockFailed
import msgpack

from pyloggr.utils.structured_data import StructuredData
from pyloggr.utils import to_unicode, sanitize_key, sanitize_tag, to_bytes
from pyloggr.utils.constants import RE_MSG_W_TRUSTED, TRUSTED_FIELDS_MAP, REGEXP_SYSLOG, REGEXP_START_SYSLOG
from pyloggr.utils.constants import RE_TRUSTED_FIELDS, REGEXP_START_SYSLOG23, FACILITY, SEVERITY, SQL_VALUES_STR
from pyloggr.utils.constants import EVENT_STR_FMT, REGEXP_SYSLOG23, FACILITY_TO_INT, SEVERITY_TO_INT
from pyloggr.utils.constants import PYLOGGR_SDID

logger = logging.getLogger(__name__)


class ParsingError(ValueError):
    """
    Triggered when a string can't be parsed into an :py:class:`Event`
    """
    # noinspection PyUnusedLocal
    def __init__(self, *args, **kwargs):
        self.json = kwargs['json'] if 'json' in kwargs else False


@python_2_unicode_compatible
@total_ordering
class Event(object):
    """
    Represents a syslog event, with optional tags, custom fields and structured data

    Attributes
    ----------
    procid: int
    severity: str
    facility: str
    app_name: str
    source: str
    programname: str
    syslogtag: str
    message: str
    uuid: str
    hmac: str
    timereported: Datetime
    timegenerated: Datetime
    timehmac: Datetime
    custom_fields: dictionnary of custom fields
    structured_data: dictionnary representing syslog structured data
    tags: set of str

    """

    HMAC_KEY = None
    __slots__ = (
        'procid', '_severity', '_facility', '_app_name', '_source', 'programname', 'syslogtag',
        '_message', '_uuid', '_hmac', '_timereported', '_timegenerated', '_timehmac', 'iut',
        'structured_data', 'relp_id', 'have_been_published', '_dirty', 'override_exchanges',
        'override_event_type'
    )

    @classmethod
    def set_hmac_key(cls, hmac_key):
        # noinspection PyUnresolvedReferences
        cls.HMAC_KEY = to_bytes(hmac_key)

    @staticmethod
    def make_severity(severity):
        """
        Return a normalized severity value

        :param severity: syslog priority (integer) or string
        :type severity: int or str or unicode
        """
        try:
            # from encoded priority
            return SEVERITY.get(int(severity) & 7, u'')
        except (ValueError, TypeError):
            pass
        if not severity:
            return u'notice'
        return SEVERITY.get(to_unicode(severity).lower(), u'notice')

    @staticmethod
    def make_facility(facility):
        """
        Return a normalized facility value

        :param facility: syslog facility (integer) or string
        :type facility: int or str or unicode
        """
        try:
            # from encoded priority
            return FACILITY.get(int(facility) >> 3, u'')
        except ValueError:
            pass
        if not facility:
            return u'user'
        return FACILITY.get(to_unicode(facility).lower(), u'user')

    @staticmethod
    def make_arrow_datetime(dt):
        """
        Parse a date-time value and return the corresponding Arrow object

        :param dt: date-time
        :type dt: Arrow or datetime or str
        :return: Arrow object
        """
        if dt is None:
            return None
        if isinstance(dt, Arrow):
            return dt
        if isinstance(dt, datetime):
            return Arrow.fromdatetime(dt).to('utc')

        # sometimes microseconds are delimited with a comma
        dt = dt.replace(',', '.')
        try:
            # ISO format
            return arrow.get(dt)
        except arrow.parser.ParserError:
            try:
                return arrow.get(dt, "YYMMDD HH:mm:ss")
            except arrow.parser.ParserError:
                try:
                    # fallback to dateutil parser
                    return Arrow.fromdatetime(dateutil.parser.parse(dt)).to('utc')
                except ValueError:
                    return None

    # noinspection PyUnusedLocal
    def __init__(
            self, procid=u'-', severity=u'', facility=u'', app_name=u'', source=u'', programname=u'',
            syslogtag=u'', message=u'', uuid=None, hmac=None, timereported=None, timegenerated=None,
            timehmac=None, custom_fields=None, structured_data=None, tags=None, iut=1, **kwargs
    ):

        try:
            self.procid = int(procid)
        except (ValueError, TypeError):
            self.procid = None

        self._severity = self.make_severity(severity)
        self._facility = self.make_facility(facility)

        self._app_name = to_unicode(app_name)
        self._source = to_unicode(source)
        self.programname = to_unicode(programname) if programname else self._app_name
        if not syslogtag:
            if self.procid is not None and self._app_name:
                self.syslogtag = u'{}[{}]'.format(self._app_name, self.procid)
            else:
                self.syslogtag = self._app_name
        else:
            self.syslogtag = to_unicode(syslogtag)
        self._message = to_unicode(message.strip('\r\n '))
        self.iut = iut
        self._timegenerated = self.make_arrow_datetime(timegenerated) if timegenerated else Arrow.utcnow()
        self._timereported = self.make_arrow_datetime(timereported) if timereported else self._timegenerated
        self.structured_data = StructuredData(structured_data)
        self.add_tags(tags)
        self.custom_fields.update(custom_fields)

        if hmac:
            self._set_hmac(hmac)
        elif self.structured_data[PYLOGGR_SDID]['hmac']:
            self._set_hmac(iter(self.structured_data[PYLOGGR_SDID]['hmac']).next())

        self._timehmac = None
        if timehmac:
            self._set_timehmac(timehmac)
        elif self.structured_data[PYLOGGR_SDID]['timehmac']:
            self._set_timehmac(iter(self.structured_data[PYLOGGR_SDID]['timehmac']).next())

        self._parse_trusted()
        self._dirty = False
        if uuid:
            self._set_uuid(uuid)
        elif self.structured_data[PYLOGGR_SDID]['uuid']:
            self._set_uuid(iter(self.structured_data[PYLOGGR_SDID]['uuid']).next())
        else:
            self.generate_uuid()
        self.relp_id = None
        self.override_exchanges = []
        self.override_event_type = None

    @property
    def message(self):
        """
        Event message
        """
        return self._message

    @message.setter
    def message(self, new_msg):
        """
        Set event message

        :param new_msg: new message value
        :type new_msg: str
        """
        new_msg = to_unicode(new_msg)
        if new_msg != self._message:
            self._message = new_msg
            self._dirty = True

    @property
    def source(self):
        """
        Event source hostname
        """
        return self._source

    @source.setter
    def source(self, new_source):
        """
        Set event source

        :param new_source: new source value
        :type new_source: str
        """
        new_source = to_unicode(new_source)
        if new_source != self._source:
            self._source = new_source
            self._dirty = True

    @property
    def app_name(self):
        """
        Name of application that generated the event
        """
        return self._app_name

    @app_name.setter
    def app_name(self, new_app_name):
        """
        Set event application name

        :param new_app_name: new application name value
        :type new_app_name: str
        """
        new_app_name = to_unicode(new_app_name)
        if new_app_name != self._app_name:
            self._app_name = new_app_name
            self._dirty = True

    @property
    def facility(self):
        """
        Event facility
        """
        return self._facility

    @facility.setter
    def facility(self, new_facility):
        """
        Set event facility

        :param new_facility: new facility
        :type new_facility: str
        """
        new_f = self.make_facility(new_facility)
        if self._facility != new_f:
            self._facility = new_f
            self._dirty = True

    @property
    def severity(self):
        """
        Event severity
        """
        return self._severity

    @severity.setter
    def severity(self, new_severity):
        """
        Set event severity

        :param new_severity: new severity
        :type new_severity: str
        """
        new_s = self.make_facility(new_severity)
        if self._severity != new_s:
            self._severity = new_s
            self._dirty = True

    @property
    def timegenerated(self):
        """
        event "first seen" datetime
        """
        return self._timegenerated.datetime

    @timegenerated.setter
    def timegenerated(self, new_time):
        """
        Set timegenerated datetime

        :param new_time: new datetime
        :type new_time: Arrow, datetime or str
        """
        self._timegenerated = self.make_arrow_datetime(new_time)

    @property
    def timereported(self):
        """
        event creation datetime
        """
        return self._timereported.datetime

    @timereported.setter
    def timereported(self, new_time):
        """
        Set timereported datetime

        :param new_time: new datetime
        :type new_time: Arrow, datetime or str
        """
        new_t = self.make_arrow_datetime(new_time)
        if new_t != self._timereported:
            self._timereported = new_t
            self._dirty = True

    @property
    def timehmac(self):
        """
        datetime, when the event HMAC was created
        """
        return None if self._timehmac is None else self._timehmac.datetime

    def _set_timehmac(self, new_time):
        new_t = self.make_arrow_datetime(new_time)
        if new_t != self._timehmac:
            self._timehmac = new_t
            self.structured_data[PYLOGGR_SDID]['timehmac'] = [str(self._timehmac)]

    def generate_uuid(self, new_uuid=None):
        """
        Generate a UUID for the current event

        :param new_uuid: if given, sets the UUID to new_uuid. if not given generate a UUID.

        :return: new UUID
        :rtype: str
        """
        if new_uuid:
            self._set_uuid(new_uuid)
            return
        digest = Hash128()
        # Hash128 doesn't accept unicode
        digest.update(self.severity.encode("utf-8"))
        digest.update(self.facility.encode("utf-8"))
        digest.update(self.app_name.encode("utf-8"))
        digest.update(self.source.encode("utf-8"))
        digest.update(self.message.encode("utf-8"))
        digest.update(str(self._timereported.to('utc')))
        uuid = urlsafe_b64encode(digest.digest())
        self._set_uuid(uuid)
        self._dirty = False
        return self._uuid

    def update_uuid_and_hmac(self):
        """
        If event is dirty (core fields have been modified), generate UUID and HMAC
        """
        if self._dirty:
            self.generate_uuid()
            if self._hmac:
                self.generate_hmac(verify_if_exists=False)

    @property
    def uuid(self):
        """
        Return the event UUID. If event is dirty, generate a new UUID and return it.
        """
        self.update_uuid_and_hmac()
        return self._uuid

    @property
    def uid(self):
        return self.uuid

    def _set_uuid(self, new_uuid):
        new_uuid = to_unicode(new_uuid)
        self._uuid = new_uuid
        self.custom_fields['uuid'] = [new_uuid]

    @property
    def priority(self):
        """
        Return the event computed syslog priority
        """
        return 8 * FACILITY_TO_INT.get(self.facility, 1) + SEVERITY_TO_INT.get(self.severity)

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        """
        Two events are equal if they have the same UUID

        :type other: Event
        :rtype: bool
        """
        if self._uuid == other._uuid:
            return self.message == other.message and self._timereported == other._timereported and \
                   self.severity == other.severity and self.facility == other.facility and \
                   self.app_name == other.app_name and self.source == other.source
        return False

    def __lt__(self, other):
        """
        self < other if self.timereported < other.timereported

        :type other: Event
        :rtype: bool
        """
        if self == other:
            return False
        return self._timereported < other._timereported

    @property
    def hmac(self):
        """
        Return the event HMAC.

        If event doesn't have a HMAC, return empty string
        If event has a HMAC and is not dirty, return HMAC
        If event is dirty, compute the new HMAC and return it
        """
        _hmac = self.structured_data[PYLOGGR_SDID]['hmac']
        if not _hmac:
            return u''
        if self._dirty:
            # event is dirty, we re-generate UUID and HMAC
            self.update_uuid_and_hmac()
        return iter(_hmac).next()

    def _set_hmac(self, new_hmac):
        self.structured_data[PYLOGGR_SDID]['hmac'] = [to_unicode(new_hmac)]

    def _make_hmac_obj(self):
        h = hmac_func.HMAC(self.HMAC_KEY, hashes.SHA256(), backend=default_backend())
        # HMAC doesn't accept unicode
        h.update(self.severity.encode("utf-8"))
        h.update(self.facility.encode("utf-8"))
        h.update(self.app_name.encode("utf-8"))
        h.update(self.source.encode("utf-8"))
        h.update(self.message.encode("utf-8"))
        h.update(str(self._timereported.to('utc')))
        h.update(str(self._timehmac.to('utc')))
        return h

    def generate_hmac(self, verify_if_exists=True):
        """
        Generate a HMAC from the fields: severity, facility, app_name, source, message, timereported

        :param verify_if_exists: if True and the event already has a HMAC, the existing HMAC will be verified instead of
        generating a new HMAC
        :type verify_if_exists: bool
        :return: a base 64 encoded HMAC
        :rtype: str
        :raise InvalidSignature: if HMAC already exists but is invalid
        """
        _hmac = self.structured_data[PYLOGGR_SDID]['hmac']
        if _hmac and verify_if_exists:
            self.verify_hmac()
            return _hmac
        self._set_timehmac(Arrow.utcnow())
        h = self._make_hmac_obj()
        _hmac.clear()
        _hmac.add(to_unicode(b64encode(h.finalize())))
        return iter(_hmac).next()

    def verify_hmac(self):
        """
        Verify event's HMAC

        Throws an InvalidSignature exception if HMAC is invalid

        :return: True
        :rtype: bool
        :raise InvalidSignature: if HMAC is invalid
        """
        _hmac = self.structured_data[PYLOGGR_SDID]['hmac']
        if not _hmac:
            logger.debug("Event (UUID: {}) doesn't have a HMAC".format(self.uuid))
            return True
        if self._dirty:
            logger.info("Can't verify HMAC: event is dirty")
            return True
        if not self._timehmac:
            raise InvalidSignature("Event (UUID: {}) has a HMAC but doesn't have a HMAC time".format(
                self.uuid
            ))
        h = self._make_hmac_obj()
        try:
            h.verify(b64decode(iter(_hmac).next()))
        except InvalidSignature:
            logger.error("Event (UUID: {}) has an invalid HMAC signature".format(self.uuid))
            raise
        return True

    @property
    def custom_fields(self):
        """
        Small helper to access pyloggr specific custom fields
        """
        return self.structured_data[PYLOGGR_SDID]

    @property
    def tags(self):
        """
        Access the event tags. Returns a set.
        """
        return self.structured_data[PYLOGGR_SDID]['tags']

    def add_tags(self, tags):
        """
        Add some tags to the event

        :param tags: a list of tags
        """
        if tags:
            self.structured_data[PYLOGGR_SDID]['tags'].update(sanitize_tag(tag) for tag in tags)

    def remove_tags(self, tags):
        """
        Remove some tags from the event.
        If the event does not really have such tag, it is ignored.

        :param tags: a list of tags
        """
        if tags:
            self.structured_data[PYLOGGR_SDID]['tags'].difference_update(
                sanitize_tag(tag) for tag in tags
            )

    def __getitem__(self, key):
        """
        Return a custom field, given its key

        :param key: custom field key
        :type key: str
        """
        return self.structured_data[PYLOGGR_SDID][key]

    def update_cfield(self, key, values):
        """
        Append some values to custom field `key`

        :param key: custom field key
        :param values: iterable
        """
        self.structured_data[PYLOGGR_SDID].add(key, values)

    def __setitem__(self, key, values):
        """
        Sets a custom field

        :param key: custom field key
        :type key: str
        :param values: custom field values
        :type values: iterable
        """
        self.structured_data[PYLOGGR_SDID][key] = values

    def __delitem__(self, key):
        """
        Deletes a custom field

        :param key: custom field key
        :type key: str
        """
        key = sanitize_key(key)
        if key in self.structured_data[PYLOGGR_SDID]:
            del self.structured_data[PYLOGGR_SDID][key]

    def update_cfields(self, d):
        """
        Add some custom fields to the event

        :param d: a dictionnary of new fields
        :type d: dict
        """
        self.structured_data[PYLOGGR_SDID].update(d)

    def __iter__(self):
        return iter(self.custom_fields)

    # noinspection PyDocstring
    def iterkeys(self):
        return iter(self.custom_fields)

    # noinspection PyDocstring
    def keys(self):
        return list(iter(self.custom_fields))

    def __contains__(self, key):
        """
        Return True if event has the given custom field, and the field is not empty

        :param key: custom field key
        :type key: str
        :rtype: bool
        """
        san = sanitize_key(key)
        if san not in self.structured_data[PYLOGGR_SDID]:
            return False
        return bool(self.structured_data[PYLOGGR_SDID][san])

    def _parse_trusted(self):
        """
        Parse the "trusted fields" that rsyslog could generate
        """
        # ex: @[_PID=5096 _UID=0 _GID=1000 _COMM=sudo test _EXE=/usr/bin/sudo test _CMDLINE="sudo test ls "]

        match_obj = RE_MSG_W_TRUSTED.match(self.message)
        if match_obj:
            self._message = to_unicode(match_obj.group(1).strip())
            s = match_obj.group(2).strip()
            trusted_fields_match = RE_TRUSTED_FIELDS.match(s)
            if not trusted_fields_match:
                return
            for f in viewvalues(trusted_fields_match.groupdict()):
                if f is None:
                    continue
                try:
                    f_name, f_content = f.split(u'=', 1)
                except (ValueError, AttributeError):
                    pass
                else:
                    f_name = to_unicode(f_name.strip(u' "\'\r\n'))
                    f_name = TRUSTED_FIELDS_MAP.get(f_name, None)
                    if f_name:
                        self.update_cfield(f_name, f_content.strip(u' "\'\r\n'))

    @classmethod
    def _load_syslog_rfc5424(cls, s):
        """
        Parse a rfc5424 string into an Event

        :param s: string event
        :type s: str
        :return: the new Event
        :rtype: Event
        """
        # noinspection PyTypeChecker
        s = to_unicode(s, fixes=True)
        match_obj = REGEXP_SYSLOG23.match(s)
        if match_obj is None:
            raise ParsingError("Event is not a SYSLOG23 string")
        flds = match_obj.groupdict()
        event_dict = dict()
        event_dict['facility'] = int(flds['PRI'])
        event_dict['severity'] = int(flds['PRI'])
        event_dict['source'] = flds['HOSTNAME']
        event_dict['app_name'] = flds['APPNAME']
        event_dict['programname'] = flds['APPNAME']
        try:
            event_dict['procid'] = int(flds['PROCID'])
        except ValueError:
            event_dict['procid'] = None
        event_dict['message'] = flds['MSG'].strip(' \n') if flds['MSG'] is not None else u''
        event_dict['timereported'] = flds['TIMESTAMP']
        event_dict['timegenerated'] = event_dict['timereported']
        if event_dict['procid'] is not None:
            event_dict['syslogtag'] = "{}[{}]".format(event_dict['app_name'], event_dict['procid'])
        else:
            event_dict['syslogtag'] = event_dict['app_name']

        if flds['STRUCTUREDDATA'] != '-':
            event_dict['structured_data'] = StructuredData.parse(flds['STRUCTUREDDATA'])

        try:
            return Event(**event_dict)
        except TypeError as ex:
            raise_from(ParsingError(u"Event could not be instantied"), ex)

    @classmethod
    def _load_syslog_rfc3164(cls, s):
        """
        Parse a rfc3164 string into an Event

        :param s: string event
        :type s: str
        :return: the new Event
        :rtype: Event
        """
        # noinspection PyTypeChecker
        s = to_unicode(s, fixes=True)
        match_obj = REGEXP_SYSLOG.match(s)
        if match_obj is None:
            raise ParsingError("Event is not a SYSLOG string")
        flds = match_obj.groupdict()
        event_dict = dict()
        event_dict['facility'] = int(flds['PRI'])
        event_dict['severity'] = int(flds['PRI'])
        event_dict['source'] = flds['HOSTNAME']
        event_dict['timereported'] = flds['TIMESTAMP']
        event_dict['timegenerated'] = event_dict['timereported']
        event_dict['message'] = flds['MSG'].strip(' \n-')
        try:
            name, number = flds['SYSLOGTAG'].split('[', 1)
        except ValueError:
            event_dict['app_name'] = flds['SYSLOGTAG'].strip()
            event_dict['procid'] = None
        else:
            event_dict['app_name'] = name
            event_dict['procid'] = number.strip('[]')
        event_dict['programname'] = event_dict['app_name']
        event_dict['syslogtag'] = flds['SYSLOGTAG'].strip()

        try:
            return Event(**event_dict)
        except TypeError as ex:
            raise_from(ParsingError(u"Event could not be instantied"), ex)

    @classmethod
    def load(cls, s):
        """
        Try to deserialize an Event from a string or a dictionnary. `load` understands JSON events, RFC 5424 events
        and RFC 3164 events, or dictionnary events. It automatically detects the type, using regexp tests.

        :param s: string (JSON or RFC 5424 or RFC 3164) or dictionnary
        :type s: str or dict or bytes
        :return: The parsed event
        :rtype: Event
        :raise `ParsingError`: if deserialization fails
        """
        if isinstance(s, dict):
            try:
                return Event(**s)
            except TypeError as ex:
                raise_from(ParsingError(u"Event could not be instantied"), ex)
                return
        if isinstance(s, past_unicode):
            s = s.encode('utf-8')
        if isinstance(s, realbytes):
            s = s.strip('\n\r ')
            if not len(s):
                raise ParsingError(u"empty string")
            first = ord(s[0])
            # 0xde or 0xdf or 0b1000xxxx from https://github.com/msgpack/msgpack/blob/master/spec.md
            if first == 222 or first == 223 or (128 <= first <= 143):
                # msgpack map format
                return cls._load_msgpack(s)
            # 123 = { in JSON
            if first == 123:
                return cls._load_json(s)
            if REGEXP_START_SYSLOG23.match(s):
                return cls._load_syslog_rfc5424(s)
            elif REGEXP_START_SYSLOG.match(s):
                return cls._load_syslog_rfc3164(s)
            else:
                raise ParsingError('unrecognized format')
        else:
            raise ParsingError(u"s must be a dict or a basestring")

    @classmethod
    def parse_bytes_to_event(cls, bytes_ev, hmac=False, swallow_exceptions=False):
        """
        Parse some bytes into an :py:class:`pyloggr.event.Event` object

        :param bytes_ev: the event as bytes
        :type bytes_ev: bytes
        :param hmac: generate/verify a HMAC
        :type hmac: bool
        :param swallow_exceptions: if True, return None rather than raising validation exceptions
        :type swallow_exceptions: bool
        :return: the new Event object
        :rtype: Event
        :raise ParsingError: if bytes could not be parsed correctly
        :raise InvalidSignature: if `hmac` is True and a HMAC already exists, but is invalid
        """
        try:
            # optimization: if we're sure that the event is JSON, skip type detection tests
            event = cls.load(bytes_ev)
        except ParsingError:
            logger.warning(u"Could not unmarshall a syslog event")
            logger.debug(to_unicode(bytes_ev))
            if swallow_exceptions:
                return
            else:
                raise

        if hmac:
            # verify HMAC if the event has one, else generate a HMAC
            try:
                event.generate_hmac(verify_if_exists=True)
            except InvalidSignature:
                if swallow_exceptions:
                    return
                else:
                    raise
        return event

    @classmethod
    def loads(cls, json_encoded):
        return cls._load_json(json_encoded)

    @classmethod
    def _load_json(cls, json_encoded):
        """
        Factory: returns an Event by parsing a JSON encoded string

        :param json_encoded: a JSON encoded event string
        :type json_encoded: str
        :rtype: Event
        :raise ParsingError: if parsing failed
        """
        try:
            d = ujson.loads(json_encoded)
        except ValueError as ex:
            raise_from(ParsingError(u"Provided string was not JSON parsable", json=True), ex)
        else:
            try:
                return Event(**d)
            except TypeError as ex:
                raise_from(ParsingError(u"Event could not be instantied"), ex)

    @classmethod
    def _load_msgpack(cls, msgpack_encoded):
        """
        Factory: returns an Event by parsing a msgpack encoded string

        :param msgpack_encoded: a JSON encoded event string
        :type msgpack_encoded: bytes
        :rtype: Event
        :raise ParsingError: if parsing failed
        """
        try:
            d = msgpack.unpackb(msgpack_encoded)
        except ValueError as ex:
            raise_from(ParsingError(u"Provided string was not msgpack parsable", json=True), ex)
        else:
            try:
                return Event(**d)
            except TypeError as ex:
                raise_from(ParsingError(u"Event could not be instantied"), ex)

    def dump(self, frmt="JSON", fname=None):
        """
        Dump the event

        Explicit format: a string with the following possible placeholders: $DATE, $DATETIME, $MESSAGE, $SOURCE,
        $APP_NAME, $SEVERITY, $FACILITY, $PROCID, $UUID, $TAGS

        :param frmt: dumping format (JSON, MSGPACK, RFC5424, RFC3164, RSYSLOG, ES or an explicit format)
        :param fname: if not None, write the dumped string to fname file
        :return: dumped string
        :raise OSError: if file operation fails (when fname is not None)
        """
        dispatch = {
            'JSON': self.dump_json,
            'MSGPACK': self.dump_msgpack,
            'RFC5424': self.dump_rfc5424,
            'RFC3164': self.dump_rfc3164,
            'RSYSLOG': self.dump_rsyslog,
            'ES': self.dumps_elastic
        }
        if frmt in dispatch:
            # noinspection PyCallingNonCallable
            s = dispatch[frmt]()
        else:
            self.update_uuid_and_hmac()
            # custom format
            return to_unicode(frmt).replace(
                "$DATE", str(self.timereported.date())
            ).replace(
                "$DATETIME", str(self._timereported)
            ).replace(
                "$MESSAGE", self.message
            ).replace(
                "$SOURCE", self.source
            ).replace(
                "$APP_NAME", self.app_name
            ).replace(
                "$SEVERITY", self.severity
            ).replace(
                "$FACILITY", self.facility
            ).replace(
                "$PROCID", str(self.procid) if self.procid else '-'
            ).replace(
                "$UUID", self.uuid
            ).replace(
                "$TAGS", ','.join(self.tags)
            )
        if fname:
            try:
                with LockFile(fname):
                    with open(fname, 'wb') as fhandle:
                        fhandle.write(s)
            except LockFailed as ex:
                raise_from(OSError("Event.dump_json: failed to lock '{}'".format(fname)), ex)
        return s

    def dump_msgpack(self):
        """
        Dump the event using msgpack
        """
        self.update_uuid_and_hmac()
        return msgpack.packb(self.dump_dict())

    def dump_rsyslog(self):
        """
        Dump the event as RSYSLOG_FileFormat

        see: http://www.rsyslog.com/doc/v8-stable/configuration/templates.html
        """
        self.update_uuid_and_hmac()
        # ex: 2015-05-17T05:35:01.651336+02:00 smtp.vesperal.eu CRON[25052]: pam_unix(cron:session): session closed for user root
        return u"{} {} {}: {}".format(str(self._timereported), self.source, self.syslogtag, self.message)

    def dump_rfc3164(self):
        """
        Dump the event into a RFC 3164 old-style syslog string
        """
        self.update_uuid_and_hmac()
        # ex: <34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8
        return u"<{}>{} {} {}: {}".format(
            self.priority,
            self._timereported.to('utc').format("MMM DD hh:mm:ss"),
            self.source,
            self.app_name,
            self.message
        )

    def dump_json(self):
        """
        Dump the event in JSON format

        :rtype: str
        """
        self.update_uuid_and_hmac()
        # instantiate a dedicate schema object to avoid thread safety issues
        return ujson.dumps(self.dump_dict())

    def dumps(self):
        return self.dump_json()

    def dump_rfc5424(self):
        """
        Dump the event into a RFC 5424 compliant string
        """
        self.update_uuid_and_hmac()
        procid = '-' if self.procid is None else self.procid
        header = u"<{}>1 {} {} {} {} -".format(
            self.priority, str(self._timereported), self.source, self.app_name, procid
        )

        if self.structured_data:
            structured_data_line = self.structured_data.dump()
        else:
            structured_data_line = "-"
        line = u"{} {} {}".format(header, structured_data_line, self.message)
        return line.encode('utf-8')

    def dumps_elastic(self):
        """
        Dumps in JSON suited for Elasticsearch

        :rtype: str
        """
        self.update_uuid_and_hmac()
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-timestamp-field.html
        d = self.dump_dict()
        return ujson.dumps(d)

    def dump_dict(self):
        """
        Serialize the event as a native python dict

        :rtype: dict
        """
        self.update_uuid_and_hmac()
        d = {
            'uuid':             self.uuid,
            'severity':         self.severity,
            'facility':         self.facility,
            'source':           self.source,
            'message':          self.message,
            'app_name':         self.app_name,
            'iut':              self.iut,
            'timereported':     str(self._timereported),
            'timegenerated':    str(self._timegenerated),
            'structured_data':  {
                sdid: {
                    key: list(values)
                    for key, values in viewitems(paramvalues)
                    if values
                }
                for sdid, paramvalues in viewitems(self.structured_data)
                if paramvalues
            }
        }
        if self.procid:
            d['procid'] = self.procid
        return d

    def dump_sql(self, cursor):
        """
        Dumps the event as a SQL insert statement

        :param cursor: SQL cursor
        :rtype: str
        """
        self.update_uuid_and_hmac()
        d = self.dump_dict()
        d['structured_data'] = Json(d['structured_data'])
        d['timereported'] = self.timereported       # datetime object
        d['timegenerated'] = self.timegenerated     # datetime object
        return cursor.mogrify(SQL_VALUES_STR, d)

    # noinspection PyDocstring
    def str(self):
        return EVENT_STR_FMT.format(self)

    def __str__(self):
        return self.str()

    def apply_filters(self, filters):
        """
        Apply some filters to the event

        :param filters: filters to apply
        """
        filters.apply(self)

    def lmdb_idx(self):
        return str(self._timereported.to('utc')) + self.uuid

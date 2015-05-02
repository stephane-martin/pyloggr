# encoding: utf-8
"""
The pyloggr.event module mainly provides the Event and EventSchema classes.

Event provides an abstraction of a log event.
EventSchema is used for marshalling/unmarshalling of Event objects.
"""

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

from future.utils import python_2_unicode_compatible, raise_from
# noinspection PyPackageRequirements,PyCompatibility
from past.builtins import basestring as basestr
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import hmac as hmac_func
from cryptography.exceptions import InvalidSignature
from psycopg2.extras import Json
from spooky_hash import Hash128

from pyloggr.utils.structured_data import parse_structured_data
from pyloggr.utils import to_unicode
from pyloggr.utils.constants import RE_MSG_W_TRUSTED, TRUSTED_FIELDS_MAP, REGEXP_SYSLOG, REGEXP_START_SYSLOG
from pyloggr.utils.constants import RE_TRUSTED_FIELDS, REGEXP_START_SYSLOG23, FACILITY, SEVERITY, SQL_VALUES_STR
from pyloggr.utils.constants import EVENT_STR_FMT, REGEXP_SYSLOG23

logger = logging.getLogger(__name__)


class ParsingError(ValueError):
    """
    Triggered when a string can't be parsed into an :py:class:`Event`
    """
    # noinspection PyUnusedLocal
    def __init__(self, *args, **kwargs):
        self.json = kwargs['json'] if 'json' in kwargs else False


SEVERITY_VALUES = SEVERITY.values()
FACILITY_VALUES = FACILITY.values()


@python_2_unicode_compatible
@total_ordering
class Event(object):
    HMAC_KEY = None
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
    trusted_pid: int
    trusted_uid= int
    trusted_gid: int
    trusted_comm: str
    trusted_exe: str
    trusted_cmdline: str
    custom_fields: dictionnary of custom fields
    structured_data: dictionnary representing syslog structured data
    tags: set of str

    """

    __slots__ = ('procid', 'trusted_uid', 'trusted_gid', 'trusted_pid', '_severity', '_facility',
                 '_app_name', '_source', 'programname', 'syslogtag', '_message', '_uuid', '_hmac',
                 '_timereported', '_timegenerated', '_timehmac', 'iut', 'trusted_comm', 'trusted_exe',
                 'trusted_cmdline', 'custom_fields', 'structured_data', '_tags', 'relp_id',
                 'have_been_published', '_dirty_uuid')

    @staticmethod
    def make_severity(severity):
        if not severity:
            return u'notice'
        try:
            # from encoded priority
            return SEVERITY.get(int(severity) & 7, u'')
        except ValueError:
            pass
        severity = to_unicode(severity)
        if severity in SEVERITY_VALUES:
            return severity
        return SEVERITY.get(severity.lower(), u'notice')

    @staticmethod
    def make_facility(facility):
        if facility is None:
            return u''
        try:
            # from encoded priority
            return FACILITY.get(int(facility) >> 3, u'')
        except ValueError:
            pass
        facility = to_unicode(facility)
        if facility in FACILITY_VALUES:
            return facility
        return FACILITY.get(facility.lower(), u'')

    @staticmethod
    def make_arrow_datetime(dt):
        if dt is None:
            return None
        if isinstance(dt, Arrow):
            return dt
        if isinstance(dt, datetime):
            return Arrow.fromdatetime(dt).to('utc')
        if isinstance(dt, basestr):
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

    def __init__(
            self, procid=u'-', severity=u'', facility=u'', app_name=u'', source=u'', programname=u'',
            syslogtag=u'', message=u'', uuid=None, hmac=None, timereported=None, timegenerated=None, timehmac=None,
            trusted_pid=None, trusted_uid=None, trusted_gid=None, trusted_comm=u'', trusted_exe=u'',
            trusted_cmdline=u'', custom_fields=None, structured_data=None, tags=None, iut=1, **kwargs
    ):

        try:
            self.procid = int(procid)
        except (ValueError, TypeError):
            self.procid = None

        try:
            self.trusted_uid = int(trusted_uid)
        except (ValueError, TypeError):
            self.trusted_uid = None

        try:
            self.trusted_gid = int(trusted_gid)
        except (ValueError, TypeError):
            self.trusted_gid = None

        try:
            self.trusted_pid = int(trusted_pid)
        except (ValueError, TypeError):
            self.trusted_pid = None

        self._severity = self.make_severity(severity)
        self._facility = self.make_facility(facility)
        self._app_name = to_unicode(app_name)
        self._source = to_unicode(source)
        self.programname = to_unicode(programname)
        self.syslogtag = to_unicode(syslogtag)
        self._message = to_unicode(message.strip('\r\n '))
        self._hmac = to_unicode(hmac)
        self.iut = iut
        self.trusted_comm = to_unicode(trusted_comm)
        self.trusted_exe = to_unicode(trusted_exe)
        self.trusted_cmdline = to_unicode(trusted_cmdline)

        self._timegenerated = Arrow.utcnow() if timegenerated is None else self.make_arrow_datetime(timegenerated)
        self._timereported = self._timegenerated if timereported is None else self.make_arrow_datetime(timereported)
        self._timehmac = self.make_arrow_datetime(timehmac)

        self.custom_fields = custom_fields if custom_fields else dict()
        self.structured_data = structured_data if structured_data else dict()
        self._tags = set(tags) if tags else set()

        self._parse_trusted()
        self._dirty_uuid = True
        if uuid:
            self._uuid = uuid
            self._dirty_uuid = False
        else:
            self.generate_uuid()
        self.relp_id = None

    @property
    def uuid(self):
        if self._dirty_uuid:
            self.generate_uuid()
            if self._hmac:
                self.generate_hmac(verify_if_exists=False)
        return self._uuid

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, new_msg):
        new_msg = to_unicode(new_msg)
        if new_msg != self._message:
            self._message = new_msg
            self._dirty_uuid = True

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, new_source):
        new_source = to_unicode(new_source)
        if new_source != self._source:
            self._source = new_source
            self._dirty_uuid = True

    @property
    def app_name(self):
        return self._app_name

    @app_name.setter
    def app_name(self, new_app_name):
        new_app_name = to_unicode(new_app_name)
        if new_app_name != self._app_name:
            self._app_name = new_app_name
            self._dirty_uuid = True

    @property
    def facility(self):
        return self._facility

    @facility.setter
    def facility(self, new_facility):
        new_f = self.make_facility(new_facility)
        if self._facility != new_f:
            self._facility = new_f
            self._dirty_uuid = True

    @property
    def severity(self):
        return self._severity

    @severity.setter
    def severity(self, new_severity):
        new_s = self.make_facility(new_severity)
        if self._severity != new_s:
            self._severity = new_s
            self._dirty_uuid = True

    @property
    def timegenerated(self):
        return self._timegenerated.datetime

    @timegenerated.setter
    def timegenerated(self, new_time):
        self._timegenerated = self.make_arrow_datetime(new_time)

    @property
    def timereported(self):
        return self._timereported.datetime

    @timereported.setter
    def timereported(self, new_time):
        new_t = self.make_arrow_datetime(new_time)
        if new_t != self._timereported:
            self._timereported = new_t
            self._dirty_uuid = True

    @property
    def timehmac(self):
        return None if self._timehmac is None else self._timehmac.datetime

    def generate_uuid(self):
        """
        Generate a UUID for the current event

        :return: new UUID
        :rtype: str
        """

        digest = Hash128()
        # Hash128 doesn't accept unicode
        digest.update(self.severity.encode("utf-8"))
        digest.update(self.facility.encode("utf-8"))
        digest.update(self.app_name.encode("utf-8"))
        digest.update(self.source.encode("utf-8"))
        digest.update(self.message.encode("utf-8"))
        digest.update(str(self._timereported.to('utc')))
        self._uuid = urlsafe_b64encode(digest.digest())
        self._dirty_uuid = False
        return self._uuid

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        """
        Two events are equal if they have the same UUID

        :type other: Event
        :rtype: bool
        """
        if self.uuid == other.uuid:
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
        if not self._hmac:
            return u''
        if not self._dirty_uuid:
            return self._hmac
        self.generate_uuid()        # self._dirty_uuid becomes False
        self.generate_hmac(verify_if_exists=False)
        return self._hmac

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
        if self._hmac and verify_if_exists:
            self.verify_hmac()
            return self._hmac
        self._timehmac = Arrow.utcnow()
        h = self._make_hmac_obj()
        self._hmac = to_unicode(b64encode(h.finalize()))
        return self._hmac

    def verify_hmac(self):
        """
        Verify the event HMAC
        Throws an InvalidSignature exception if HMAC is invalid

        :return: True
        :rtype: bool
        :raise InvalidSignature: if HMAC is invalid
        """
        if not self._hmac:
            raise InvalidSignature("Event (UUID: {}) doesn't have a HMAC".format(self.uuid))
        if not self._timehmac:
            raise InvalidSignature("Event (UUID: {}) doesn't have a HMAC time".format(self.uuid))
        h = self._make_hmac_obj()
        try:
            h.verify(b64decode(self._hmac))
        except InvalidSignature:
            logger.error("Event (UUID: {}) has an invalid HMAC signature".format(self.uuid))
            raise
        return True

    @property
    def tags(self):
        """
        The tags as a Python list
        """
        return list(self._tags)

    def add_tags(self, tags):
        """
        Add some tags to the event

        :param tags: a tag (str) or a list of tags
        """
        if isinstance(tags, basestr):
            self._tags.add(tags)
        else:
            self._tags.update(tags)

    def remove_tags(self, tags):
        """
        Remove some tags from the event.
        If the event does not really have such tag, it is ignored.

        :param tags: a tag (str) or a list of tags
        """

        if isinstance(tags, basestr):
            self._tags.remove(tags)
        else:
            self._tags.difference_update(tags)

    def __getitem__(self, key):
        """
        Return a custom field, given its key

        :param key: custom field key
        :type key: str
        """
        return self.custom_fields.get(key, None)

    def __setitem__(self, key, value):
        """
        Sets a custom field

        :param key: custom field key
        :type key: str
        :param value: custom field value
        :type value: str
        """
        self.custom_fields[key] = value

    def __delitem__(self, key):
        """
        Deletes a custom field

        :param key: custom field key
        :type key: str
        """
        del self.custom_fields[key]

    def update_fields(self, d):
        """
        Add some custom fields to the event

        :param d: a dictionnary of new fields
        :type d: dict
        """
        self.custom_fields.update(d)

    def __iter__(self):
        return iter(self.custom_fields)

    def iterkeys(self):
        return self.__iter__()

    def keys(self):
        return self.custom_fields.keys()

    def __contains__(self, key):
        """
        Return True if event has a given custom field

        :param key: custom field key
        :type key: str
        :rtype: bool
        """
        return key in self.custom_fields

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
            for f in trusted_fields_match.groupdict().values():
                if f is None:
                    continue
                try:
                    f_name, f_content = f.split('=', 1)
                except (ValueError, AttributeError):
                    pass
                else:
                    f_name = to_unicode(f_name.strip())
                    if f_name in TRUSTED_FIELDS_MAP:
                        f_name = TRUSTED_FIELDS_MAP[f_name]
                        self.__setattr__(f_name, f_content.strip(' "\''))

    @classmethod
    def _load_syslog_rfc5424(cls, s):
        """
        Parse a rfc5424 string into an Event

        :param s: string event
        :type s: str
        :return: the new Event
        :rtype: Event
        """
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
            event_dict['structured_data'] = parse_structured_data(flds['STRUCTUREDDATA'])

        return Event(**event_dict)

    @classmethod
    def _load_syslog_rfc3164(cls, s):
        """
        Parse a rfc3164 string into an Event

        :param s: string event
        :type s: str
        :return: the new Event
        :rtype: Event
        """
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

        return Event(**event_dict)

    @classmethod
    def load(cls, s):
        """
        Try to deserialize an Event from a string or a dictionnary. `load` understands JSON events, RFC 5424 events
        and RFC 3164 events, or dictionnary events. It automatically detects the type, using regexp tests.

        :param s: string (JSON or RFC 5424 or RFC 3164) or dictionnary
        :type s: str or dict
        :return: The parsed event
        :rtype: Event
        :raise `ParsingError`: if deserialization fails
        """
        if isinstance(s, dict):
            return Event(**s)
        if isinstance(s, basestr):
            if REGEXP_START_SYSLOG23.match(s):
                return cls._load_syslog_rfc5424(s)
            elif REGEXP_START_SYSLOG.match(s):
                return cls._load_syslog_rfc3164(s)
            else:
                return cls._load_json(s)
        else:
            raise ValueError(u"s must be a dict or a basestring")

    @classmethod
    def parse_bytes_to_event(cls, bytes_ev, hmac=False, json=False):
        """
        Parse some bytes into an :py:class:`pyloggr.event.Event` object


        :param bytes_ev: the event as bytes
        :type bytes_ev: bytes
        :param hmac: generate/verify a HMAC
        :type hmac: bool
        :param json: whether bytes_ev is a JSON string (if not sure, you can keep False)
        :type json: bool
        :return: the new Event object
        :rtype: Event
        :raise ParsingError: if bytes could not be parsed correctly
        :raise InvalidSignature: if `hmac` is True and a HMAC already exists, but is invalid
        """
        unicode_ev = to_unicode(bytes_ev)
        try:
            # optimization: if we're sure that the event is JSON, skip type detection tests
            event = cls._load_json(unicode_ev) if json else cls.load(unicode_ev)
        except ParsingError:
            logger.warning(u"Could not unmarshall a syslog event")
            logger.debug(to_unicode(unicode_ev))
            raise

        if hmac:
            # verify HMAC if the event has one, else generate a HMAC
            event.generate_hmac(verify_if_exists=True)
        return event

    @classmethod
    def _load_json(cls, json_encoded):
        """
        :type json_encoded: str
        :rtype: Event
        """
        try:
            d = ujson.loads(json_encoded)
        except ValueError as ex:
            raise_from(ParsingError(u"Provided string was not JSON parsable", json=True), ex)
        else:
            return Event(**d)

    def dumps(self):
        """
        :rtype: str
        """
        # instantiate a dedicate schema object to avoid thread safety issues
        return ujson.dumps(self.dump())

    def dumps_elastic(self):
        """
        Dumps in JSON suited for Elasticsearch

        :rtype: str
        """
        pass

    def dump(self):
        """
        Serialize the event as a native python dict

        :rtype: dict
        """
        return {
            'procid':           self.procid if self.procid else u'-',
            'uuid':             self.uuid,
            'hmac':             self.hmac,
            'severity':         self.severity,
            'facility':         self.facility,
            'source':           self.source,
            'message':          self.message,
            'app_name':         self.app_name,
            'programname':      self.programname,
            'syslogtag':        self.syslogtag,
            'iut':              self.iut,

            'trusted_pid':      self.trusted_pid if self.trusted_pid else None,
            'trusted_uid':      self.trusted_uid if self.trusted_uid else None,
            'trusted_gid':      self.trusted_gid if self.trusted_gid else None,
            'trusted_comm':     self.trusted_comm,
            'trusted_exe':      self.trusted_exe,
            'trusted_cmdline':  self.trusted_cmdline,

            'custom_fields':    self.custom_fields,
            'structured_data':  self.structured_data,
            'tags':             self.tags,
            'timereported':     str(self._timereported.to('utc')),
            'timegenerated':    str(self._timegenerated.to('utc')),
            'timehmac':         str(self._timehmac.to('utc')) if self._timehmac else None
        }

    def dump_sql(self, cursor):
        """
        :rtype: str
        """
        d = self.dump()
        d['tags'] = self.tags
        d['custom_fields'] = Json(self.custom_fields)
        d['structured_data'] = Json(self.structured_data)
        d['timereported'] = self._timereported
        d['timegenerated'] = self._timegenerated
        d['timehmac'] = self._timehmac
        return cursor.mogrify(SQL_VALUES_STR, d)

    def str(self):
        return EVENT_STR_FMT.format(self)

    def __str__(self):
        return self.str()

    def apply_filters(self, filters):
        """
        Apply some filters to the event
        """
        filters.apply(self)

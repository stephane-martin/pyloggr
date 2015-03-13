# encoding: utf-8
"""
The pyloggr.event module mainly provides the Event and EventSchema classes.

Event provides an abstraction of a log event.
EventSchema is used for marshalling/unmarshalling of Event objects.
"""

__author__ = 'stef'


import logging
from base64 import b64encode, b64decode
from datetime import datetime
import ujson
from arrow import Arrow
from marshmallow import Schema, fields
from marshmallow.exceptions import UnmarshallingError
from future.utils import python_2_unicode_compatible, raise_from
from builtins import str as text
# noinspection PyPackageRequirements
import past.builtins
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import hmac as hmac_func
from cryptography.exceptions import InvalidSignature
from dateutil.tz import tzutc
from psycopg2.extras import Json

from .utils.fix_unicode import to_unicode
from .utils.constants import RE_MSG_W_TRUSTED, TRUSTED_FIELDS_MAP, REGEXP_SYSLOG, REGEXP_START_SYSLOG, RE_TRUSTED_FIELDS
from .utils.constants import REGEXP_START_SYSLOG23, FACILITY, SEVERITY, SQL_VALUES_STR, EVENT_STR_FMT, REGEXP_SYSLOG23
from .config import HMAC_KEY


logger = logging.getLogger(__name__)


class ParsingError(ValueError):
    """
    Triggered when a string can't be parsed into an `Event`
    """
    pass


class CFieldSchema(Schema):
    """
    CFieldSchema()
    Marshmallow schema for :py:class:`CField` class
    """

    class Meta:
        strict = True
        json_module = ujson
        ordered = True

    key = fields.String()
    value = fields.String()

    def make_object(self, data):
        if data is None:
            return None
        return CField(**data)


class EventSchema(Schema):
    """
    Marshmallow schema for the :py:class:`Event` class
    """

    class Meta:
        strict = True
        json_module = ujson
        ordered = True

    procid = fields.String()
    severity = fields.String(required=True)
    facility = fields.String(required=True)
    app_name = fields.String(required=False)
    source = fields.String(required=True)
    programname = fields.String(required=False)
    syslogtag = fields.String(required=False)
    message = fields.String(required=True)
    iut = fields.Integer(required=False)
    uuid = fields.String(required=False)
    hmac = fields.String(required=False)
    timereported = fields.DateTime(required=False)
    timegenerated = fields.DateTime(required=False)
    timehmac = fields.DateTime(required=False)
    trusted_pid = fields.String(required=False)
    trusted_uid = fields.String(required=False)
    trusted_gid = fields.String(required=False)
    trusted_comm = fields.String(required=False)
    trusted_exe = fields.String(required=False)
    trusted_cmdline = fields.String(required=False)
    cfields = fields.Nested(CFieldSchema, allow_null=True, many=True, default=list())
    # noinspection PyTypeChecker
    tags = fields.List(fields.String, allow_none=False, default=list())

    def make_object(self, data):
        return Event(**data)


# noinspection PyUnusedLocal
@EventSchema.data_handler
def handle_version(serializer, data, o):
    data['@version'] = 1
    return data


# noinspection PyUnusedLocal
@EventSchema.data_handler
def handle_numeric(serializer, data, o):
    data['procid'] = None if o.procid is None else o.procid
    data['trusted_uid'] = None if o.trusted_uid is None else o.procid
    data['trusted_gid'] = None if o.trusted_gid is None else o.procid
    data['trusted_pid'] = None if o.trusted_pid is None else o.procid
    return data


# noinspection PyUnusedLocal
@EventSchema.preprocessor
def handle_non_numeric_procid(schema, data):
    data['procid'] = data['procid'].replace('-', '').strip()
    return data


@python_2_unicode_compatible
class CField(object):
    """
    Encapsulate custom fields in Events

    Attributes
    ----------
    key: str
        custom field key
    value: str
        custom field value
    schema: CFieldSchema
        schema (class variable)
    """

    schema = CFieldSchema()
    __slots__ = ('key', 'value')

    def __init__(self, key="", value=""):
        self.key = key
        self.value = value

    def str(self):
        return u"{}: {}".format(self.key, self.value)

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.str()


@python_2_unicode_compatible
class Event(object):
    """
    Represents a syslog event, with optional tags, additional fields and structured data

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
    cfields: list of CField
    tags: set of str

    Todo
    ====
    delete iut field ?

    """
    schema = EventSchema()

    __slots__ = ('procid', 'trusted_uid', 'trusted_gid', 'trusted_pid', 'severity', 'facility',
                 'app_name', 'source', 'programname', 'syslogtag', 'message', 'uuid', 'hmac',
                 'timereported', 'timegenerated', 'timehmac', 'iut', 'trusted_comm', 'trusted_exe',
                 'trusted_cmdline', 'cfields', '_tags')

    def __init__(self, procid=None, severity="", facility="", app_name="", source="", programname="",
                 syslogtag="", message="", uuid="", hmac="", timereported=None, timegenerated=None, timehmac=None, iut=1,
                 trusted_pid=None, trusted_uid=None, trusted_gid=None, trusted_comm="", trusted_exe="", trusted_cmdline="",
                 cfields=None, tags=None):

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

        self.severity = severity
        self.facility = facility
        self.app_name = app_name
        self.source = source
        self.programname = programname
        self.syslogtag = syslogtag
        self.message = message.strip()
        self.uuid = uuid
        self.hmac = hmac
        self.timereported = timereported
        self.timegenerated = timegenerated
        self.timehmac = timehmac
        self.iut = iut
        self.trusted_comm = trusted_comm
        self.trusted_exe = trusted_exe
        self.trusted_cmdline = trusted_cmdline
        self.cfields = list() if cfields is None else cfields
        self._tags = set() if tags is None else set(tags)
        self._parse_trusted()
        self._generate_uuid()
        self._generate_time()

    def _generate_uuid(self):
        """
        Generate a UUID for the current event, if it hasn't got one before

        :rtype: str
        """
        if len(self.uuid) > 0:
            logger.debug("Event already has an UUID: {}".format(self.uuid))
            return
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(self.severity.encode("utf-8"))
        digest.update(self.facility.encode("utf-8"))
        digest.update(self.app_name.encode("utf-8"))
        digest.update(self.source.encode("utf-8"))
        digest.update(self.message.encode("utf-8"))
        if self.timereported is not None:
            digest.update(str(self.timereported))
        self.uuid = b64encode(digest.finalize())
        logger.debug("New UUID: {}".format(self.uuid))
        return self.uuid

    def _generate_time(self):
        """
        If the event hasn't got a timegenerated field, give the current timestamp
        """
        if self.timegenerated is None:
            self.timegenerated = datetime.now(tzutc())

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        """
        Two events are equal if they have the same UUID
        :rtype: bool
        """
        return self.uuid == other.uuid

    def __cmp__(self, other):
        """
        Compare two events by their "first seen" time
        :rtype: int
        """
        if self.uuid == other.uuid:
            return 0
        return cmp(self.timegenerated, other.timegenerated)

    def _hmac(self):
        h = hmac_func.HMAC(HMAC_KEY, hashes.SHA256(), backend=default_backend())
        h.update(self.severity.encode("utf-8"))
        h.update(self.facility.encode("utf-8"))
        h.update(self.app_name.encode("utf-8"))
        h.update(self.source.encode("utf-8"))
        h.update(self.message.encode("utf-8"))
        if self.timereported is not None:
            # take care of changing timezones...
            h.update(str(Arrow.fromdatetime(self.timereported).float_timestamp))

        h.update(str(self.timehmac))
        return h

    def generate_hmac(self):
        """
        Generate a HMAC from the fields: severity, facility, app_name, source, message, timereported

        :return: a base 64 encoded HMAC
        :rtype: str
        """
        if self.hmac:
            logger.warning("Event already has a HMAC")
            self.verify_hmac()
            return
        self.timehmac = datetime.now(tzutc())
        h = self._hmac()
        self.hmac = b64encode(h.finalize())
        return self.hmac

    def verify_hmac(self):
        """
        Verify the event HMAC
        Throws an InvalidSignature exception if HMAC is invalid

        :return: True
        :rtype: bool
        :raise InvalidSignature: if HMAC is invalid
        """
        if not self.hmac:
            raise InvalidSignature("Event (UUID: {}) doesn't have a HMAC".format(self.uuid))
        if not self.timehmac:
            raise InvalidSignature("Event (UUID: {}) doesn't have a HMAC time".format(self.uuid))
        h = self._hmac()
        try:
            h.verify(b64decode(self.hmac))
        except InvalidSignature:
            logger.error("Event (UUID: {}) has an invalid HMAC signature".format(self.uuid))
            raise
        else:
            logger.debug("HMAC of event is valid (UUID: {})".format(self.uuid))
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
        if isinstance(tags, past.builtins.basestring):
            self._tags.add(tags)
        else:
            self._tags.update(tags)

    def remove_tags(self, tags):
        """
        Remove some tags from the event.
        If the event does not really have such tag, it is ignored.

        :param tags: a tag (str) or a list of tags
        """

        if isinstance(tags, past.builtins.basestring):
            self._tags.remove(tags)
        else:
            self._tags.difference_update(tags)

    def __getitem__(self, key):
        """
        Return a custom field, given its key

        :param key: custom field key
        :type key: str
        """
        for f in self.cfields:
            if key == f.key:
                return f.value
        return None

    def __setitem__(self, key, value):
        """
        Sets a custom field
        :param key: custom field key
        :type key: str
        :param value: custom field value
        :type value: str
        """

        existing_indice = None
        for (i, f) in enumerate(self.cfields):
            if key == f.key:
                existing_indice = i
                break
        if existing_indice is not None:
            self.cfields[existing_indice] = CField(key, value)
        else:
            self.cfields.append(CField(key, value))

    def __delitem__(self, key):
        """
        Deletes a custom field

        :param key: constm field key
        :type key: str
        """
        existing_indice = None
        for (i, f) in enumerate(self.cfields):
            if key == f.key:
                existing_indice = i
                break
        if existing_indice is not None:
            del self.cfields[existing_indice]

    def update(self, d):
        """
        Add some custom fields to the event

        :param d: a dictionnary of new fields
        :type d: dict
        """
        if d is not None:
            for (key, value) in d.items():
                self[key] = value

    def __iter__(self):
        return iter([f.key for f in self.cfields])

    def iterkeys(self):
        return self.__iter__()

    def __contains__(self, key):
        """
        Return True if event has a given custom field

        :param key: custom field key
        :type key: str
        :rtype: bool
        """
        return key in [f.key for f in self.cfields]

    @property
    def fields_as_dict(self):
        """
        Returns the custom fields as a Python dict

        :return: dict
        :rtype: dict
        """
        return dict([(f.key, f.value) for f in self.cfields])

    def _parse_trusted(self):
        """
        Parse the "trusted fields" that rsyslog could have generated
        """
        # ex: @[_PID=5096 _UID=0 _GID=1000 _COMM=sudo test _EXE=/usr/bin/sudo test _CMDLINE="sudo test ls "]

        match_obj = RE_MSG_W_TRUSTED.match(self.message)
        if match_obj:
            self.message = match_obj.group(1).strip()
            s = match_obj.group(2).strip()
            trusted_fields = RE_TRUSTED_FIELDS.match(s).groupdict().values()
            for f in trusted_fields:
                try:
                    f_name, f_content = f.split('=', 1)
                except ValueError:
                    pass
                else:
                    f_name = to_unicode(f_name.strip())
                    if f_name in TRUSTED_FIELDS_MAP:
                        f_name = TRUSTED_FIELDS_MAP[f_name]
                        self.__setattr__(f_name, f_content.strip(' "\''))

    @classmethod
    def _load_dictionnary(cls, d):
        """
        Parse a dictionnary into an Event

        :param d: dictionnary
        :return: Event
        """
        # workaround: marshmallow doesnt like None datetimes
        if d.get('timegenerated') is None:
            d.pop('timegenerated', None)
        if d.get('timereported') is None:
            d.pop('timereported', None)
        if d.get('timehmac') is None:
            d.pop('timehmac', None)

        try:
            return cls.schema.load(d).data
        except UnmarshallingError as ex:
            raise_from(ParsingError("Error when unmarshalling the event"), ex)

    @classmethod
    def _load_syslog_rfc5424(cls, s):
        """
        Parse a rfc5424 string into an Event

        :param s: string event
        :return: Event
        """
        # todo: structured data can be present
        match_obj = REGEXP_SYSLOG23.match(s)
        if match_obj is None:
            raise ParsingError("Event is not a SYSLOG23 string")
        flds = match_obj.groupdict()
        event_dict = dict()
        event_dict['facility'] = FACILITY.get(int(flds['PRI']) >> 3, None)
        event_dict['severity'] = SEVERITY.get(int(flds['PRI']) & 7, None)
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

        ev = cls._load_dictionnary(event_dict)
        if flds['STRUCTUREDDATA'] != '-':
            ev.add_tags('rfc5424 structured data')
            logger.debug(flds['STRUCTUREDDATA'])
        return ev

    @classmethod
    def _load_syslog_rfc3164(cls, s):
        """
        Parse a rfc3164 string into an Event

        :param s: string event
        :return: Event
        """
        match_obj = REGEXP_SYSLOG.match(s)
        if match_obj is None:
            raise ParsingError("Event is not a SYSLOG string")
        flds = match_obj.groupdict()
        event_dict = dict()
        event_dict['facility'] = FACILITY.get(int(flds['PRI']) >> 3, None)
        event_dict['severity'] = SEVERITY.get(int(flds['PRI']) & 7, None)
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

        return cls._load_dictionnary(event_dict)

    @classmethod
    def load(cls, s):
        """
        Try to deserialize an Event from a string or a dictionnary

        :param s: string (JSON or RFC 5424 or RFC 3164) or dictionnary
        :return: The parsed event
        :rtype: Event
        :raise `ParsingError`: if deserialization failed
        """
        if isinstance(s, dict):
            return cls._load_dictionnary(s)
        if isinstance(s, past.builtins.basestring):
            s = to_unicode(s)

            if REGEXP_START_SYSLOG23.match(s):
                return cls._load_syslog_rfc5424(s)
            elif REGEXP_START_SYSLOG.match(s):
                return cls._load_syslog_rfc3164(s)
            else:
                return cls._load_json(s)
        else:
            raise ValueError(u"s must be a dict or a basestring")

    @classmethod
    def parse_bytes_to_event(cls, bytes_ev):
        """
        Parse some bytes into an :py:class:`pyloggr.event.Event` object

        Note
        ====
        We generate a HMAC for this new event

        :param bytes_ev: the event as bytes
        :type bytes_ev: bytes
        :return: Event object
        :raise ParsingError: if bytes could not be parsed correctly
        """
        try:
            event = cls.load(bytes_ev)
        except ParsingError:
            logger.warning(u"Could not unmarshall a syslog event")
            logger.debug(to_unicode(bytes_ev))
            raise
        else:
            event.generate_hmac()
            return event

    @classmethod
    def _load_json(cls, json_encoded):
        """
        :type json_encoded: str
        """
        try:
            d = ujson.loads(json_encoded)
        except ValueError as ex:
            raise_from(ParsingError(u"Provided string was not JSON parsable"), ex)
        else:
            return cls._load_dictionnary(d)

    def dumps(self):
        return self.schema.dumps(self).data

    def dumps_elastic(self):
        """
        Dumps in JSON suited for Elasticsearch
        """
        deserialized, errors = self.schema.dump(self)
        custom_fields = self.fields_as_dict
        deserialized.update(custom_fields)
        deserialized['@timestamp'] = deserialized['timereported']
        del deserialized['timereported']
        del deserialized['cfields']
        return ujson.dumps(deserialized)

    def dump(self):
        return self.schema.dump(self).data

    def dump_sql(self, cursor):
        d = self.dump()
        d['tags'] = self.tags
        d['cfields'] = Json(self.fields_as_dict)
        d['timereported'] = self.timereported
        d['timegenerated'] = self.timegenerated
        d['timehmac'] = self.timehmac
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

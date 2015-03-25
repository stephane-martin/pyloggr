# encoding: utf-8
"""
The pyloggr.event module mainly provides the Event and EventSchema classes.

Event provides an abstraction of a log event.
EventSchema is used for marshalling/unmarshalling of Event objects.
"""

__author__ = 'stef'


import logging
from base64 import b64encode, b64decode
import ujson
from arrow import Arrow
from marshmallow import Schema, fields
from marshmallow.exceptions import UnmarshallingError
from future.utils import python_2_unicode_compatible, raise_from
# noinspection PyPackageRequirements
import past.builtins
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import hmac as hmac_func
from cryptography.exceptions import InvalidSignature
from psycopg2.extras import Json
from spooky_hash import Hash128

from .utils.structured_data import parse_structured_data
from .utils.fix_unicode import to_unicode
from .utils.constants import RE_MSG_W_TRUSTED, TRUSTED_FIELDS_MAP, REGEXP_SYSLOG, REGEXP_START_SYSLOG, RE_TRUSTED_FIELDS
from .utils.constants import REGEXP_START_SYSLOG23, FACILITY, SEVERITY, SQL_VALUES_STR, EVENT_STR_FMT, REGEXP_SYSLOG23
from .config import HMAC_KEY

logger = logging.getLogger(__name__)
hmac_func.HMAC(HMAC_KEY, hashes.SHA256(), backend=default_backend())


class ParsingError(ValueError):
    """
    Triggered when a string can't be parsed into an :py:class:`Event`
    """
    def __init__(self, *args, **kwargs):
        self.json = kwargs['json'] if 'json' in kwargs else False


class EventSchema(Schema):
    """
    Marshmallow schema for the :py:class:`Event` class
    """

    class Meta:
        strict = True
        json_module = ujson
        ordered = True

    procid = fields.String(required=False, default="-")                # string because procid can be "-"
    severity = fields.String(required=True)
    facility = fields.String(required=True)
    app_name = fields.String(required=False, default='')
    source = fields.String(required=True)
    programname = fields.String(required=False, default='')
    syslogtag = fields.String(required=False, default='')
    message = fields.String(required=True)
    iut = fields.Integer(required=False, default=1)
    uuid = fields.String(required=False, default=None)
    hmac = fields.String(required=False, default=None)
    timereported = fields.DateTime(required=False, default=None)
    timegenerated = fields.DateTime(required=False, default=None)
    timehmac = fields.DateTime(required=False, default=None)
    trusted_pid = fields.Integer(required=False, default=None)
    trusted_uid = fields.Integer(required=False, default=None)
    trusted_gid = fields.String(required=False, default=None)
    trusted_comm = fields.String(required=False, default='')
    trusted_exe = fields.String(required=False, default='')
    trusted_cmdline = fields.String(required=False, default='')
    custom_fields = fields.Field(required=False, default=dict())
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
    try:
        data['procid'] = int(data['procid'])
    except (ValueError, TypeError):
        data['procid'] = None
    return data


# noinspection PyUnusedLocal
@EventSchema.preprocessor
def handle_non_numeric_procid(schema, data):
    data['procid'] = data['procid'].replace('-', '').strip()
    return data

@python_2_unicode_compatible
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
    trusted_pid: int
    trusted_uid= int
    trusted_gid: int
    trusted_comm: str
    trusted_exe: str
    trusted_cmdline: str
    custom_fields: dictionnary of custom fields
    tags: set of str

    """

    __slots__ = ('procid', 'trusted_uid', 'trusted_gid', 'trusted_pid', 'severity', 'facility',
                 'app_name', 'source', 'programname', 'syslogtag', 'message', 'uuid', 'hmac',
                 'timereported', 'timegenerated', 'timehmac', 'iut', 'trusted_comm', 'trusted_exe',
                 'trusted_cmdline', 'custom_fields', '_tags')

    def __init__(
            self, procid='-', severity="", facility="", app_name="", source="", programname="",
            syslogtag="", message="", uuid=None, hmac=None, timereported=None, timegenerated=None, timehmac=None, iut=1,
            trusted_pid=None, trusted_uid=None, trusted_gid=None, trusted_comm="", trusted_exe="", trusted_cmdline="",
            custom_fields=None, tags=None
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
        self.custom_fields = custom_fields if custom_fields else dict()
        self._tags = set(tags) if tags else set()
        self._parse_trusted()
        self._generate_uuid()
        self._generate_time()

    def _generate_uuid(self):
        """
        Generate a UUID for the current event, if it hasn't got one before

        :rtype: str
        """
        if self.uuid:
            logger.debug("Event already has an UUID: {}".format(self.uuid))
            return
        digest = Hash128()
        # Hash128 doesn't accept unicode
        digest.update(self.severity.encode("utf-8"))
        digest.update(self.facility.encode("utf-8"))
        digest.update(self.app_name.encode("utf-8"))
        digest.update(self.source.encode("utf-8"))
        digest.update(self.message.encode("utf-8"))
        if self.timereported is not None:
            digest.update(str(Arrow.fromdatetime(self.timereported)))
        self.uuid = b64encode(digest.digest())
        logger.debug("New UUID: {}".format(self.uuid))
        return self.uuid

    def _generate_time(self):
        """
        If the event hasn't got a timegenerated field, give the current timestamp
        """
        if self.timegenerated is None:
            self.timegenerated = Arrow.utcnow().datetime

    def __hash__(self):
        return self.uuid

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
        # HMAC doesn't accept unicode
        h.update(self.severity.encode("utf-8"))
        h.update(self.facility.encode("utf-8"))
        h.update(self.app_name.encode("utf-8"))
        h.update(self.source.encode("utf-8"))
        h.update(self.message.encode("utf-8"))
        if self.timereported is not None:
            # take care of timezones...
            h.update(str(Arrow.fromdatetime(self.timereported)))
        h.update(str(self.timehmac))
        return h

    def generate_hmac(self):
        """
        Generate a HMAC from the fields: severity, facility, app_name, source, message, timereported

        :return: a base 64 encoded HMAC
        :rtype: str
        :raise InvalidSignature: if HMAC already exists but is invalid
        """
        if self.hmac:
            logger.warning("Event already has a HMAC")
            self.verify_hmac()
            return
        self.timehmac = Arrow.utcnow().datetime
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

    def update(self, d):
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
        Parse the "trusted fields" that rsyslog could have generated
        """
        # ex: @[_PID=5096 _UID=0 _GID=1000 _COMM=sudo test _EXE=/usr/bin/sudo test _CMDLINE="sudo test ls "]

        match_obj = RE_MSG_W_TRUSTED.match(self.message)
        if match_obj:
            self.message = match_obj.group(1).strip()
            s = match_obj.group(2).strip()
            trusted_fields_match = RE_TRUSTED_FIELDS.match(s)
            if trusted_fields_match:
                for f in trusted_fields_match.groupdict().values():
                    if f is not None:
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
    def _load_dictionnary(cls, d):
        """
        Parse a dictionnary into an Event

        :param d: dictionnary
        :return: Event
        """
        # workaround: marshmallow doesn't like 'None' objects
        if d.get('timegenerated') is None:
            d.pop('timegenerated', None)
        if d.get('timereported') is None:
            d.pop('timereported', None)
        if d.get('timehmac') is None:
            d.pop('timehmac', None)

        # instantiate a dedicate schema object to avoid thread safety issues
        schema = EventSchema()
        try:
            return schema.load(d).data
        except UnmarshallingError as ex:
            raise_from(ParsingError("Error when unmarshalling the event"), ex)

    @classmethod
    def _load_syslog_rfc5424(cls, s):
        """
        Parse a rfc5424 string into an Event

        :param s: string event
        :return: Event
        """
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
            parsed = parse_structured_data(flds['STRUCTUREDDATA'])
            if parsed is not None:
                ev.add_tags('rfc5424_structured_data')
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
        Try to deserialize an Event from a string or a dictionnary. `load` understands JSON events, RFC 5424 events
        and RFC 3164 events, or dictionnary events. It automatically detects the type, using regexp tests.

        :param s: string (JSON or RFC 5424 or RFC 3164) or dictionnary
        :return: The parsed event
        :rtype: Event
        :raise `ParsingError`: if deserialization fails
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
    def parse_bytes_to_event(cls, bytes_ev, hmac=False, json=False):
        """
        Parse some bytes into an :py:class:`pyloggr.event.Event` object

        Note
        ====
        We generate a HMAC for this new event. If the event already has a HMAC, we verify it

        :param bytes_ev: the event as bytes
        :type bytes_ev: bytes
        :param hmac: generate/verify a HMAC
        :type hmac: bool
        :param json: whether bytes_ev is a JSON string (if not sure, you can keep False)
        :type json: bool
        :return: Event object
        :raise ParsingError: if bytes could not be parsed correctly
        :raise InvalidSignature: if `hmac` is True and a HMAC already exists, but is invalid
        """
        try:
            # optimization: if we're sure that the event is JSON, skip type detection tests
            event = cls._load_json(bytes_ev) if json else cls.load(bytes_ev)
        except ParsingError:
            logger.warning(u"Could not unmarshall a syslog event")
            logger.debug(to_unicode(bytes_ev))
            raise
        if hmac:
            # verify HMAC if the event has one, else generate a HMAC
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
            raise_from(ParsingError(u"Provided string was not JSON parsable", json=True), ex)
        else:
            return cls._load_dictionnary(d)

    def dumps(self):
        # instantiate a dedicate schema object to avoid thread safety issues
        return EventSchema().dumps(self).data

    def dumps_elastic(self):
        """
        Dumps in JSON suited for Elasticsearch
        """
        # instantiate a dedicate schema object to avoid thread safety issues
        deserialized, errors = EventSchema().dump(self)
        deserialized['@timestamp'] = deserialized['timereported']
        del deserialized['timereported']
        return ujson.dumps(deserialized)

    def dump(self):
        # instantiate a dedicate schema object to avoid thread safety issues
        return EventSchema().dump(self).data

    def dump_sql(self, cursor):
        d = self.dump()
        d['tags'] = self.tags
        d['custom_fields'] = Json(self.custom_fields)
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

# encoding: utf-8
"""
Useful constants
"""

__author__ = 'stef'

import re

RE_MSG_W_TRUSTED = re.compile(r'\A(.*)\s+@\[(.*)\]\s*\Z', flags=re.DOTALL | re.MULTILINE)
TRUSTED_LABELS = r'(_PID|_UID|_GID|_COMM|_EXE|_CMDLINE)'
RE_TRUSTED_FIELDS = re.compile(
    r'\A(?P<u1>{}=.*?)(\s(?P<u2>{}=.*?))?(\s(?P<u3>{}=.*?))?(\s(?P<u4>{}=.*?))?(\s(?P<u5>{}=.*?))?(\s(?P<u6>{}=.*?))?\Z'.format(
        TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS
    )
)


STRUCT_DATA = r'(-|(\[([#-~]+)(\s([#-~]+)="(([^"]|(\\"))+)")*\])+)'

REGEXP_SYSLOG23 = re.compile(
    r"\A\s*<(?P<PRI>\d+)>\d+\s+(?P<TIMESTAMP>\d\S+\d(\s|T)\d\S+\dZ?)\s+(?P<HOSTNAME>\S+)\s+(?P<APPNAME>\S+)\s+(?P<PROCID>\S+)\s+(?P<MSGID>\S+)\s+"
    r"(?P<STRUCTUREDDATA>" + STRUCT_DATA + ")" + r"(\s+(?P<MSG>.*))?\Z",
    flags=re.DOTALL | re.MULTILINE | re.UNICODE
)
# <13>1 2015-05-08 22:21:43.758882+00:00 montgomery.local pyloggr - - - zogzog
YEAR  = r'(?:\d\d){1,2}'
MONTH = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b'
MONTHDAY = r'(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])'
MONTHNUM = r'(?:0?[1-9]|1[0-2])'


HOUR = r'(?:2[0123]|[01][0-9])'
MINUTE = r'(?:[0-5][0-9])'
SECOND = r'(?:(?:[0-5][0-9]|60)(?:[.,][0-9]+)?)'
TIME = r'(?!<[0-9]){}:{}(?::{})(?![0-9])'.format(HOUR, MINUTE, SECOND)
ISO8601_TIMEZONE = r'(?:Z|[+-]{}(?::?{}))'.format(HOUR, MINUTE)

TIMESTAMP_ISO8601 = r'{}-{}-{}[T ]{}:?{}(?::?{})?{}?'.format(
    YEAR, MONTHNUM, MONTHDAY, HOUR, MINUTE, SECOND, ISO8601_TIMEZONE
)
TIMESTAMP_SYSLOG = r'{}\s+{}\s{}'.format(MONTH, MONTHDAY, TIME)
TIMESTAMP = r'({}|{})'.format(TIMESTAMP_ISO8601, TIMESTAMP_SYSLOG)


OLD_SYSLOG = r"\A\s*<(?P<PRI>\d+)>\s*(?P<TIMESTAMP>{})\s+(?P<HOSTNAME>\S+)\s+(?P<SYSLOGTAG>[^:\s\[]+(\[\d+\])?):?\s*(?P<MSG>.*)\Z".format(
    TIMESTAMP
)

REGEXP_SYSLOG = re.compile(
    OLD_SYSLOG,
    flags=re.DOTALL | re.MULTILINE | re.UNICODE
)

REGEXP_START_SYSLOG23 = re.compile(r'\A\s*<(?P<PRI>\d+)>\d+\s', flags=re.MULTILINE)
REGEXP_START_SYSLOG = re.compile(r'\A\s*<(?P<PRI>\d+)>\s*[a-zA-Z0-9]{2}', flags=re.MULTILINE)


TRUSTED_FIELDS_MAP = {
    '_PID':     'trusted_pid',
    '_UID':     'trusted_uid',
    '_GID':     'trusted_gid',
    '_COMM':    'trusted_comm',
    '_EXE':     'trusted_exe',
    '_CMDLINE': 'trusted_cmdline'
}

EVENT_STR_FMT = u"""
UUID            {0.uuid}
Source          {0.source}
HMAC            {0.hmac}
Time generated  {0.timegenerated}
Time reported   {0.timereported}
Time HMAC       {0.timehmac}
Facility        {0.facility}
Severity        {0.severity}
Program name    {0.programname}
App name        {0.app_name}
Tag             {0.syslogtag}
Proc id         {0.procid}
Message         {0.message}
Tags            {0.tags}
Custom fields   {0.custom_fields}
Structured      {0.structured_data}

"""

FACILITY = {
    0:                  u'kern',
    "0":                u'kern',
    'kernel':           u'kern',
    'kern':             u'kern',
    1:                  u'user',
    "1":                u'user',
    'user':             u'user',
    'usr':              u'user',
    2:                  u'mail',
    "2":                u'mail',
    'mail':             u'mail',
    'email':            u'mail',
    3:                  u'daemon',
    "3":                u'daemon',
    'daemon':           u'daemon',
    'demon':            u'daemon',
    4:                  u'auth',
    "4":                u'auth',
    'auth':             u'auth',
    5:                  u'syslog',
    "5":                u'syslog',
    'syslog':           u'syslog',
    6:                  u'lpr',
    "6":                u'lpr',
    'lpr':              u'lpr',
    7:                  u'news',
    "7":                u'news',
    'news':             u'news',
    8:                  u'uucp',
    "8":                u'uucp',
    'uucp':             u'uucp',
    9:                  u'clock',
    "9":                u'clock',
    'clock':            u'clock',
    10:                 u'authpriv',
    "10":               u'authpriv',
    'authpriv':         u'authpriv',
    11:                 u'ftp',
    "11":               u'ftp',
    'ftp':              u'ftp',
    12:                 u'ntp',
    "12":               u'ntp',
    'ntp':              u'ntp',
    13:                 u'audit',
    "13":               u'audit',
    'audit':            u'audit',
    14:                 u'alert',
    "14":               u'alert',
    'alert':            u'alert',
    15:                 u'cron',
    "15":               u'cron',
    'cron':             u'cron',
    16:                 u'local0',
    "16":               u'local0',
    'local0':           u'local0',
    17:                 u'local1',
    "17":               u'local1',
    'local1':           u'local1',
    18:                 u'local2',
    "18":               u'local2',
    'local2':           u'local2',
    19:                 u'local3',
    "19":               u'local3',
    'local3':           u'local3',
    20:                 u'local4',
    "20":               u'local4',
    'local4':           u'local4',
    21:                 u'local5',
    "21":               u'local5',
    'local5':           u'local5',
    22:                 u'local6',
    "22":               u'local6',
    'local6':           u'local6',
    23:                 u'local7',
    "23":               u'local7',
    'local7':           u'local7'
}

FACILITY_TO_INT = {
    u'kern':        0,
    u'user':        1,
    u'mail':        2,
    u'daemon':      3,
    u'auth':        4,
    u'syslog':      5,
    u'lpr':         6,
    u'news':        7,
    u'uucp':        8,
    u'clock':       9,
    u'authpriv':    10,
    u'ftp':         11,
    u'ntp':         12,
    u'audit':       13,
    u'alert':       14,
    u'cron':        15,
    u'local0':      16,
    u'local1':      17,
    u'local2':      18,
    u'local3':      19,
    u'local4':      20,
    u'local5':      21,
    u'local6':      22,
    u'local7':      23,
    '':             1
}


SEVERITY = {
    0:              u'emerg',
    "0":            u'emerg',
    'emergency':    u'emerg',
    'emerg':        u'emerg',
    1:              u'alert',
    "1":            u'alert',
    'alert':        u'alert',
    2:              u'crit',
    "2":            u'crit',
    'critical':     u'crit',
    'crit':         u'crit',
    3:              u'err',
    "3":            u'err',
    'error':        u'err',
    'err':          u'err',
    4:              u'warning',
    "4":            u'warning',
    'warning':      u'warning',
    'warn':         u'warning',
    5:              u'notice',
    "5":            u'notice',
    'note':         u'notice',
    'notice':       u'notice',
    6:              u'info',
    "6":            u'info',
    'info':         u'info',
    u'info':        u'info',
    7:              u'debug',
    "7":            u'debug',
    'debug':        u'debug'
}

SEVERITY_TO_INT = {
    u'emerg': 0,
    u'alert': 1,
    u'crit': 2,
    u'err': 3,
    u'warning': 4,
    u'notice': 5,
    u'info': 6,
    u'debug': 7,
    u'': 5
}


SQL_COLUMNS = [
    u"procid", u"severity", u"facility", u"app_name", u"source", u"programname", u"syslogtag", u"uuid",
    u"timereported", u"timegenerated", u"timehmac", u"message", u"hmac", u'structured_data'
]
SQL_COLUMNS_STR = u','.join(SQL_COLUMNS)
SQL_VALUES_STR = \
    u'(' \
    + u','.join(
        [u'%(' + column + u')s' for column in SQL_COLUMNS]
    ) \
    + u')'

D_COLUMNS = u'd.procid::int, d.severity, d.facility, d.app_name, d.source, d.programname, d.syslogtag, d.uuid,' \
            u'd.timereported::timestamptz, d.timegenerated::timestamptz, d.timehmac::timestamptz, d.message, d.hmac,' \
            u'd.structured_data::jsonb'

# if we already have an event with same UUID in database, that's a duplicate, and we skip the insert
SQL_INSERT_QUERY = u"""WITH data({}) AS (
VALUES
      {}
)
INSERT INTO {} ({})
SELECT {}
FROM data d
WHERE NOT EXISTS (SELECT 1 FROM {} s2 WHERE s2.uuid = d.uuid);"""

RELP_OPEN_COMMAND = "open 52 relp_version=0\nrelp_software=pyloggr\ncommands=syslog\n"
RELP_CLOSE_COMMAND = "close 0\n"

PYLOGGR_ENTERPRISE_NUMBER = u"45878"    # http://www.iana.org/assignments/enterprise-numbers/enterprise-numbers
PYLOGGR_SDID = u"pyloggr@{}".format(PYLOGGR_ENTERPRISE_NUMBER)

CIPHERS = "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:" \
          "DHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:RSA-AES256-SHA:" \
          "HIGH:" \
          "-aNULL:-eNULL:-MD5:-DSS:-3DES:-DES:-RC4:-RC2:-ADH:-kECDH:-SRP:-KRB5:-PSK"

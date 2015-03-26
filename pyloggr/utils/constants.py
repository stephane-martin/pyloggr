# encoding: utf-8
__author__ = 'stef'

import re
import regex

RE_MSG_W_TRUSTED = re.compile(r'\A(.*)\s+@\[(.*)\]\s*\Z', flags=re.DOTALL | re.MULTILINE)
TRUSTED_LABELS = r'(_PID|_UID|_GID|_COMM|_EXE|_CMDLINE)'
RE_TRUSTED_FIELDS = re.compile(
    r'\A(?P<u1>{}=.*?)(\s(?P<u2>{}=.*?))?(\s(?P<u3>{}=.*?))?(\s(?P<u4>{}=.*?))?(\s(?P<u5>{}=.*?))?(\s(?P<u6>{}=.*?))?\Z'.format(
        TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS, TRUSTED_LABELS
    )
)


STRUCT_DATA = r'(-|(\[([#-~]+)(\s([#-~]+)="(([^"]|(\\"))+)")*\])+)'

REGEXP_SYSLOG23 = regex.compile(
    r"\A\s*<(?<PRI>\d+)>\d+\s+(?<TIMESTAMP>\S+)\s+(?<HOSTNAME>\S+)\s+(?<APPNAME>\S+)\s+(?<PROCID>\S+)\s+(?<MSGID>\S+)\s+"
    r"(?<STRUCTUREDDATA>" + STRUCT_DATA + ")" + r"(\s+(?<MSG>.*))?\Z",
    flags=regex.DOTALL | regex.MULTILINE | regex.V1 | regex.UNICODE
)

YEAR  = r'(?>\d\d){1,2}'
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

REGEXP_SYSLOG = regex.compile(
    OLD_SYSLOG,
    flags=re.DOTALL | re.MULTILINE | regex.V1 | regex.UNICODE
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
Trusted PID     {0.trusted_pid}
Trusted UID     {0.trusted_uid}
Trusted GID     {0.trusted_gid}
EXE             {0.trusted_exe}
Comm            {0.trusted_comm}
CMDline         {0.trusted_cmdline}
Message         {0.message}
Tags            {0.tags}
Fields          {0.custom_fields}
Structured      {0.structured_data}

"""

FACILITY = {
    0:              'kern',
    1:              'user',
    2:              'mail',
    3:              'daemon',
    4:              'auth',
    5:              'syslog',
    6:              'lpr',
    7:              'news',
    8:              'uucp',
    9:              'clock',
    10:             'authpriv',
    11:             'ftp',
    12:             'ntp',
    13:             'audit',
    14:             'alert',
    15:             'cron',
    16:             'local0',
    17:             'local1',
    18:             'local2',
    19:             'local3',
    20:             'local4',
    21:             'local5',
    22:             'local6',
    23:             'local7'
}

SEVERITY = {
    0:  'emerg',
    1:  'alert',
    2:  'crit',
    3:  'err',
    4:  'warning',
    5:  'notice',
    6:  'info',
    7:  'debug'
}


SQL_COLUMNS = [
    u"procid", u"severity", u"facility", u"app_name", u"source", u"programname", u"syslogtag", u"uuid", u"timereported",
    u"timegenerated", u"timehmac", u"trusted_pid", u"trusted_uid", u"trusted_gid", u"trusted_comm", u"trusted_exe",
    u"trusted_cmdline", u"message", u"hmac", u"tags", u"custom_fields"
]
SQL_COLUMNS_STR = u','.join(SQL_COLUMNS)
SQL_VALUES_STR = \
    u'(' \
    + u','.join(
        [u'%(' + column + u')s' for column in SQL_COLUMNS]
    ) \
    + u')'

D_COLUMNS = u'd.procid::int, d.severity, d.facility, d.app_name, d.source, d.programname, d.syslogtag, d.uuid,' \
            u'd.timereported::timestamptz, d.timegenerated::timestamptz, d.timehmac::timestamptz, d.trusted_pid::int,' \
            u'd.trusted_uid::int, d.trusted_gid::int, d.trusted_comm, d.trusted_exe, d.trusted_cmdline, d.message, d.hmac,' \
            u'd.tags::text[], d.custom_fields::jsonb'

# if we already have an event with same UUID in database, that's a duplicate, and we skip the insert
SQL_INSERT_QUERY = u"""WITH data({}) AS (
VALUES
      {}
)
INSERT INTO {} ({})
SELECT {}
FROM data d
WHERE NOT EXISTS (SELECT 1 FROM {} s2 WHERE s2.uuid = d.uuid);"""

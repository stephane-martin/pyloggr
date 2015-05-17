# encoding: utf-8

import unicodedata
import re


cdef bytes byte_cr = b'\r'
cdef bytes byte_lf = b'\n'


LIGATURES = {
    ord(u'Ĳ'): u'IJ',
    ord(u'ĳ'): u'ij',
    ord(u'ﬀ'): u'ff',
    ord(u'ﬁ'): u'fi',
    ord(u'ﬂ'): u'fl',
    ord(u'ﬃ'): u'ffi',
    ord(u'ﬄ'): u'ffl',
    ord(u'ﬅ'): u'ſt',
    ord(u'ﬆ'): u'st'
}


SINGLE_QUOTE_RE = re.compile(u'[\u2018-\u201b]')
DOUBLE_QUOTE_RE = re.compile(u'[\u201c-\u201f]')
ANSI_RE = re.compile(u'\033\\[((?:\\d|;)*)([a-zA-Z])')


CONTROL_CHARS = {}
for i in range(32):
    CONTROL_CHARS[i] = None
for char in u'\t\n\f\r':
    del CONTROL_CHARS[ord(char)]


cdef unicode guess_bytes(bytes bstring):
    """
    Convert a bytes string to unicode string, by guessing encoding

    :param bstring: bytes string
    :return: unicode string
    """
    cdef int l
    l = len(bstring)
    if l == 0:
        return u''
    if bstring[0] == b'\xfe':
        if l == 1:
            return u'þ'
        elif bstring[1] == b'\xff':
            return bstring.decode('utf-16')
    if bstring[0] == b'\xff':
        if l == 1:
            return u'ÿ'
        elif bstring[1] == b'\xfe':
            return bstring.decode('utf-16')
    try:
        return bstring.decode('utf-8')
    except UnicodeDecodeError:
        pass

    chars = set(bstring)
    if byte_cr in chars and byte_lf not in chars:
        return bstring.decode('macroman')
    else:
        return bstring.decode('windows-1252', 'replace')


cdef unicode common_fixes(unicode s):
    """
    Apply common fixes to a unicode string

    :param s: unicode string
    :return: normalized unicode string
    """
    cdef unicode t
    t = s.translate(LIGATURES)
    t = t.translate(CONTROL_CHARS)
    t = ANSI_RE.sub(u'', t)
    t = t.replace(u'\r\n', u'\n').replace(u'\r', u'\n').replace('\u2028', u'\n').replace('\u2029', u'\n').replace('\u0085', u'\n')
    t = SINGLE_QUOTE_RE.sub(u"'", DOUBLE_QUOTE_RE.sub(u'"', t))
    return unicodedata.normalize('NFC', t)


cpdef unicode to_unicode(s, bint fixes=False):
    """
    Convert to unicode

    :param s: object to convert
    :param fixes: should we apply common unicode fixes, such as NFC normalization
    :return: unicode string
    """
    if type(s) is unicode:
        if fixes:
            return common_fixes(<unicode>s)
        return <unicode>s
    if isinstance(s, bytes):
        return common_fixes(guess_bytes(<bytes>s))
    if type(s) is int or type(s) is float:
        return u'{}'.format(s)
    if isinstance(s, unicode):
        if fixes:
            return common_fixes(<unicode>s)
        return <unicode>s
    if s is None:
        return u''
    return to_unicode(str(s))

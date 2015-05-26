# encoding: utf-8

from cpython cimport bool
from cpython.unicode cimport PyUnicode_Check, PyUnicode_Translate, PyUnicode_AsUTF8String, PyUnicode_Format
from cpython.unicode cimport PyUnicode_Replace

from cpython.bytes cimport PyBytes_Size, PyBytes_Check
from cpython.string cimport PyString_AsDecodedObject
from cpython.int cimport PyInt_Check
from cpython.float cimport PyFloat_Check
from cpython.set cimport PySet_New, PySet_Contains

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
    ord(u'ﬅ'): u'ft',
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
    l = PyBytes_Size(bstring)
    if l == 0:
        return u''
    if bstring[0] == b'\xfe':
        if l == 1:
            return u'þ'
        elif bstring[1] == b'\xff':
            try:
                return PyString_AsDecodedObject(bstring, 'utf-16', 'strict')
            except UnicodeDecodeError:
                pass
    if bstring[0] == b'\xff':
        if l == 1:
            return u'ÿ'
        elif bstring[1] == b'\xfe':
            try:
                return PyString_AsDecodedObject(bstring, 'utf-16', 'strict')
            except UnicodeDecodeError:
                pass
    try:
        return PyString_AsDecodedObject(bstring, 'utf-8', 'strict')
    except UnicodeDecodeError:
        pass


    chars = PySet_New(bstring)
    if PySet_Contains(chars, byte_cr) and not PySet_Contains(chars, byte_lf):
        return PyString_AsDecodedObject(bstring, 'macroman', 'strict')
        #return bstring.decode('macroman')
    else:
        #return bstring.decode('windows-1252', 'replace')
        return PyString_AsDecodedObject(bstring, 'windows-1252', 'strict')


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
    # PyUnicode_Replace(object str, object substr, object replstr, Py_ssize_t maxcount)
    t = PyUnicode_Replace(
        PyUnicode_Replace(
            PyUnicode_Replace(
                PyUnicode_Replace(
                    PyUnicode_Replace(
                        t, u'\r\n', u'\n', -1
                    ),
                    u'\r', u'\n', -1
                ),
                '\u2028', u'\n', -1
            ),
            '\u2029', u'\n', -1
        ),
        '\u0085', u'\n', -1
    )
    t = SINGLE_QUOTE_RE.sub(u"'", DOUBLE_QUOTE_RE.sub(u'"', t))
    return unicodedata.normalize('NFC', t)

# todo: HTML strip

cpdef bytes to_bytes(object s):
    """
    Convert object to a bytes string

    :param s: object
    :return: bytes
    """
    if PyUnicode_Check(s):
        return PyUnicode_AsUTF8String(<unicode>s)
    if PyBytes_Check(s):
        return <bytes>s
    # todo: better default
    return PyUnicode_AsUTF8String(to_unicode(s))

cpdef unicode to_unicode(object s, bool fixes=False):
    """
    Convert to unicode

    :param s: object to convert
    :param fixes: should we apply common unicode fixes, such as NFC normalization
    :return: unicode string
    """
    if PyUnicode_Check(s):
        if fixes:
            return common_fixes(<unicode>s)
        return <unicode>s
    if isinstance(s, bytes):
        # always apply common fixes when the source is bytes
        return common_fixes(guess_bytes(<bytes>s))
    if PyInt_Check(s) or PyFloat_Check(s):
        return u'{}'.format(s)
    if s is None:
        return u''
    return PyString_AsDecodedObject(str(s), 'utf-8', 'strict')

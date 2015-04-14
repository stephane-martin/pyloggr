# encoding: utf-8
__author__ = 'stef'

cdef bytes byte_cr = b'\r'
cdef bytes byte_lf = b'\n'


cdef unicode guess_bytes(bytes bstring):
    if bstring[0] == b'\xfe':
        if len(bstring) > 1:
            if bstring[1] == b'\xff':
                return bstring.decode('utf-16')
    if bstring[0] == b'\xff':
        if len(bstring) > 1:
            if bstring[1] == b'\xfe':
                return bstring.decode('utf-16')
    try:
        return bstring.decode('utf-8')
    except UnicodeDecodeError:
        pass

    if byte_cr in bstring and byte_lf not in bstring:
        return bstring.decode('macroman')
    else:
        return bstring.decode('windows-1252', 'replace')


cpdef unicode to_unicode(basestring s):
    if type(s) is unicode:
        return <unicode>s
    if isinstance(s, bytes):
        return guess_bytes(<bytes>s)
    if isinstance(s, unicode):
        return unicode(s)
    return u''

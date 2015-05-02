# encoding: utf-8
__author__ = 'stef'

cdef bytes byte_cr = b'\r'
cdef bytes byte_lf = b'\n'


cdef unicode guess_bytes(bytes bstring):
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


cpdef unicode to_unicode(s):
    if type(s) is unicode:
        return <unicode>s
    if isinstance(s, bytes):
        return guess_bytes(<bytes>s)
    if type(s) is int or type(s) is float:
        return u'{}'.format(s)
    if isinstance(s, unicode):
        return <unicode>s
    return u''      # None falls here

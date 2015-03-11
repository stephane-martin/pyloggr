# encoding: utf-8
__author__ = 'stef'

from builtins import str as text
import ftfy

def to_unicode(s):
    if isinstance(s, text):
        return s
    return ftfy.guess_bytes(s)[0]

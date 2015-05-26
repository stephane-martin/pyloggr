# encoding: utf-8

"""
Pyparsing parser for rfc5424 structured data

Credit: https://bitbucket.org/evax/loggerglue
"""

import logging
from itertools import imap
from io import BytesIO
import pickle
import re
import csv

from future.utils import viewitems, viewvalues
from future.builtins import str as real_unicode
from future.builtins import bytes as real_bytes
from pyparsing import CharsNotIn, QuotedString, Group, ZeroOrMore, OneOrMore, Or, Word, White, ParseException
from pyparsing import Suppress, Literal
from cytoolz.itertoolz import isiterable

from pyloggr.utils import sanitize_key, to_unicode, to_bytes
from pyloggr.utils.constants import PYLOGGR_SDID

sp = Suppress(White(" ", exact=1))
nilvalue = Word("-")
sd_name = CharsNotIn('= ]"', 1, 32)
param_name = sd_name.setResultsName('SD_PARAM_NAME')
param_value = QuotedString(quoteChar='"', escChar='\\', multiline=True, unquoteResults=False)
param_value = param_value.setResultsName('SD_PARAM_VALUE')
sd_id = sd_name.setResultsName('SD_ID')
sd_param = Group(param_name + Suppress(Literal('=')) + param_value)
sd_params = Group(ZeroOrMore(sp + sd_param))
sd_element = Group(Suppress('[') + sd_id + sd_params.setResultsName('SD_PARAMS') + Suppress(']'))
sd_element = sd_element.setResultsName('SD_ELEMENT')
sd_elements = Group(OneOrMore(sd_element))
structured_data_parser = Or([nilvalue, sd_elements])
structured_data_parser = structured_data_parser.setResultsName('STRUCTURED_DATA')
structured_data_parser_string = pickle.dumps(structured_data_parser)


_escape_re = re.compile(r'([,="\]])')


def _escape_param_values(param_values):
    # RFC 5424: PARAM-VALUE = UTF-8-STRING ; characters '"', '\' and ']' MUST be escaped.
    return u','.join(_escape_re.sub(r'\\\1', pvalue) for pvalue in param_values)


def split_escape(s):
    """
    Split string by comma delimiter, excepted escaped commas

    :param s: string
    :type s: str
    :rtype: str
    """
    return list(imap(
        lambda t: to_unicode(t).strip(' \r\n'),
        csv.reader(BytesIO(to_bytes(s.strip('"'))), delimiter=',', escapechar='\\').next()
    ))


class StructuredDataNamesValues(dict):
    """
    Dict subclass. Values are sets of unicode strings. Keys are sanitized before used.
    """
    def __init__(self, d=None):
        if d is None:
            super(StructuredDataNamesValues, self).__init__()
        elif isinstance(d, StructuredDataNamesValues):
            super(StructuredDataNamesValues, self).__init__(d)
        elif isinstance(d, dict):
            super(StructuredDataNamesValues, self).__init__()
            list(imap(
                lambda (key, values): self.__setitem__(key, values),
                viewitems(d)
            ))
        else:
            raise ValueError(
                "StructuredDataNamesValues.__init__: argument should be a dict, not a '{}'".format(
                    str(type(d))
                )
            )

    def __setitem__(self, key, values):
        if isinstance(values, real_bytes) or isinstance(values, real_unicode) or (not isiterable(values)):
            super(StructuredDataNamesValues, self).__setitem__(sanitize_key(key), {to_unicode(values)})
        else:
            super(StructuredDataNamesValues, self).__setitem__(
                sanitize_key(key),
                set(imap(lambda value: to_unicode(value), values))
            )

    def __getitem__(self, key):
        san = sanitize_key(key)
        if not super(StructuredDataNamesValues, self).__contains__(san):
            super(StructuredDataNamesValues, self).__setitem__(san, set())
        return super(StructuredDataNamesValues, self).__getitem__(san)

    def add(self, key, values):
        if values is None:
            return
        if isinstance(values, real_bytes) or isinstance(values, real_unicode) or (not isiterable(values)):
            values = [values]
        san = sanitize_key(key)
        if not super(StructuredDataNamesValues, self).__contains__(san):
            super(StructuredDataNamesValues, self).__setitem__(san, set())
        super(StructuredDataNamesValues, self).__getitem__(san).update(
            to_unicode(value) for value in values
        )

    def update(self, e=None, **f):
        """
        Update the object using another dict. New values are added to the values set.
        """
        # for k in E: self[k] = E[k]
        # for k in F: self[k) = F(k)
        if e is not None:
            if not isinstance(e, dict):
                raise ValueError("StructuredDataNamesValues.update: e should be a dict, not a '{}'".format(
                    str(type(e))
                ))
            list(imap(
                lambda (key, values): self.add(key, values),
                viewitems(e)
            ))
        list(imap(
            lambda (key, values): self.add(key, values),
            viewitems(f)
        ))

    def dump(self):
        """
        Return string representation
        """
        return u' '.join(
            u'{}="{}"'.format(key, _escape_param_values(values)) for key, values in viewitems(self) if values
        )

    def __bool__(self):
        return any(len(values) > 0 for values in viewvalues(self))

    def __nonzero__(self):
        return self.__bool__()


class StructuredData(dict):
    """
    Encapsulate RFC 5424 structured data
    """
    def __init__(self, d=None):
        if d is None:
            super(StructuredData, self).__init__()
        elif isinstance(d, StructuredData):
            super(StructuredData, self).__init__(d)
        elif isinstance(d, dict):
            list(imap(
                lambda (sdid, paramnames): self.__setitem__(sdid, paramnames),
                viewitems(d)
            ))
        else:
            raise ValueError("StructuredData.__init__: argument should be a dict, not a '{}'".format(
                str(type(d))
            ))
        if PYLOGGR_SDID not in self:
            self.__setitem__(PYLOGGR_SDID, StructuredDataNamesValues())
        if 'tags' not in self[PYLOGGR_SDID]:
            self[PYLOGGR_SDID]['tags'] = set()

    def __setitem__(self, sdid, paramvalues):
        if not isinstance(paramvalues, dict):
            raise ValueError("value should be a dict")
        sdid = sanitize_key(sdid)
        if not super(StructuredData, self).__contains__(sdid):
            super(StructuredData, self).__setitem__(sdid, StructuredDataNamesValues())
        existing_paramvalues = super(StructuredData, self).__getitem__(sdid)
        list(imap(
            lambda (name, values): existing_paramvalues.__setitem__(name, values),
            viewitems(paramvalues)
        ))

    @classmethod
    def parse(cls, s):
        """
        Parse structured data from string into dict of dict

        :param s: string
        """
        results = cls()
        # workaround for thread safety
        parser = pickle.loads(structured_data_parser_string)
        logger = logging.getLogger(__name__)
        try:
            toks = parser.parseString(s, parseAll=True)
        except ParseException:
            logger.error("Unparsable structured data: {}".format(s))
            return results
        s_data = toks['STRUCTURED_DATA']
        if s_data == '-':
            return results
        list(imap(
            lambda element: results.__setitem__(
                element['SD_ID'],
                {
                    param['SD_PARAM_NAME']: split_escape(param.get('SD_PARAM_VALUE', u''))
                    for param in element['SD_PARAMS']
                }
            ),
            s_data
        ))
        return results

    def dump(self):
        return u''.join(
            u'[{} {}]'.format(sdid, paramvalues.dump())
            for sdid, paramvalues in viewitems(self)
            if paramvalues
        )

    def __bool__(self):
        return any(values for values in viewvalues(self))

    def __nonzero__(self):
        return self.__bool__()

    def update(self, e=None, **f):
        """
        Update structured data using another dict. Values are added to the values sets.
        """
        if e is not None:
            if not(isinstance(e, dict)):
                raise ValueError("StructuredData.update: e should be a dict, not a '{}'".format(
                    str(type(e))
                ))
            list(self[sdid].update(paramvalues) for sdid, paramvalues in viewitems(e))
        list(self[sdid].update(paramvalues) for sdid, paramvalues in viewitems(f))

# encoding: utf-8

"""
Pyparsing parser for rfc5424 structured data

Credit: https://bitbucket.org/evax/loggerglue
"""

import logging
from pyparsing import CharsNotIn, QuotedString, Group, ZeroOrMore, OneOrMore, Or, Regex, Word, White, ParseException
from pyparsing import Suppress, Literal

sp = Suppress(White(" ", exact=1))
nilvalue = Word("-")
sd_name = CharsNotIn('= ]"', 1, 32)
param_name = sd_name.setResultsName('SD_PARAM_NAME')
# todo: respect escaping RFC 5424
param_value = QuotedString(quoteChar='"', escChar='\\', multiline=True)
param_value = param_value.setResultsName('SD_PARAM_VALUE')
sd_id = sd_name.setResultsName('SD_ID')
sd_param = Group(param_name + Suppress(Literal('=')) + param_value)
sd_params = Group(ZeroOrMore(sp + sd_param))
sd_element = Group(Suppress('[') + sd_id + sd_params.setResultsName('SD_PARAMS') + Suppress(']'))
sd_element = sd_element.setResultsName('SD_ELEMENT')
sd_elements = Group(OneOrMore(sd_element))
structured_data_parser = Or([nilvalue, sd_elements])
structured_data_parser = structured_data_parser.setResultsName('STRUCTURED_DATA')


def parse_structured_data(s):
    """
    Parse structured data from string into dict of dict

    :param s: string
    """
    results = {}
    try:
        toks = structured_data_parser.parseString(s, parseAll=True)
    except ParseException:
        return None
    s_data = toks['STRUCTURED_DATA']
    if s_data == '-':
        return None
    for element in s_data:
        sdid = element['SD_ID']
        results[sdid] = {}
        for param in element['SD_PARAMS']:
            results[sdid][param['SD_PARAM_NAME']] = param['SD_PARAM_VALUE']
    return results

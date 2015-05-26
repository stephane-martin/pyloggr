# encoding: utf-8

"""
Pyparsing stuff to parse `packers_config`
"""

__author__ = 'stef'

from collections import namedtuple
import logging
from functools import partial


from pyparsing import Group, Optional, delimitedList, OneOrMore, Keyword, ParseException, MatchFirst, Suppress
from pyparsing import ZeroOrMore
from future.utils import raise_from

from pyloggr.utils import to_unicode
from pyloggr.utils.parsing_classes import label, open_delim, close_delim, kw_arg_assign, my_qs, if_cond
from pyloggr.utils.parsing_classes import condition, comment_line, integer, Constant
from pyloggr.packers.merge_by_format import PackerByFormat
from pyloggr.packers.merge_by_time import PackerByTime


logger = logging.getLogger(__name__)


packers_implementation = {
    'merge_by_time': PackerByTime,
    'merge_by_format': PackerByFormat
}

packer_name = MatchFirst([Keyword(name) for name in packers_implementation])


def make_kw_argument(toks):
    values = list(toks[1])
    l = lambda x: x.name if isinstance(x, Constant) else x
    values = map(l, values)
    nb_values = len(values)
    return {toks[0]: values} if nb_values > 1 else {toks[0]: values[0]}


def make_arguments(toks):
    list_of_dicts = toks[0]
    res = {}
    for d in list_of_dicts:
        res.update(d)
    return res


def make_packer(toks):
    name = toks[0]
    args = toks[1]
    return partial(packers_implementation[name], **args)


PackerGroup = namedtuple("PackerGroup", ['name', 'condition', 'packers'])


def make_packer_group_without(toks):
    name = toks[0]
    packers = list(toks[1])
    return PackerGroup(name=name, packers=packers, condition=None)


def make_packer_group_with(toks):
    name = toks[0]
    cond = toks[1]
    packers = list(toks[2])
    return PackerGroup(name=name, packers=packers, condition=cond)


def make_all_groups(toks):
    toks = list(toks)
    return {group.name: group for group in toks}


packer_kw_argument = (label + kw_arg_assign + Group(delimitedList(my_qs | integer))).setParseAction(make_kw_argument)
packer_arguments = Optional(Group(delimitedList(packer_kw_argument))).setParseAction(make_arguments)
packer_call = (packer_name + open_delim + packer_arguments + close_delim).setParseAction(make_packer)


packer_group_with_condition = label + \
                              open_delim + \
                              Suppress(ZeroOrMore(comment_line)) + \
                              if_cond + \
                              condition + \
                              open_delim + \
                              Group(OneOrMore(packer_call | comment_line)) + \
                              close_delim + \
                              Suppress(ZeroOrMore(comment_line)) + \
                              close_delim


packer_group_with_condition = packer_group_with_condition.setParseAction(make_packer_group_with)

packer_group_without_condition = label + \
                                 open_delim + \
                                 Group(OneOrMore(packer_call | comment_line)) + \
                                 close_delim

packer_group_without_condition = packer_group_without_condition.setParseAction(make_packer_group_without)

packer_group = packer_group_with_condition | packer_group_without_condition


packers_config_parser = (OneOrMore(comment_line | packer_group)).setParseAction(make_all_groups)


def _parse_string(s):
    try:
        return packers_config_parser.parseString(s, parseAll=True)[0]
    except ParseException as ex:
        logging.exception(ex)
        raise_from(ValueError("Syntax Error in packers configuration"), ex)


def parse_config_file(packer_config_filename):
    """
    Parse the `packers_config` configuration file.

    :param packer_config_filename: location of configuration file
    :type packer_config_filename: str
    """
    with open(packer_config_filename, 'rb') as handle:
        s = handle.read()
    return _parse_string(to_unicode(s))


b = """
openvpn_packer_group {
    # openvpn emits multi-lines events
    # merge them if they were emitted roughly at the same timestamp (here 250ms interval)

    # there should be only zero or one condition per packer group

    if (app_name ~* 'ovpn') and (facility == 'daemon') {
        merge_by_time(
            queue_max_age=5000,
            merge_event_window=250
        )
    }
}
"""

if __name__ == '__main__':
    CONFIG_DIR = "/Users/stef/Documents/Seafile/dev/pyloggr_project/config"
    from pyloggr.config import set_configuration
    set_configuration(CONFIG_DIR)
    print(packer_group_with_condition.parseString(b))

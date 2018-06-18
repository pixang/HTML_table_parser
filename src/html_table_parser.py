#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import numbers
import collections

from extract_table_info import ExtractTableInfo

import numpy as np
from bs4 import BeautifulSoup, Tag
from bs4 import SoupStrainer
from pandas import Series, DataFrame
from pandas.errors import EmptyDataError
from pandas.io.common import (_is_url, urlopen,
                              parse_url, _validate_header_arg)
from pandas.compat import (lrange, lmap, u, string_types, iteritems,
                           binary_type)
import pandas as pd
from pandas.io.parsers import TextParser

char_types = string_types + (binary_type,)

_RE_WHITESPACE = re.compile(r'[\r\n]+|\s{2,}')


def _remove_whitespace(s, regex=_RE_WHITESPACE):
    return regex.sub(' ', s.strip())


def _read(obj):
    if _is_url(obj):
        with urlopen(obj) as url:
            text = url.read()
    elif hasattr(obj, 'read'):
        text = obj.read()
    elif isinstance(obj, char_types):
        text = obj
        try:
            if os.path.isfile(text):
                with open(text, 'rb') as f:
                    return f.read()
        except (TypeError, ValueError):
            pass
    else:
        raise TypeError("Cannot read object of type %r" % type(obj).__name__)
    return text


class HtmlTableParser(object):
    """Base class for parsers that parse HTML into DataFrames.
    """

    def __init__(self, io):
        self.io = io
        self._common_info = {}
        self._strainer = SoupStrainer('table')


    def parse_tables(self):
        html_body = self._build_doc()

        self._parse_common_info(html_body)

        tables = self._parse_tables(html_body)
        return (self._build_table(table) for table in tables)

    def get_common_info(self):
        return self._common_info

    # find table, filter no useful table, get table_name and table context unit
    def _parse_tables(self, doc):
        page = 0
        tables = []
        div_tag = doc.find('div')
        get_table_start = False

        for element in div_tag.next_siblings:
            if element.name == 'hr':
                page += 1

            if page <= 5:
                continue
            elif page > 5:
                if not get_table_start and isinstance(element, Tag):
                    if u'PART II' in element.get_text():
                        get_table_start = True

            if page > 25:
                if isinstance(element, Tag):
                    if u'PART III' in element.get_text():
                        break

            if isinstance(element, Tag):
                if element.find('table'):
                    table = element.find('table')
                    if table.find('tbody'):
                        tbody = table.find('tbody')
                        tr = tbody.find_all('tr')
                        if len(tr) == 1:
                            continue

                    dict = self.get_table_name_unit(element)

                    table['name'] = dict['table_name']
                    table['digit_change_unit'] = dict['digit_change_unit']
                    tables.append(element.find('table'))

        if not tables:
            raise ValueError("No useful tables found")

        return tables

    # process a table, structure the table info
    def _build_table(self, table):
        body = ExtractTableInfo(table).parse_raw_tbody()

        return body

    # get the info about doc, t
    def _parse_common_info(self, html_body):
        self._common_info['type'] = self.parse_type(html_body)
        self._common_info['company_name'] = self.parse_company_name(html_body)
        self._common_info['company_id'] = self.parse_company_id(html_body)
        self._common_info['date'] = self.parse_date(html_body)

    def parse_type(self, html_body):
        if html_body.find('type'):
            type_tag = html_body.find('type')
            return type_tag.stripped_strings
        elif html_body.find('description'):
            type_tag = html_body.find('description')
            return type_tag.stripped_strings
        else:
            ValueError('No type found')

    def parse_company_id(self, html_body):
        _RE_EIN = re.compile(r'[\s\S]*(\d{2}\s*\-\s*\d{5,10})[\s\S]*')
        _RE_NOT_EIN = re.compile(r'[\s\S]*\d{3}\s*\-\s*\d{5,10}[\s\S]*')

        div_tag = html_body.find('div')
        company_id = ''
        find = False
        for element in div_tag.next_siblings:
            if element.name == 'hr':
                break
            if isinstance(element, Tag):

                str = element.get_text()

                p = _RE_EIN.match(element.get_text())
                s = _RE_NOT_EIN.match(element.get_text())
                if p and not s:
                    company_id = p.group(1)
                    find = True
                    break
        if not find:
             raise ValueError("No company_id found")
        return company_id

    def parse_company_name(self, html_body):
        div_tag = html_body.find('div')
        p_tag = html_body.find('p')
        find = False
        for element in div_tag.next_siblings:
            if element.name == 'hr':
                break
            if isinstance(element, Tag):

                if u'Exact name of Registrant' in element.get_text():
                    previous_node = element.previous_sibling
                    company_name = previous_node.get_text()
                    find = True
                    break
        if not find:
            flag = 0
            add_flag = False
            for element in p_tag.next_siblings:
                if element.name == 'hr':
                    break
                if isinstance(element, Tag):

                    if u'Commission File Number' in element.get_text():
                        add_flag = True

                    if add_flag:
                        flag += 1
                    if flag == 3:
                        company_name = element.get_text()
                        find = True
                        break
        if not find:
            raise ValueError("No company_name found")
        return company_name

    def parse_date(self, html_body):

        _RE_DATE = re.compile(r'[\s\S]*([A-Z][a-z]+\s+\d{2}\s*,\s+\d{4})[\s\S]*')

        div_tag = html_body.find('div')
        find = False
        date = ''
        for element in div_tag.next_siblings:
            if element.name == 'hr':
                break
            if isinstance(element, Tag):

                string = element.get_text()
                string = string.replace(u'\xa0', u' ')

                p = _RE_DATE.match(string)
                if p:
                    date = p.group(1)
                    find = True
                    break

        if not find:
            raise ValueError("No date found")
        return date

    def get_table_name_unit(self, element):
        __PAREN_IN_HEADER = re.compile(r'[\s\S]*\((.+)\)[\s\S]*')
        _RE_PAGEINATION = re.compile(r'\d{2,3}')

        dict = {}
        dict['digit_change_unit'] = u''
        dict['table_name'] = u'table_name'

        possible_table_name = []
        possible_number = 3

        for node in element.previous_siblings:
            if isinstance(node, Tag):

                if node.name == 'hr':
                    break

                text = node.get_text()
                text = _remove_whitespace(text)
                s = __PAREN_IN_HEADER.match(text)
                if s:
                    if u'million' in s.group(1) and u'thousand' in s.group(1):
                        dict['digit_change_unit'] = u'million_thousand'
                    elif u'million' in s.group(1):
                        dict['digit_change_unit'] = u'million'
                    elif u'thousand' in s.group(1):
                        dict['digit_change_unit'] = u'thousand'
                    else:
                        dict['digit_change_unit'] = u''
                    continue

                if text != u'' and text != u'\xa0':
                    if possible_number > 0:
                        possible_table_name.append(text)
                        possible_number -= 1
                    else:
                        break
        possible_table_name.sort(key=lambda x: len(x))
        if len(possible_table_name) > 0:
            if len(possible_table_name[0]) > 80:
                dict['table_name'] = 'table_name'
            else:
                if _RE_PAGEINATION.match(possible_table_name[0]):
                    if not u'Inc' in possible_table_name[1] and not u'None.' in possible_table_name[1]:
                        dict['table_name'] = possible_table_name[1]
                    else:
                        dict['table_name'] = possible_table_name[2]
                else:
                    dict['table_name'] = possible_table_name[0]
        return dict

    def _setup_build_doc(self):
        raw_text = _read(self.io)
        if not raw_text:
            raise ValueError('No text parsed from document: {doc}'
                             .format(doc=self.io))
        return raw_text

    def _build_doc(self):
        return BeautifulSoup(self._setup_build_doc(), 'lxml')


def _expand_elements(body):
    lens = Series(lmap(len, body))
    lens_max = lens.max()
    not_max = lens[lens != lens_max]

    empty = ['']
    for ind, length in iteritems(not_max):
        body[ind] += empty * (lens_max - length)


def _data_to_frame(data):
    _expand_elements(data)
    tp = TextParser(data)
    df = tp.read()
    return df

#
# def parse(io):
#     p = _HtmlTableParser(io)
#     tables = p.parse_tables()
#
#     # ret = []
#     # for table in tables:
#     #     try:
#     #         # ret.append(_data_to_frame(table))
#     #         ret.append(table)
#     #     except EmptyDataError:
#     #         continue
#     return tables
# def get_common_info():
#         return self.common_info
#
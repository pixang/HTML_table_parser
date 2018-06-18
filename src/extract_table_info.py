
from bs4 import BeautifulSoup, Tag
import os
import re
import csv
import pandas as pd

import refine_table_info

_RE_WHITESPACE = re.compile(r'[\r\n]+|\s{2,}')
_RE_TEXT_INDENT = re.compile(r'.*\-left:(\d+\.?\d*)')
_RE_POSITIVE_NUMBER = re.compile('^\d+[,\d]*$')
_RE_NEGATIVE_NUMBER = re.compile('^\(\d+[,\d]*$')
_RE_POSITIVE_DECIMAL_NUMBER = re.compile('^\d+[.\d]+$')
_RE_NEGATIVE_DECIMAL_NUMBER = re.compile('^\(\d+[.\d]+$')
_RE_COLON_AT_END = re.compile(r'.*:$')
_RE_ANNONATATION = re.compile(r'\(\d\)')

def _remove_whitespace(s, regex=_RE_WHITESPACE):
    return regex.sub(' ', s.strip())


def _get_text_indent(s, regex=_RE_TEXT_INDENT):
    style = _remove_whitespace(s)
    p = regex.match(style)
    if p:
        return p.group(1)
    else:
        return 0

def _is_positive_number(cell_value, regex=_RE_POSITIVE_NUMBER):
    p = regex.match(cell_value)
    if p:
        return True
    else:
        return False

def _is_negative_number(cell_value, regex=_RE_NEGATIVE_NUMBER):
    p = regex.match(cell_value)
    if p:
        return True
    else:
        return False

def _is_positive_decimal_number(cell_value, regex=_RE_POSITIVE_DECIMAL_NUMBER):
    p = regex.match(cell_value)
    if p:
        return True
    else:
        return False

def _is_negative_decimal_number(cell_value, regex=_RE_NEGATIVE_DECIMAL_NUMBER):
    p = regex.match(cell_value)
    if p:
        return True
    else:
        return False

def _text_have_bold_attr(s):
    style = _remove_whitespace(s)
    if 'font-weight:bold' in style:
        return True
    else:
        return False

def _colon_at_end(s, regex=_RE_COLON_AT_END):
    p = regex.match(s)
    if p:
        return True
    else:
        return False

def _is_annotation(cell_value, regex=_RE_ANNONATATION):
    p = regex.match(cell_value)
    if p:
        return True
    else:
        return False


class ExtractTableInfo(object):

    def __init__(self, table):
        self._table = table
        self._output = []
        self.reserve_position = []
        self.col_sum = 0
        self.need_insert_row = False
        self.name = table['name']
        self.context_change_unit = table['digit_change_unit']
        self.up_left_change_unit = u''

    # parse entry
    def parse_raw_tbody(self):
        tbody = self._parse_tbody()

        try:
            res = self._parse_tr(tbody[0])
        except IndexError:
            res = self._parse_tr()

        return self._refine_table_info(self._parse_raw_data(res))

    # get the init structure of a table
    def _parse_raw_data(self, rows):
        data = []
        col_index = 0
        row_index = 0

        # data.append([_remove_whitespace(item) for item in
        #              self._text_getter(col) for col in td])
        # data.append([_remove_whitespace(self._text_getter(col, is_first_td)) for col in td])
        for row in rows:

            data_in_row = []
            td = self._parse_td(row)

            for col in td:
                for item in (self._text_getter(col, row_index, col_index)):
                    data_in_row.append(item)
                    col_index += 1

            col_index = 0
            row_index += 1


            # if tmp_str != u'':
            data.append(data_in_row)


        # consider rowspan
        for position in self.reserve_position:
            ins_row = position[0]
            ins_col = position[1]
            col_span = position[2]
            i = 0
            while col_span > 0:
                print 'ins_row:  ', ins_row
                print 'ins_col:  ', ins_col
                print 'data_len', len(data[ins_row])
                print 'data_len - 1', len(data[ins_row-1])

                # print 'data[ins_row - 1][ins_col]', data[ins_row - 1][ins_col + col_span - 1]['value']
                if ins_col == len(data[ins_row]):
                    data[ins_row].append(data[ins_row][ins_col-1])

                else:
                    data[ins_row].insert(ins_col, data[ins_row - 1][ins_col])
                col_span -= 1
                i += 1
        # data.insert(0, [u'' for n in range(self.col_sum)])
        df = pd.DataFrame(data)

        df.to_csv(os.path.join('/Users/stevehan/Desktop/' + 'Facebook.csv'), index=False, encoding='utf-8')

        filter_data = []
        for data_in_row in data:
            tmp_str = u''.join([(item['value']) for item in data_in_row
                                if not isinstance(item['value'], (int, float))])
            if tmp_str != u'':
                filter_data.append(data_in_row)

        #
        # for row in data:
        #     print row
        return filter_data

    # get text in a cell, include other useful info about the cell
    def _text_getter(self, obj, row_index, col_index):
        cell_info = {}
        # get the cell value
        text = []
        for string in obj.stripped_strings:
            text.append(string)


        cell_info['value'] = _remove_whitespace(u' '.join(text))
        if _is_annotation(cell_info['value']):
            cell_info['value'] = u''

        col_span = int(obj.get('colspan')) if obj.get('colspan') else 1
        row_span = int(obj.get('rowspan')) if obj.get('rowspan') else 1

        # remember the position for insert if row_span > 1
        while row_span > 1:
            self.reserve_position.append([row_index + row_span - 1, col_index, col_span])
            row_span -= 1

        if row_index == 0:
            if col_span == 1:
                self.need_insert_row = True
                self.col_sum += 1
            else:
                self.col_sum = col_span

        if col_index == 0:
            if obj.get('style'):
                # print obj.get('style')
                text_indent_td = _get_text_indent(obj.get('style'))
                # print text_indent_td
            else:
                # print obj.contents
                text_indent_td = 0
                # print text_indent_td

            if obj.find('p'):
                # print obj.p.get('style')
                text_indent_td_child = _get_text_indent(obj.p.get('style'))
                # print text_indent_td_child
            else:
                text_indent_td_child = 0
                # print text_indent_td_child
            cell_info['text_indent'] = max(text_indent_td_child, text_indent_td)

            if obj.find('font'):
                font_tag = obj.find('font')
                if font_tag.find('b'):
                    cell_info['is_bold'] = False
                elif font_tag.get('style'):
                    if _text_have_bold_attr(font_tag.get('style')):
                        cell_info['is_bold'] = True
                    else:
                        cell_info['is_bold'] = False
                else:
                    cell_info['is_bold'] = False

            elif obj.find('p'):
                p = obj.find('p')
                if _text_have_bold_attr(p.get('style')):
                    cell_info['is_bold'] = True
                else:
                    cell_info['is_bold'] = False

            if _colon_at_end(cell_info['value']):
                cell_info['colon_end'] = True
            else:
                cell_info['colon_end'] = False

        if col_index != 0:
            if _is_positive_number(cell_info['value']):

                # it is year in header
                if len(cell_info['value']) == 4:
                    cell_info['is_time'] = True
                else:
                    cell_info['value'] = int(cell_info['value'].replace(u',', u''))
                    cell_info['is_content'] = True

            elif _is_negative_number(cell_info['value']):
                tmp = cell_info['value'].replace(u',', u'')
                tmp = tmp.replace('(', u'')
                cell_info['value'] = -int(tmp)
                cell_info['is_content'] = True

            elif _is_positive_decimal_number(cell_info['value']):
                cell_info['is_content'] = True
                cell_info['value'] = float(cell_info['value'])

            elif _is_negative_decimal_number(cell_info['value']):
                cell_info['is_content'] = True
                tmp = cell_info['value'].replace('(', u'')
                cell_info['value'] = -float(tmp)

        return [cell_info for n in range(col_span)]


    def _refine_table_info(self, table_info):
        table_name = self.name
        self.up_left_change_unit = refine_table_info.get_up_left_unit(table_info)
        context_change_unit = self.context_change_unit
        up_left_change_unit = self.up_left_change_unit


        table_first_refine = refine_table_info.get_row_header_structure(table_info, table_name)

        main_data_at_column = refine_table_info.get_main_col_idx(table_first_refine)


        refine_table = refine_table_info.refine_table_content(table_first_refine, context_change_unit, up_left_change_unit)

        main_data_extract = refine_table_info.get_main_data(refine_table, main_data_at_column)

        return main_data_extract


    def _parse_td(self, row):
        return row.find_all(('td', 'th'))

    def _parse_tr(self, element):
        return element.find_all('tr')

    def _parse_tbody(self):
        return self._table.find_all('tbody')


def write_to_csv(data, path='/Users/stevehan/Desktop/'):
    with open(os.path.join(path, 'output.csv'), 'w') as csv_file:
        table_writer = csv.writer(csv_file)
        for row in data:
            for item in row:
                if isinstance(item, int):
                    item = str(item)
            table_writer.writerow([str(item.encode('utf-8')) for item in row])
    return


# from bs4 import BeautifulSoup
#
# soup = BeautifulSoup(open("/Users/stevehan/Desktop/Apple.htm"), "lxml")
#
# p = ExtractTableInfo(soup)
#
# table_data = p.parse_raw_tbody()
# for row in table_data:
#     print row

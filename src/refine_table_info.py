# -*- coding: utf-8 -*-

import os
import re
import numbers
import collections
from tqdm import tqdm


import numpy as np
from pandas import Series, DataFrame
from pandas.compat import (lmap, iteritems)
import pandas as pd
from pandas.io.parsers import TextParser

#
# data_path = '../data/output'
#
# table = pd.read_csv(os.path.join(data_path, 'MicroSoft_0.csv'))


# for item in table['0']:
#     print item
#
# print s


_RE_SHARE = re.compile(r'.*([S|s]hare).*')
_RE_UNIT = re.compile(r'.*[U|u]nit.*')

def is_share_in(s, reguar=_RE_SHARE):
    if reguar.match(s):
        return True
    else:
        return False

def is_unit_in(s, reguar = _RE_UNIT):
    if reguar.match(s):
        return True
    else:
        return False

def write_to_csv(table_info):
    df = pd.DataFrame(table_info)
    df.to_csv(os.path.join('/Users/stevehan/Desktop/' + 'damn.csv'), index=False, encoding='utf-8')




def get_up_left_unit(table_info):
    unit_for_whole_table = u''
    for item in table_info:
        first_col_val = item[0]['value']
        if u':' in first_col_val:
            item[0]['value'] = item[0]['value'].replace(u':', u'')
        if first_col_val == u'':
            continue
        elif u'million' in first_col_val.strip() or u'thousand' in first_col_val.strip():
            unit_for_whole_table = u'million_thousand'
            item[0]['value'] = u''
            break
        elif u'million' in first_col_val.strip():
            unit_for_whole_table = u'million'
            item[0]['value'] = u''
            break
        elif u'thousand' in first_col_val.strip():
            unit_for_whole_table = u'thousand'
            item[0]['value'] = u''
            break
        elif u'Continued' in first_col_val.strip():
            item[0]['value'] = u''
        else:
            unit_for_whole_table = u''
    return unit_for_whole_table

def get_local_unit_change(table_info, main_col_idx):
    for row in table_info:
        if row[main_col_idx].has_key('is_header'):

            if isinstance(row[main_col_idx]['value'], int) or isinstance(row[main_col_idx]['value'], float):
                 break
            if u'million' in row[main_col_idx]['value'] and u'thousand' in row[main_col_idx]['value']:
                local_unit_change = u'million_thousand'

            elif u'million' in row[main_col_idx]['value']:
                local_unit_change = u'million'

            elif u'thousand' in row[main_col_idx]['value']:
                local_unit_change = u'thousand'
            else:
                local_unit_change = u''
            return local_unit_change
        else:
            break




# row header structure
def get_row_header_structure(table_info, table_name):
    s = set()

    write_to_csv(table_info)
    for item in table_info:
        s.add(item[0]['text_indent'])
    # print s

    text_indent = list(s)
    text_indent = map(float, text_indent)
    text_indent.sort()

    # print text_indent

    bold_font_in_header = False
    sub_title_in_header = False
    sub_title_name = u''
    sub_title_position = -1
    tmp_title= u''
    # get the rank of header
    for row_idx, row in enumerate(table_info):

        #   需要修改
        if row[0]['value'] == u'' or (row[0]['value'] != u'' and row_idx == 0):
            for col in row:
                col['is_header'] = True
        
            continue

        if row_idx < len(table_info)-1:
            if row[0]['text_indent'] == table_info[row_idx+1][0]['text_indent']:
                is_blank_row = True

                for col_idx, col in enumerate(row):
                    if col_idx == 0 or row_idx == 0:
                        continue
                    if col['value'] != u'':
                        col['is_content'] = True
                        is_blank_row = False

                if is_blank_row and text_indent.index(float(row[0]['text_indent'])) == 0 and not (u'(Note 7)' in row[0]['value']) \
                        and not (u'(Note 14)' in row[0]['value']):
                    row[0]['rank'] = -1
                    row[0]['parent_name'] = table_name
                    row[0]['parent_position'] = -1

                    sub_title_in_header = True
                    sub_title_name = row[0]['value']
                    sub_title_position = row_idx
                    continue

                # not blank row but the font is bold and the text indent rank is 0
                if not is_blank_row and row[0]['is_bold'] and text_indent.index(float(row[0]['text_indent'])) == 0:
                    row[0]['rank'] = -1
                    row[0]['parent_name'] = table_name
                    row[0]['parent_position'] = -1

                    sub_title_in_header = True
                    sub_title_name = row[0]['value']
                    sub_title_position = row_idx

                    continue


        row[0]['rank'] = text_indent.index(float(row[0]['text_indent']))



        if sub_title_in_header:
            if row[0]['rank'] == 0:
                row[0]['parent_name'] = sub_title_name
                row[0]['parent_position'] = sub_title_position

                continue

        else:
            if row[0]['rank'] == 0:
                row[0]['parent_name'] = table_name
                row[0]['parent_position'] = -1

                continue

        # Blank row hava be deleted, so it should not exist a row with data but no row_eader,
        # every row should hava a rank key
        if not table_info[row_idx - 1][0].has_key('rank'):
            continue
        if not table_info[row_idx - 1][0].has_key('parent_name') or not table_info[row_idx - 1][0].has_key('parent-position'):
            break

        if u'Total' in row[0]['value']:
            if u'Total' in table_info[row_idx - 1][0]['value']:
                row[0]['parent_name'] = sub_title_name
                row[0]['parent_position'] = sub_title_position

            else:
                row[0]['parent_name'] = table_info[row_idx - 1][0]['parent_name']
                row[0]['parent_position'] = table_info[row_idx-1][0]['parent_position']

        elif row[0]['rank'] == table_info[row_idx-1][0]['rank']:
            row[0]['parent_name'] = table_info[row_idx-1][0]['parent_name']
            row[0]['parent_position'] = table_info[row_idx-1][0]['parent_position']

        elif (row[0]['rank'] - table_info[row_idx-1][0]['rank']) >= 1:
            row[0]['parent_name'] = table_info[row_idx-1][0]['value']
            row[0]['parent_position'] = row_idx - 1

        elif table_info[row_idx-1][0]['rank'] - row[0]['rank'] >= 1:
            current_row_idx = row_idx
            while current_row_idx > 0:
                if row[0]['rank'] == table_info[current_row_idx-1][0]['rank']:
                    row[0]['parent_name'] = table_info[current_row_idx-1][0]['parent_name']
                    row[0]['parent_position'] = table_info[current_row_idx-1][0]['parent_position']

                    break
                current_row_idx -= 1

    # for item in table_info:
    #     print item
    # write_to_csv(table_info)

    return table_info



def get_row_header_name(table_info, row_idx):
    current_row_idx = row_idx
    row_header = []
    while True:
         row_header.insert(0,table_info[current_row_idx][0]['value'])

         if not table_info[current_row_idx][0].has_key('parent_position'):
             break
         if table_info[current_row_idx][0]['parent_position'] == -1:
             row_header.insert(0, table_info[current_row_idx][0]['parent_name'])
             break

         current_row_idx = table_info[current_row_idx][0]['parent_position']

    return u'.'.join(row_header)


def get_column_header_name(table_info, col_idx):
    column_header = []

    for row_idx in range(3):
        if table_info[row_idx][col_idx].has_key('is_header'):
            column_header.append(table_info[row_idx][col_idx]['value'])

    column_header_name = u'.'.join(column_header)

    return column_header_name

def refine_table_content(table_info, context_change_unit, up_left_change_unit):
    main_col = get_main_col_idx(table_info)


    for main_col_idx in main_col:

        local_unit_change = get_local_unit_change(table_info, main_col_idx)

        for row_idx, row in enumerate(table_info):

            if row[main_col_idx].has_key('is_content'):

                # 列名称 表名称
                column_name = get_column_header_name(table_info, main_col_idx)
                row_name = get_row_header_name(table_info, row_idx)
                row[main_col_idx]['column'] = column_name
                row[main_col_idx]['row'] = row_name

                # print isinstance(row[main_col_idx]['value'], int) or isinstance(row[main_col_idx]['value'], float)
                if isinstance(row[main_col_idx]['value'], int) or isinstance(row[main_col_idx]['value'], float):
                    if row[main_col_idx + 1]['value'] == u'%':
                        row[main_col_idx]['unit'] = 'percentage'

                    elif row[main_col_idx-1]['value'] == u'$':
                        row[main_col_idx]['unit'] = u'$'
                        tmp = row[main_col_idx]['value']
                        row[main_col_idx]['value'] = get_real_number(tmp, local_unit_change, up_left_change_unit,
                                                                     context_change_unit)
                    elif is_share_in(row_name) or is_share_in(column_name):
                        row[main_col_idx]['unit'] = u'share'
                        tmp = row[main_col_idx]['value']
                        row[main_col_idx]['value'] = get_other_real_number(tmp, local_unit_change, up_left_change_unit, context_change_unit)
                    elif is_unit_in(row_name) or is_unit_in(column_name):
                        row[main_col_idx]['unit'] = u'unit'
                        tmp = row[main_col_idx]['value']
                        row[main_col_idx]['value'] = get_other_real_number(tmp, local_unit_change, up_left_change_unit, context_change_unit)
                    elif row[main_col_idx-1]['value'] == row[main_col_idx]['value']:
                        row[main_col_idx]['unit'] = u'$'
                        tmp = row[main_col_idx]['value']
                        row[main_col_idx]['value'] = get_real_number(tmp, local_unit_change, up_left_change_unit, context_change_unit)

            elif row[main_col_idx]['value'] == u'-':
                row[main_col_idx]['is_content'] = True
                row[main_col_idx]['value'] = row[main_col_idx-1]['value'] + row[main_col_idx]['value'] + row[main_col_idx+1]['value']
                row[main_col_idx]['unit'] = 'N/A'
            else:
                row[main_col_idx]['unit'] = 'N/A'
    return table_info


def get_main_col_idx(table_info):
    main_col_idx = []
    head_last_line = -1
    for row_idx in range(4):
        if row_idx > len(table_info) - 1:
            break
        for col in table_info[row_idx]:
            if col.has_key('is_header'):
                head_last_line = row_idx
                break
    for col_idx, col in enumerate(table_info[head_last_line]):
        # print len(table_info[head_last_line]) -2 ,  col_idx
        if col_idx == 0 or (col_idx == len(table_info[head_last_line]) - 1):

            continue
        # print col['value'], table_info[head_last_line][col_idx-1]
        # print col['value'] == table_info[head_last_line][col_idx-1]['value']
        if col['value'] == table_info[head_last_line][col_idx-1]['value'] and col['value'] == table_info[head_last_line][col_idx+1]['value']:
            main_col_idx.append(col_idx)

        elif col['value'] == table_info[head_last_line][col_idx-1]['value'] and col['value'] != table_info[head_last_line][col_idx + 1]['value'] and col['value'] != table_info[head_last_line][col_idx-2]['value']:
            for i in range(3):
                if i > len(table_info) - 2 - head_last_line:
                    break
                if table_info[head_last_line + i + 1][col_idx].has_key('is_content'):
                    main_col_idx.append(col_idx)
                elif table_info[head_last_line + i + 1][col_idx -1].has_key('is_content'):
                    main_col_idx.append(col_idx -1)

        elif col['value'] != u'' and table_info[head_last_line][col_idx+1]['value'] ==u'' and (table_info[head_last_line][col_idx-1]['value'] ==u'' or col_idx -1 ==0 ):
            main_col_idx.append(col_idx)

    # for item in main_col_idx:
    #     print item

    return main_col_idx


def get_real_number(number ,local_unit_change,up_left_change_unit,context_change_unit):
    if local_unit_change != u'':
        if local_unit_change == 'million' or local_unit_change == 'million_thousand':
            number *= 1000000
        elif local_unit_change == 'thousand':
            number *= 1000
    elif up_left_change_unit != u'':
        if up_left_change_unit == 'million' or up_left_change_unit == 'million_thousand':
            number *= 1000000
        elif up_left_change_unit == 'thousand':
            number *= 1000
    elif context_change_unit != u'':
        if context_change_unit == 'million' or context_change_unit == 'million_thousand':
            number *= 1000000
        elif context_change_unit == 'thousand':
            number *= 1000
    return number


def get_other_real_number(number, local_unit_change, up_left_change_unit, context_change_unit):
    if local_unit_change != u'':
        if local_unit_change == 'million_thousand':
            number *= 1000
    elif up_left_change_unit != u'':
        if up_left_change_unit == 'million_thousand':
            number *= 1000
    elif context_change_unit != u'':
        if context_change_unit == 'million_thousand':
            number *= 1000
    return number

def get_main_data(refine_table, main_data_at_column):
    cell_data = []
    for main_col_idx in main_data_at_column:
        for row_idx, row in enumerate(refine_table):
            if row[main_col_idx].has_key('is_content'):
                del row[main_col_idx]['is_content']
                cell_data.append(row[main_col_idx])

    return cell_data





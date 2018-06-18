import os
import re
from tqdm import tqdm
import json
from html_table_parser import HtmlTableParser



data_path = '../data'
out_path = '../output'

input_path = os.path.join(data_path, 'input')
out_path = os.path.join(data_path, 'output')

for file_name in tqdm(sorted(os.listdir(input_path))):
    if re.match(r'\..*', file_name):
        continue
    print file_name
    parser = HtmlTableParser(input_path + '/' + file_name)


    tables = parser.parse_tables()
    common_info = parser.get_common_info()

    data = [table for table in tables if table != []]
    # json_data = {}
    # for item in data:
    #     json_data['table'] = table
    #     json_data['common_info'] = common_info
    #     data_string = json.dumps(json_data)
    #     print data_string
    # #
    for d in data:
        data_string = json.dumps(d)
        print data_string




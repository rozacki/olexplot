import regex
from datetime import datetime
from decimal import *

regex_pattern_position_time = regex.compile(r'\d+\.\d+ -\d+\.\d+ \d+ [a-zA-Z]+')
getcontext().prec = 10


def _line_to_scv(line):
    '''
    Convert space separated to comman separated
    convert from WGS84 format to lat and long
    convert unix epoch to datetime
    :param line:
    :return:
    '''
    splits = line.split(' ')
    splits.append('\n')
    # convert from WGS84 format to lat and long
    splits[0] = str(Decimal(splits[0]) / Decimal(60))
    splits[1] = str(Decimal(splits[1]) / Decimal(60))
    # convert unix epoch to datetime
    splits[2] = str(datetime.fromtimestamp(int(splits[2])))
    return ','.join(splits)


def convert():
    '''
    Produce such format:
    lat,long,time,boat
    :return:
    '''
    with open('data/olexplot', encoding='cp1252') as source:
        with open('data/olexplot_prep.csv', 'w') as target:
            target.write('lat,long,time,boat,\n')
            with open('data/olexplot_meta.csv', 'w') as meta:
                meta_line = []
                for line in source:
                    if not line:
                        continue
                    line = line.replace('\n', '')
                    if regex_pattern_position_time.match(line):

                        if len(meta_line):
                            meta_line.append('\n')
                            meta.write(','.join(meta_line))
                            meta_line.clear()

                        target.write(_line_to_scv(line))
                    else:
                        meta_line.append(line)

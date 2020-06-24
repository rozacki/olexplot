import regex
from decimal import *
from datetime import datetime

from convert_to_csv import regex_pattern_position_time


regex_pattern_route_name = regex.compile(r'Navn [a-zA-Z 0-9\.]+')
regex_pattern_route_type = regex.compile(r'Rute [a-zA-Z ]+')


def _convert_line_to_dict(line):
    '''
    Convert space separated to dictinoary
    convert from WGS84 format to lat and long
    convert unix epoch to datetime
    :param line:
    :return:
    '''
    splits = line.split(' ')
    # convert from WGS84 format to lat and long
    splits[0] = str(Decimal(splits[0]) / Decimal(60))
    splits[1] = str(Decimal(splits[1]) / Decimal(60))
    # convert unix epoch to datetime
    splits[2] = str(datetime.fromtimestamp(int(splits[2])))
    return {'lat': splits[0], 'long': splits[1], 'time': splits[2], 'cursor': splits[3]}


class OlexplotChunkReader:

    def __init__(self, file_name):
        '''
        open file and skip first two lines
        :param file_name:
        :return:
        '''
        self._file = open(file_name, encoding='cp1252')
        self._file.readline()
        self._file.readline()

    def __del__(self):
        self._file.close()

    def __next__(self):
        chunk = []
        for i, line in enumerate(self._file):
            if line == '\n':
                return chunk
            line = line.replace('\n', '')
            chunk.append(line)
        if len(chunk) > 0:
            return chunk
        raise StopIteration()

    def __iter__(self):
        return self


class OlexplotRouteReader:
    def __init__(self, file_name):
        self._name = None
        self._type = None
        self._no_name_counter = 0
        self._route = dict()
        self.chunk_reader = OlexplotChunkReader(file_name)

    def __next__(self):
        lines = next(self.chunk_reader)
        chunk = dict()
        chunk['time_and_position'] = list()
        for line in lines:
            if regex_pattern_position_time.match(line):
                chunk['time_and_position'].append(_convert_line_to_dict(line))
            elif regex_pattern_route_name.match(line):
                chunk['name'] = line
            elif regex_pattern_route_type.match(line):
                chunk['type'] = line

        if not chunk.get('name'):
            chunk['name'] = str(self._no_name_counter)
            self._no_name_counter = self._no_name_counter + 1

        if not chunk.get('type'):
            chunk['type'] = 'missing type'

        return chunk

    def __iter__(self):
        return self


def _sanitize_postgres_column(column):
    return '\'' + column.replace('\'', "''") + '\''


def store_chunks_in_csv(reader, output='data/olexplot.csv'):
    with open(output,'w') as olexplot_csv:
        olexplot_csv.write('long,lat,time,cursor,type,name\n')
        for chunk in reader:
            for row in chunk['time_and_position']:
                l = [row['long'], row['lat'],
                     row['time'],
                     _sanitize_postgres_column(row["cursor"]),
                     _sanitize_postgres_column(chunk["type"]),
                     _sanitize_postgres_column(chunk["name"])]
                olexplot_csv.write(','.join(l))
                olexplot_csv.write('\n')


# test - comment if not needed
reader = OlexplotRouteReader('tests/data/test_olexplot')
store_chunks_in_csv(reader, output='tests/data/olexplot.csv')

# prod - comment if not needed
#reader = OlexplotRouteReader('data/olexplot')
#store_chunks_in_csv(reader, output='data/olexplot.csv')


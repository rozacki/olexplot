import os
import psycopg2

from geojson import Point, Feature, FeatureCollection, dumps
import jsonstreams


connection_string = 'host=\'{}\' dbname=\'{}\' user=\'{}\''


def _get_partition_info(user='postgres', host='localhost', database_name='olexplot', table_name='olexplot',
                        partition_by='year', year=None, month=None):
    '''
    Data is in database
    :param user:
    :param host:
    :param database_name:
    :param table_name:
    :param partition_by:
    :return:
    '''
    partitions = []
    with psycopg2.connect(connection_string.format(host,database_name,user)) as conn:
        with conn.cursor() as cur:
            if partition_by == 'year':
                cur.execute(f'select count(*),date_part(\'year\', time) as year from {table_name} '
                            f'group by date_part(\'year\', time) order by date_part(\'year\', time)')
            elif partition_by == 'year month':
                cur.execute(f'select count(*),date_part(\'year\', time) as year, date_part(\'month\',time) '
                         f'as month from {table_name} where date_part(\'year\', time)={year} '
                            f'group by date_part(\'year\', time), date_part(\'month\', time) '
                         f'order by date_part(\'year\', time), date_part(\'month\', time)')
            elif partition_by == 'year month day':
                cur.execute(f'select count(*),'
                            f'date_part(\'year\', time) as year, '
                            f'date_part(\'month\',time) as month, '
                            f'date_part(\'day\',time) as day '
                            f'from {table_name} where date_part(\'year\', time)={year} '
                            f'and date_part(\'month\',time)={month} '
                            f'group by date_part(\'year\', time), date_part(\'month\', time), date_part(\'day\', time)'
                            f'order by date_part(\'year\', time), date_part(\'month\', time),'
                            f' date_part(\'day\', time)')
            else:
                raise Exception(f'partition by {partition_by} not supported')
            for row in cur.fetchall():
                # count, year, month
                partitions.append(row)
    cur.close()
    return partitions


class GeoJsonWriter:
    '''
    {
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          0.0,
          1.1
        ]
      },
      "properties": {
        "time": "2015-12-16 15:39:10",
        "cursor": "Brunsirkel",
        "type": "Rute uten navn",
        "name": "0"
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          2.1,
          3.1
        ]
      },
      "properties": {
        "time": "2016-01-13 15:13:55",
        "cursor": "Sp\u00f8rsm\u00e5l",
        "type": "Rute uten navn",
        "name": "1"
      }
    },
    '''
    def __init__(self):
        self.geojson_file = None
        self.features_array = None

    def open(self, file_path):
        self.geojson_file = jsonstreams.Stream(jsonstreams.Type.object, filename=file_path)
        self.geojson_file.write("type", "FeatureCollection")
        self.features_array = self.geojson_file.subarray('features')

    def add_feature(self, long, lat, time, cursor, type_, name):
        with self.features_array.subobject() as feature:
            feature.write('type', 'Feature')
            with feature.subobject('geometry') as geometry:
                geometry.write('type', 'Point')
                with geometry.subarray('coordinates') as coordinates:
                    coordinates.write(long)
                    coordinates.write(lat)
            with feature.subobject('properties') as properties:
                properties.write('time', str(time))
                properties.write('cursor', cursor)
                properties.write('type', type_)
                properties.write('name', name)

    def close(self):
        self.features_array.close()
        self.geojson_file.close()


def export_year_month_partitions_into_files(partitions_info, user='postgres', host='localhost', database_name='olexplot',
                                            table_name='olexplot', sample=False):
    geojson_writer = GeoJsonWriter()
    with psycopg2.connect(connection_string.format(host, database_name, user)) as conn:
        for partition in partitions_info:
            year = partition[1]
            month = partition[2]

            geojson_writer.open(os.path.join('data', 'partitions', f'{int(year)}_{int(month):02d}.json'))
            with conn.cursor() as cur:
                if sample:
                    cur.execute(f'select * from {table_name} where date_part(\'year\', time)={year} '
                                f'and date_part(\'month\', time)={month} limit 20')
                else:
                    cur.execute(f'select * from {table_name} where date_part(\'year\', time)={year} '
                            f'and date_part(\'month\', time)={month}')

                for row in cur.fetchall():
                    geojson_writer.add_feature(row[0], row[1], row[2], row[3], row[4], row[5])

            geojson_writer.close()


def _prepare_fs_folders(year, month, day):
    partitions_folder = 'data/partitions'

    if day:
        os.makedirs(os.path.join(partitions_folder, str(year), str(month)), exist_ok=True)
        file_name = os.path.join(partitions_folder, str(year), str(month), str(day))
    elif month:
        os.makedirs(os.path.join(partitions_folder, str(year)), exist_ok=True)
        file_name = os.path.join(partitions_folder, str(year), str(month))
    elif year:
        os.makedirs(os.path.join(partitions_folder), exist_ok=True)
        file_name = os.path.join(partitions_folder, str(year))
    file_name = file_name + '.json'
    return file_name


def export_year_partitions_into_files(partitions_info, user='postgres', host='localhost', database_name='olexplot',
                                            table_name='olexplot', partition_by='year'):
    geojson_writer = GeoJsonWriter()
    parts = partition_by.split()
    partition_by_year = True if 'year' in parts else False
    partition_by_month = True if 'month' in parts else False
    partition_by_day = True if 'day' in parts else False
    with psycopg2.connect(connection_string.format(host, database_name, user)) as conn:
        for partition_info in partitions_info:
            # these indexes are fixed
            count = partition_info[0]
            year = partition_info[1] if partition_by_year else ''
            month = partition_info[2] if partition_by_month else ''
            day = partition_info[3] if partition_by_day else ''

            file_name = _prepare_fs_folders(year, month, day)
            geojson_writer.open(file_name)
            with conn.cursor() as cur:
                if partition_by_day:
                    cur.execute(f'select * from {table_name} where '
                                f'date_part(\'year\', time)={year} and date_part(\'month\', time)={month} '
                                f'and date_part(\'day\',time)={day}')
                elif partition_by_month:
                    cur.execute(f'select * from {table_name} where '
                                f'date_part(\'year\', time)={year} and date_part(\'month\', time)={month}')
                elif partition_by_year:
                    cur.execute(f'select * from {table_name} where date_part(\'year\', time)={year}')

                for row in cur.fetchall():
                    geojson_writer.add_feature(row[0], row[1], row[2], row[3], row[4], row[5])

            geojson_writer.close()


partition_by = 'year month day'
partitions_info = _get_partition_info(partition_by=partition_by, year=2020, month=1)
export_year_partitions_into_files(partitions_info, partition_by=partition_by)

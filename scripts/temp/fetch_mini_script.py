import requests
import pandas as pd
import io
import os
import sys
def csv_parser(csv_frame):


    data_column_renamer = {
        'Timestamp':'timestamp',
        'Variable':'variable',
        'Units':'units',
        'Value':'value',
        'Sensor Centroid Longitude':'lon',
        'Sensor Centroid Latitude':'lat',
        'Raw ID':'sensor_id',
        'Sensor Name':'sensor_name'
    }

    return csv_frame[data_column_renamer.keys()].rename(columns=data_column_renamer)

var_list = [
'Humidity',
'NO',
'PM1',
'NO2',
'CO',
'PM10',
'Temperature',
'PM 4',
'PM2.5',
'Pressure',
'O3',
'Sound'


]

api_url = 'http://uoweb3.ncl.ac.uk/api/v1.1/sensors/json/'

params = {
    'broker': 'aq_mesh_api,Emote Air Quality Sensor',
}

r = requests.get(url=api_url,params=params)
sensor_json = r.json()
sensor_name , year, month = sys.argv[1:]

for var_name in var_list:
    url = 'http://uoweb3.ncl.ac.uk/api/v1.1/sensors/{sensor_name}/data/cached/{variable}/{year}/{month}/csv/'.format(
        sensor_name=sensor_name,
        variable=var_name,
        year=year,
        month=month
    )

    csv_file_name = '{sensor_name}-{variable}-{year}-{month}.csv'.format(
        sensor_name=sensor_name,
        variable=var_name,
        year=year,
        month=month
    )

    csv_file_path = '/obs_data/ncl_archive/{file_name}'.format(file_name=csv_file_name)

    if os.path.exists(csv_file_path):
        continue

    r = requests.get(url)

    if r.status_code != 404:

        print(r.url)

        data = r.content.decode('utf8')
        cleaned_csv = csv_parser(pd.read_csv(io.StringIO(data)))

        cleaned_csv['observatory'] = 'ncl'

        cleaned_csv.to_csv(csv_file_path,index=False)


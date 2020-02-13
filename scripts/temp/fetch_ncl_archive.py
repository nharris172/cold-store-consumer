import requests
import pandas as pd
import io
import os
import itertools
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
def mygrouper(n, iterable):
     args = [iter(iterable)] * n
     return ([e for e in t if e != None] for t in itertools.zip_longest(*args))
api_url = 'http://uoweb3.ncl.ac.uk/api/v1.1/sensors/json/'

params = {
    'broker': 'aq_mesh_api,Emote Air Quality Sensor',
}

r = requests.get(url=api_url,params=params)
sensor_json = r.json()
year = 2019

commands = []

for month in range(1,13):
    month_frames = []
    for sensor in sensor_json['sensors']:
        print(sensor)
        print(sensor['Sensor Name'])
        command = 'python3 /scripts/fetch_mini_script.py {sensor_name} {year} {month}'.format(
            sensor_name=sensor['Sensor Name'],
            year=year,
            month=month
        )
        commands.append(command)

for sub_commands in mygrouper(16,commands):
    print(' & '.join(sub_commands))
    os.system(' & '.join(sub_commands))



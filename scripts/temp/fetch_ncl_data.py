import requests
import datetime
import pandas as pd
import io
import math

import os

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


def get_ncl_data(starttime,endtime):
    api_url = 'http://uoweb3.ncl.ac.uk/api/v1.1/sensors/data/csv/'

    params = {
        'broker': 'aq_mesh_api,Emote Air Quality Sensor',
        'starttime': starttime.strftime("%Y%m%d"),
        'endtime': endtime.strftime("%Y%m%d")
    }

    r = requests.get(api_url, params)
    data = r.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))



    cleaned_csv = csv_parser(df)

    cleaned_csv['observatory'] = 'ncl'
    return cleaned_csv


END_PERIOD = datetime.datetime.combine(
    (datetime.datetime.now() +datetime.timedelta(days=1)  ).date(),
    datetime.time(0,0)
)

START_PERIOD = END_PERIOD - datetime.timedelta(days=90)

TIME_DELTA = datetime.timedelta(days=1)

steps = int(math.ceil((END_PERIOD - START_PERIOD).total_seconds()/TIME_DELTA.total_seconds()))

for step in range(steps):
    starttime = START_PERIOD + step*TIME_DELTA
    endtime = START_PERIOD + (step+1)*TIME_DELTA
    print(starttime,endtime)


    csv_name = '{obs}-{start}-{end}.csv'.format(
            start=starttime.strftime("%Y%m%d"),
            end=endtime.strftime("%Y%m%d"),
            obs='ncl'

        )

    csv_file_path = '/obs_data/{file_name}'.format(file_name=csv_name)

    if os.path.exists(csv_file_path):
        continue

    cleaned_csv = get_ncl_data(starttime,endtime)

    cleaned_csv.to_csv(csv_file_path)
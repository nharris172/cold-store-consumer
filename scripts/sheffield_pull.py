import requests
import datetime
import os
import math
import pandas as pd

BROKERS = 'AMfixed,eWatch'


def json_to_frame(data_json):
    csv_rows = []
    for sensor in data_json['sensors']:
        print(sensor)
        for var_name,rows in sensor['data'].items():
            for row in rows:

                value = row['Value']
                units = row['Units']
                sensor_name = row['Sensor Name']
                variable = row['Variable']
                dt = datetime.datetime(1970,1,1) + datetime.timedelta(seconds=int(row['Timestamp']))
                lon = sensor['Sensor Centroid Longitude']["0"]
                lat = sensor['Sensor Centroid Latitude']["0"]
                raw_id = sensor["Raw ID"]["0"]
                csv_row = [
                    dt,
                    variable,
                    units,
                    value,
                    lon,
                    lat,
                    raw_id,
                    sensor_name,
                    'sheff'

                ]
                csv_rows.append(csv_row)
    headers = [
        'timestamp',
                   'variable',
    'units',
    'value',
    'lon',
    'lat',
    'sensor_id',
    'sensor_name',
    'observatory'

    ]
    return pd.DataFrame(csv_rows,columns=headers)

def treat_sheffield_output(data_string):

    raw_data = data_string.split("\n", 8)[8]
    contents = {"sensors": []}
    if len(raw_data) < 1:
        return contents



    cnt_sensor = -1

    seen_data = True


    for line in raw_data.splitlines():
        if line.strip() and not line.startswith('# Begin CSV table for pair') and not line.startswith(
                '# End CSV table for pair'):

            if line.startswith('#'):

                if seen_data:
                    cnt_sensor += 1

                    contents['sensors'].append(
                        {"Location (WKT)": {"0": ""}, "Third Party": {"0": "false"}, "Sensor Name": {"0": ""},
                         "Sensor Height Above Ground": {"0": ""}, "Raw ID": {"0": ""}, "data": {},
                         'Sensor Centroid Longitude':{"0":""},'Sensor Centroid Latitude':{"0":""},
                         })

                    seen_data = False

                if line.startswith('# site.longitude:'):

                    contents['sensors'][cnt_sensor]['Location (WKT)']['0'] = 'POINT (' + line.split(' ')[2] + ' '
                    contents['sensors'][cnt_sensor]['Sensor Centroid Longitude']['0'] = line.split(' ')[2]

                elif line.startswith('# site.latitude:'):

                    contents['sensors'][cnt_sensor]['Location (WKT)']['0'] += line.split(' ')[2] + ')'
                    contents['sensors'][cnt_sensor]['Sensor Centroid Latitude']['0'] = line.split(' ')[2]

                elif line.startswith('# sensor.id:'):

                    contents['sensors'][cnt_sensor]['Sensor Name']['0'] = contents['sensors'][cnt_sensor]['Raw ID'][
                        '0'] = line.split(' ')[2]

                elif line.startswith('# sensor.heightAboveGround:'):

                    contents['sensors'][cnt_sensor]['Sensor Height Above Ground']['0'] = float(line.split(' ')[2])

                elif line.startswith('# Column_'):

                    key = line.split(' ')[3].split('~')[1]

                    if key != 'time' and key != 'sensor':
                        contents['sensors'][cnt_sensor]['data'][key] = []
            else:

                if not seen_data: seen_data = True
                dat = list(contents['sensors'][cnt_sensor]['data'].keys())

                entries = line.split(',')

                for entry in entries[2:]:
                    contents['sensors'][cnt_sensor]['data'][dat[entries.index(entry) - 2]].append({

                        "Variable": dat[entries.index(entry) - 2],

                        "Units": "-",

                        "Sensor Name": entries[1],

                        "Timestamp": entries[0],

                        "Value": entry,

                        "Flagged as Suspect Reading": 'false'

                    })

    return contents


def get_sheffield_data(start,end):
    url = 'https://sheffield-portal.urbanflows.ac.uk/uflobin/ufdex'

    params = {
        #2020-02-11T10:47:33''

        'Tfrom':start.strftime('%Y-%m-%dT%H:%M:%S'),
        'Tto':end.strftime('%Y-%m-%dT%H:%M:%S'),
        'aktion':'CSV_show',
        'byFamily':BROKERS,
        'freqInMin':1,
        'tok':'generic'

    }

    r = requests.get(url,params)
    data = r.content.decode('utf8')

    return json_to_frame(treat_sheffield_output(data))


END_PERIOD = datetime.datetime.combine(
    (datetime.datetime.now() +datetime.timedelta(days=1)  ).date(),
    datetime.time(0,0)
)

START_PERIOD = END_PERIOD - datetime.timedelta(days=90)

TIME_DELTA = datetime.timedelta(days=1)

steps = int(math.ceil((END_PERIOD - START_PERIOD).total_seconds()/TIME_DELTA.total_seconds()))

for starttime,endtime in [[START_PERIOD + step*TIME_DELTA,START_PERIOD + (step+1)*TIME_DELTA] for step in range(steps)][::-1]:
    print(starttime,endtime)


    csv_name = '{obs}-{start}-{end}.csv'.format(
            start=starttime.strftime("%Y%m%d"),
            end=endtime.strftime("%Y%m%d"),
            obs='sheff'

        )

    csv_file_path = '/obs_data/{file_name}'.format(file_name=csv_name)

    if os.path.exists(csv_file_path):
        continue

    cleaned_csv = get_sheffield_data(starttime,endtime)

    cleaned_csv.to_csv(csv_file_path, index=False)
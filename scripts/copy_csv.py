import psycopg2
from pathlib import Path
import pandas as pd

con = psycopg2.connect("dbname='cold_store_db' user='postgres' host='cold_store_db' password='your_password'")
cur = con.cursor()
for filename in Path('/obs_data/').rglob('*.csv'):
    print(filename)

    frame = pd.read_csv(filename).head(1)
    row_json = frame.to_dict(orient='rows')[0]
    query = """select count(*) from sensor_data where timestamp = '{timestamp}'
     and  sensor_id = '{sensor_id}' 
      and variable = '{variable}' and observatory = '{obs}'""".format(
        sensor_id=row_json['sensor_id'],
        timestamp=row_json['timestamp'],
        variable=row_json['variable'],
        obs=row_json.get('observatory','')

    )

    cur.execute(query)
    if cur.fetchone()[0] > 0:
        continue
    with open(filename) as my_file:

        cols = ','.join(list(pd.read_csv(filename).columns.values))
        #This is only a test file, not all the directory
        sql = """COPY sensor_data ({cols}) FROM stdin  
               DELIMITER as ',' CSV HEADER""".format(cols=cols)
        cur.copy_expert(sql, my_file)
    con.commit()


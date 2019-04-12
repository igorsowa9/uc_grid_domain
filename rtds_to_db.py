import sys, time
from datetime import datetime, timedelta
import psycopg2
from receive import receive
from settings import settings_fromRTDS, NumData, dbname
from settings import IP_receive, Port_receive
import random
import numpy as np
from sql_queries import sqlquery_rtds_write


def db_connection(dbname):
    try:
        global conn
        conn = psycopg2.connect("dbname='" + dbname + "' user='postgres' host='localhost' password='postgres'")
        return conn
    except:
        sys.exit(0)


def receive_rtds_data():

    # read RTDS measurements - see settings
    #ldata = receive(IP_receive, Port_receive, NumData)
    dt_now = datetime.utcnow()

    w1 = 4.1 + random.randint(1, 101)/100
    e1 = 0.87 + random.randint(1, 101) / 1000
    f1 = 50
    w2 = 3.8 + random.randint(1, 101)/100
    e2 = 0.94 + random.randint(1, 101) / 1000
    f2 = 50
    ldata = [w1, e1, f1, w2, e2, f2]  # e.g. 4 converters with omega and e_voltage values for secondary control

    np_set = np.array(settings_fromRTDS)
    SQLtext = ""
    unique_devices_id = np.unique(np_set[:, 2])
    for device_id in unique_devices_id:
        device_name = np.unique(np_set[np_set[:, 2] == device_id, 1])[0]
        ds = np_set[np_set[:, 2] == str(device_id), :]
        value1 = np.round(ldata[int(ds[ds[:, 4] == str(1), 0])], 4)
        value2 = np.round(ldata[int(ds[ds[:, 4] == str(2), 0])], 4)
        value3 = np.round(ldata[int(ds[ds[:, 4] == str(3), 0])], 4)
        SQLtext += sqlquery_rtds_write(device_name, device_id, value1, value2, value3, dt_now)

    SQLtext += " DELETE FROM rtds_devices WHERE value_ts < (now() at time zone 'utc')-'5 seconds'::interval; "

    conn = db_connection(dbname)
    cursor = conn.cursor()

    try:
        cursor.execute(SQLtext)
        conn.commit()
    except psycopg2.OperationalError as e:
        sys.exit()
    finally:
        conn.close()


def receive_repeatedly(sleep):
    try:
        while True:
            receive_rtds_data()
            time.sleep(sleep)
    except KeyboardInterrupt:
        print("User's interrupt")
        conn.close()


receive_repeatedly(0.01)

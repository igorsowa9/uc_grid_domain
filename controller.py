import sys
import psycopg2
from datetime import datetime
from send import send
from settings import settings_fromRTDS, dbname
from settings import IP_send, Port_send
from sql_queries import sqlquery_control_read, sql_query_control_timeupdate
import numpy as np


def db_connection(dbname):
    try:
        global conn
        conn = psycopg2.connect("dbname='" + dbname + "' user='postgres' host='localhost' password='postgres'")
        print("DB: " + dbname + " connected.")
        return conn
    except:
        print("I am unable to connect to the database. STOP.")
        sys.exit(0)


def load_measurement(time_limit=1, limit=2):
    """
    should download all what necessary with one query to limit delay.
    """

    conn = db_connection(dbname)
    cursor = conn.cursor()
    dt_now = datetime.utcnow()

    # cursor.execute(
    #     "SELECT "+str(measurement)+", value_ts FROM rtds_devices "
    #     "WHERE value_ts < (now() at time zone 'utc')-'"+str(time_limit)+" seconds'::interval "
    #     "AND device_id = '"+str(device_id)+"' "
    #     "ORDER BY value_ts DESC "
    #     "LIMIT "+str(limit)+";")
    cursor.execute(
        "SELECT device_id, value1, value2, value3, value_ts FROM rtds_devices "
        "WHERE value_ts < (now() at time zone 'utc')-'"+str(time_limit)+" seconds'::interval "
        "ORDER BY value_ts DESC " 
        "LIMIT "+str(limit)+";")
    data = np.array(cursor.fetchall())
    return data

# settings_fromRTDS = [
#     [0, 'device001', 1, 'value1', 1],
#     [1, 'device001', 1, 'value2', 2],
#     [2, 'device001', 1, 'value3', 3],
#     [3, 'device002', 2, 'value1', 1],
#     [4, 'device002', 2, 'value2', 2],
#     [5, 'device002', 2, 'value3', 3]]

data = load_measurement()
d1 = data[data[:, 0] == "1", :][0]
d2 = data[data[:, 0] == "2", :][0]

control_output = np.round((float(d1[1])+float(d2[1]))/2 + (float(d1[2])+float(d2[2]))/2, 4)

print("difference between the timestamp of the measurement (stamped when data received from RTDS, "
      "and the moment of control value derivation:")
print(datetime.utcnow() - data[0][4])
print(control_output)



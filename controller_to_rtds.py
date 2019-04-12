import sys
import psycopg2
from datetime import datetime
from send import send
from settings import settings_toRTDS, dbname
from settings import IP_send, Port_send
from sql_queries import sqlquery_control_read, sql_query_control_timeupdate
from load_db import db_connection

now = datetime.utcnow()
data_to_RTDS = []
conn = db_connection(dbname)
cursor = conn.cursor()

for i in range(0, len(settings_toRTDS)):

    node_id = settings_toRTDS[i][1]
    injection_id = settings_toRTDS[i][2]
    id = settings_toRTDS[i][3]
    controltype = settings_toRTDS[i][4]

    query = sqlquery_control_read(str(node_id), str(injection_id), str(controltype), str(now), str(1))
    cursor.execute(query)
    r = cursor.fetchall()
    data_to_RTDS.append(float(r[0][2]))

    # update the timestamp: "time_sent"
    time_query = sql_query_control_timeupdate("time_sent", now, node_id, id, controltype, now)
    cursor.execute(time_query)
    conn.commit()

conn.close()
send(data_to_RTDS, IP_send, Port_send) # data_to_RTDS = [0.9, -0.2, 0.7, 0.5, 1.0, 0.9, -0.96, -0.6]

# if successfull send -> update "time_received"
now = datetime.utcnow()
conn = db_connection(dbname)
cursor = conn.cursor()

for i in range(0, len(settings_toRTDS)):
    node_id = settings_toRTDS[i][1]
    id = settings_toRTDS[i][3]
    controltype = settings_toRTDS[i][4]

    try:
        cursor.execute(sql_query_control_timeupdate("time_received", now, node_id, id, controltype, now))
        conn.commit()
    except psycopg2.OperationalError as e:
        print('RPI1: Unable to execute query!\n{0}').format(e)
        sys.exit(0)
conn.close()

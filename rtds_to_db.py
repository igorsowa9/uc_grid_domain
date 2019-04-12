import sys
from datetime import datetime, timedelta
import psycopg2
from receive import receive
from settings import settings_fromRTDS, NumData, default_accuracy, dbname
from settings import IP_receive, Port_receive
from sql_queries import sqlquery_pmu_measurement_write

def db_connection(dbname):
    try:
        global conn
        conn = psycopg2.connect("dbname='" + dbname + "' user='postgres' host='localhost' password='postgres'")
        print("DB: " + dbname + " connected.")
        return(conn)
    except:
        print("I am unable to connect to the database. STOP.")
        sys.exit(0)


# read RTDS measurements - see settings
ldata = receive(IP_receive, Port_receive, NumData)

SQLtext_measurements = ""
current_time = datetime.utcnow()

for i in range(0,NumData):
    measurementvalue = ldata[i]
    s = settings_fromRTDS[i]
    mtype = int(settings_fromRTDS[i][1])
    phaseid = settings_fromRTDS[i][2]
    nodeid = int(settings_fromRTDS[i][3])
    SQLtext_measurements += sqlquery_measurement_write(str(mtype),
                                                       str(phaseid),
                                                       str(nodeid),
                                                       str(current_time),
                                                       str(measurementvalue),
                                                       str(default_accuracy))
    SQLtext_measurements += " "

# writes measurements to the DB
conn = db_connection(dbname)
cursor = conn.cursor()

try:
    cursor.execute(SQLtext_measurements)
    conn.commit()
    print("RPI2: ...trying to write measurements in DB...")
except psycopg2.OperationalError as e:
    print('RPI2: Unable to execute query!\n{0}').format(e)
    sys.exit(1)
finally:
    print('RPI2: Measurements written in DB, closing the connection.')
    conn.close()
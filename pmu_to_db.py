import paho.mqtt.client as mqttcli
import paho.mqtt.publish as publish
import time
import sys, os
import psycopg2
from datetime import datetime
from sql_queries import sqlquery_pmu_write
import json
from settings import *


def db_connection(dbname):
    try:
        global conn
        conn = psycopg2.connect("dbname='" + dbname + "' user='postgres' host='localhost' password='postgres'")
        print("DB: " + dbname + " connected.")
        return conn
    except:
        print("I am unable to connect to the database. STOP.")
        sys.exit(0)


def message_handler(message_decoded, userdata):
    print("Message from: " + str(userdata) + " received (" + str(datetime.now()) + "): " + message_decoded)
    message_decoded = message_decoded.replace("'", '"')

    try:
        message = json.loads(message_decoded)
    except json.decoder.JSONDecodeError as e:
        message = None

    phasor_ts = message["ts"]
    phasor_ts_dt = datetime.strptime(phasor_ts, '%Y-%m-%d %H:%M:%S.%f')

    # INSERT query and DELETE older tuples then X minutes according to upc time
    postgres_query = sqlquery_pmu_write(userdata, phasor_ts_dt, message["magnitude"])
    postgres_query += "; "
    postgres_query += "DELETE FROM pmu001 WHERE phasor_ts < (now() at time zone 'utc')-'1 minutes'::interval;"

    conn = db_connection(dbname)
    cursor = conn.cursor()

    try:
        cursor.execute(postgres_query)
        conn.commit()
        print("...trying to write PMU measurements into DB... (and deleting old ones)")
    except psycopg2.OperationalError as e:
        print('RPI2: Unable to execute query!\n{0}').format(e)
        sys.exit(1)
    finally:
        print('RPI2: PMU measurements written in DB, closing the connection.')
        print('')
        conn.close()


def on_message_control(client, userdata, message):

    message_decoded = message.payload.decode("utf-8")
    message_handler(message_decoded, userdata)


def receive_pmu_data(loopt):

    vm = mqttcli.Client(userdata="pmu001")
    vm.connect(broker_ip)
    print("Broker "+str(broker_ip)+" connected.")

    topic = "pmu001/data"

    try:
        vm.loop_start()
        vm.subscribe([(topic, 0)])
        vm.message_callback_add(topic, on_message_control)
        time.sleep(loopt)
        vm.loop_stop()

    except KeyboardInterrupt:
        vm.loop_stop()
        print("User's Keyboard Interrupt Exception")


def receive_repeatedly():
    while True:
        receive_pmu_data(10)


receive_repeatedly()
def sqlquery_control_read(node_id, injection_id, control_type, time, limit=str(1)):

    """
    Query to read the the control control values from measuremencommand.control
    -4- P and Q optimal settings are read by the RPI in order to be sent to RTDS according to settings_toRTDS

    :param nodeid: in pc-test-cast, 0-9
    :param resource_id: injections 0-13
    :param control_type: 2 is P control (0...1) 4 is Q control
    :param time: usually now timestamp
    :param limit:
    :return: sql query
    """

    SQLtext = ""
    SQLtext += " SELECT scl_control_id, time_target, wr_value FROM measurecommand.scl_writing WHERE \n"
    SQLtext += " scl_control_id = (SELECT scl_control_id \n"
    SQLtext += " FROM measurecommand.control \n"
    SQLtext += " NATURAL JOIN bridge.cim_scl_control \n"
    SQLtext += " NATURAL JOIN networktopology.cim_control \n"
    SQLtext += " NATURAL JOIN networktopology.control_parameters \n"
    SQLtext += " NATURAL JOIN networktopology.injections \n"
    SQLtext += " WHERE node_id='" + str(node_id) + "' AND injection_id='" + str(injection_id) + "' AND control_type='" + str(control_type) + "') \n"
    SQLtext += " AND time_target < '" + time + "' \n"
    SQLtext += " ORDER BY measurecommand.scl_writing.time_target DESC \n"
    SQLtext += " LIMIT " + limit + "\n"
	# Ok!
	# why putting the limit ? In teory there should be maximum one or?
    return SQLtext


def sqlquery_measurement_read(measurement_type, time1, time2, ldevice_id=1, limit=False):

    """
    Query to read the the measurements from measurementcommand.measurement_historian.
    -2- OPF reads the measurements for constraints: Pup, Pdown, Qup, Qdown

    :param measurement_type: type of measurement as in database description 28-31 P/Q max/min constraints
    :param time1: timestamp lower boundry
    :param time2: timestamp upper boundry
    :param limit: limits number of returned rows, default with no limit
    :return: sql query
    """

    SQLtext = ""
    SQLtext += " SELECT distinct on (networktopology.nodes.id) "
    SQLtext += " networktopology.nodes.id, "
    SQLtext += " networktopology.cim_measurement.phase, "
    SQLtext += " measurecommand.measurement_historian.value, "
    SQLtext += " measurecommand.measurement_accuracy.accuracy, "
    SQLtext += " measurement_type "
    SQLtext += " FROM measurecommand.physicaldevice \n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.logicaldevice\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.logicalnode\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.dataobject\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.dataattribute\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.measurement\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.measurement_historian\n"
    SQLtext += " NATURAL JOIN\n"
    SQLtext += " measurecommand.measurement_accuracy\n"
    SQLtext += " NATURAL JOIN \n"
    SQLtext += " bridge.cim_scl_measurement\n"
    SQLtext += " NATURAL JOIN \n"
    SQLtext += " networktopology.cim_measurement\n"
    SQLtext += " NATURAL JOIN networktopology.nodes\n"
    SQLtext += " WHERE measurecommand.measurement.dattribute_id_value = measurecommand.dataattribute.dattribute_id\n"
    SQLtext += " AND ldevice_id > 2 "
    SQLtext += " AND measurement_type = '" + str(measurement_type) + "'\n"
    SQLtext += " AND measurecommand.measurement_historian.timestamp > '" + time1 + "'\n"
    SQLtext += " AND measurecommand.measurement_historian.timestamp < '" + time2 + "'\n"
    SQLtext += " ORDER BY networktopology.nodes.id ASC, measurecommand.measurement_historian.timestamp DESC , "
    SQLtext += " measurecommand.measurement_accuracy.acc_timestamp DESC \n"

    if not limit == False: SQLtext += "LIMIT "+str(int(limit))+"\n"
    return SQLtext
	# Ok!
	# why putting the limit ? In teory there should be maximum one or?
	# I changed to ldevice_id > 2 to avoid state estimation and forecast measurements

def sqlquery_control_write(node_id, injection_id, control_type, time1, controlvalue):

    """
    Query to write control values.
    -3- After OPF, the optimal calculated control values are written to DB.

    :param node_id: in 0-9
    :param id: general id for all resource, e.g. for injections (in pc-test-case): 0-13
    :param control_type: 1-6 in networktopology.cim_control_index
    :param time1: ts without timezone 2017-01-01 01:01:01
    :param controlvalue: value to write
    :return: sql query
    """

    SQLtext = ""
    SQLtext += "INSERT INTO measurecommand.scl_writing (scl_control_id,time_written,time_target,wr_value) VALUES \n"
    SQLtext += "((SELECT scl_control_id \n"
    SQLtext += "FROM measurecommand.control \n"
    SQLtext += "NATURAL JOIN bridge.cim_scl_control \n"
    SQLtext += "NATURAL JOIN networktopology.cim_control \n"
    SQLtext += "NATURAL JOIN networktopology.control_parameters \n"
    SQLtext += "NATURAL JOIN networktopology.injections \n"
    SQLtext += "WHERE node_id='" + str(node_id) + "' AND injection_id ='" + str(injection_id) + "' " 
    SQLtext += "AND control_type= '"+ str(control_type) + "'),'" + time1 + "','" + time1 + "','" + str(controlvalue) + "'); \n"
	# Ok!
	# I replaced id with injection_id
    return SQLtext


def sql_query_control_timeupdate(time_type, time_value, node_id, injection_id, controltype, time_written):

    """
    Updates different timestamps in the measurecommand.scl_writing.

    :param time_type: string of which ts needs to be updated
    :param time_value: value of that time
    :param node_id: in which node
    :param id: which resource in networktopology.injections
    :param controltype: 1-6 in networktopology.cim_control_index
    :param time_written: i.e. first timestamp from measurecommand.scl_writing
    :return: sql query
    """

    SQLtext = ""
    SQLtext += "UPDATE measurecommand.scl_writing SET " + str(time_type) + " = '"+str(time_value)+"' WHERE \n"
    SQLtext += "scl_control_id = (SELECT scl_control_id\n"
    SQLtext += "FROM measurecommand.control\n"
    SQLtext += "NATURAL JOIN bridge.cim_scl_control\n"
    SQLtext += "NATURAL JOIN networktopology.cim_control\n"
    SQLtext += "NATURAL JOIN networktopology.control_parameters\n"
    SQLtext += "NATURAL JOIN networktopology.injections\n"
    SQLtext += "WHERE node_id='"+str(node_id)+"' AND id='"+str(injection_id)+"' AND control_type='"+str(controltype)+"') \n"
    SQLtext += "AND time_written < '"+str(time_written)+"'\n"
    return SQLtext
	# Ok!
	# we don't need to set also the time written

def sqlquery_control_movetohistorian(node_id,injection_id):

    """
    Moves everything from measurecommand.scl_writing to measurecommand.scl_writing_historian.
    -(6)- Called if the control are implemented, e.g. (15s) after sending value to RTDS

    :return: sql query
    """

    SQLtext = ""
    SQLtext += " INSERT INTO measurecommand.scl_writing_historian( "
    SQLtext += " wr_value, time_written, time_target, time_sent, time_received, time_applied, scl_control_id) \n"
    SQLtext += " (SELECT wr_value, time_written, time_target, time_sent, time_received, time_applied, scl_control_id "
    SQLtext += " FROM measurecommand.scl_writing WHERE scl_control_id =(SELECT scl_control_id \n"
    SQLtext += " FROM measurecommand.control \n"
    SQLtext += " NATURAL JOIN bridge.cim_scl_control \n"
    SQLtext += " NATURAL JOIN networktopology.cim_control \n"
    SQLtext += " NATURAL JOIN networktopology.control_parameters \n"
    SQLtext += " NATURAL JOIN networktopology.injections \n"
    SQLtext += " WHERE node_id='" + str(node_id) + "' AND injection_id ='" + str(injection_id) + "' " 
    SQLtext += " AND control_type= '"+ str(control_type) + "'));"
    SQLtext += " DELETE FROM measurecommand.scl_writing WHERE scl_control_id =(SELECT scl_control_id \n"
    SQLtext += " FROM measurecommand.control \n"
    SQLtext += " NATURAL JOIN bridge.cim_scl_control \n"
    SQLtext += " NATURAL JOIN networktopology.cim_control \n"
    SQLtext += " NATURAL JOIN networktopology.control_parameters \n"
    SQLtext += " NATURAL JOIN networktopology.injections \n"
    SQLtext += " WHERE node_id='" + str(node_id) + "' AND injection_id ='" + str(injection_id) + "' " 
    SQLtext += " AND control_type= '"+ str(control_type) + "');"
    return SQLtext


def sqlquery_pmu_write(device_id, timestamp, value):
    """
    Created quesry writes PMU measurement (8 channels) values to the DB.
    """
    SQLtext = ""
    SQLtext += "INSERT INTO pmu001 (device_id, phasor_ts, value, db_writing_ts) "
    SQLtext += "VALUES ('"+str(device_id)+"', '"+str(timestamp)+"', '"+str(value)+"', (now() at time zone 'utc') );"
    return SQLtext


def sqlquery_rtds_write(device_name, device_id, value1, value2, value3, dt_measurement):
    """
    Same like above but for RTDS data.
    """
    SQLtext = ""
    SQLtext += "INSERT INTO rtds_devices (device_name, device_id, value1, value2, value3, value_ts, db_writing_ts) "
    SQLtext += "VALUES ('" + str(device_name) + "', '" + str(device_id) + "', '" + str(value1) + "', '" + str(value2) \
               + "', '" + str(value3) + "', '" + str(dt_measurement) + "', (now() at time zone 'utc') ); "
    return SQLtext


def sqlquery_write_flag(ftype_name, flag_timestamp, flag_message):
    """
    Query for writing management.flag i.e.
    <<Flags should be used for communication of your algorithm to the rest of the world.
    For instance you can use it when there are no data in input or when you produce a certain output.>>
    :param ftype_name: string name according to descriptions in management.flag_index. for PC:
            "OPF_SF" "OPF_NSF" "OPF_IED_CE" "OPF_IED_OE"
    :param flag_timestamp: datetime.utcnow()
    :param flag_message: some text/comment
    :return: sql query
    """

    pc_control = ["OPF_SF", "OPF_NSF", "OPF_IED_CE", "OPF_IED_OE", "OPF_SE"]
    if not str(ftype_name) in pc_control:
        print("sqlquery_write_flag: ftype_name type missing in the management.flag_index!")
        return 0

    SQLtext = ""
    SQLtext += "INSERT INTO management.flag(ftype_id,flag_timestamp,flag_message) "
    SQLtext += "VALUES((SELECT ftype_id FROM management.flag_index WHERE ftype_name = '" + str(ftype_name) + "'),'" \
               + str(flag_timestamp) + "','" + flag_message + "');\n"
    return SQLtext


def sqlquery_write_log(flag_timestamp, log_message):
    """
    <<Logs are needed to store information that are produced by your algorithm and needed by your algorithm itself.>>
    algorithm_id is constant =4 for power control algorithm
    :param flag_timestamp: datetime.utcnow()
    :param log_message: str of the message
    :return:
    """

    SQLtext = ""
    SQLtext += "INSERT INTO management.algorithm_log(algorithm_id,timestamp,log_message) "
    SQLtext += " VALUES('4','" + str(flag_timestamp) + "','" + str(log_message) + "');\n"
    return SQLtext
	#Ok!


def sqlquery_test():

    """
    Test Query.
    :return: sql query according to needs
    """

    SQLtext = ""
    SQLtext += "INSERT INTO measurecommand.scl_writing (scl_control_id,time_written,time_target,wr_value) VALUES\n"
    SQLtext += "((SELECT scl_control_id\n"
    SQLtext += "FROM measurecommand.control\n"
    SQLtext += "NATURAL JOIN bridge.cim_scl_control\n"
    SQLtext += "NATURAL JOIN networktopology.cim_control\n"
    SQLtext += "NATURAL JOIN networktopology.control_resources\n"
    SQLtext += "NATURAL JOIN networktopology.injections\n"
    SQLtext += "NATURAL JOIN networktopology.nodes\n"
    SQLtext += "WHERE nodeid='0' AND resource_id='0' AND control_type='4'),'2017-11-30 11:44:44','2017-11-30 11:40:00','1111');\n"

    return SQLtext

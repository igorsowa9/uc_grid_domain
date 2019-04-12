# database settings
dbname = 'n5geh'

# PMU settings
broker_ip = "10.12.0.1"
settings_fromPMU = [
    ['V1a', 21, 'AN', 1],  # exact structure to be defined, but we have max 8 channels
    ['V1b', 21, 'BN', 2],
    ['V1c', 21, 'CN', 2],

    ['I1a', 21, 'AN', 1],
    ['I1b', 21, 'BN', 2],
    ['I1c', 21, 'CN', 2],

    ['V2a', 21, 'AN', 1],
    ['V3a', 21, 'AN', 2]]


# RTDS  settings
IP_send = '134.130.169.62'  # of GTNET
IP_receive = '134.130.169.62'  # IP of the SAU machine (e.g. of the RPI)
# where you run it
Port_send = 12345
Port_receive = 12345

# power system settings
NumData = 4  # number of measurements from RTDS
# name, mtype, phaseid, nodeid / line_id, // others: time1, measurementvalue, accuracyvalue
settings_fromRTDS = [
    [0, 'device001', 1, 'value1', 1],
    [1, 'device001', 1, 'value2', 2],
    [2, 'device001', 1, 'value3', 3],
    [3, 'device002', 2, 'value1', 1],
    [4, 'device002', 2, 'value2', 2],
    [5, 'device002', 2, 'value3', 3]]

#  name, node_id / line_id , id (from networktopology.injections), control type
#  without slack generator only converters and flexible loads
settings_toRTDS = [
    ['Pset3', 4, 1, 20, 2],
    ['Pset3', 4, 3, 20, 2]]
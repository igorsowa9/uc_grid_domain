# database settings
dbname = 'n5geh'

# networks settings
IP_send = '134.130.169.62'  # of GTNET
IP_receive = '134.130.169.62'  # IP of the SAU machine (e.g. of the RPI)
# where you run it
Port_send = 12345
Port_receive = 12345

# power system settings
NumData = 99 #number if settings_fromRTDS
# name, mtype, phaseid, nodeid / line_id, // others: time1, measurementvalue, accuracyvalue
default_accuracy = 1
settings_fromRTDS = [
    ['pos01', 21, 'ABCN', 1], #node_id of tap changer?
    ['pos12', 21, 'ABCN', 2], #node_id of tap changer?
    ['V1rms', 2, 'ABCN', 1]]

#  name, node_id / line_id , id (from networktopology.injections), control type
#  without slack generator only converters and flexible loads
settings_toRTDS = [
    ['Pset3', 4, 1, 20, 2],
    ['Pset3', 4, 3, 20, 2],
    ['Pset4', 4, 4, 20, 2]]
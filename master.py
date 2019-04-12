import pmu_to_db
import rtds_to_db
import sys
import numpy as np

np.set_printoptions(suppress=True)
# better as separate scripts or in parallel here

# runs in the cloud
pmu_to_db.receive_repeatedly()

# partially should run at VM (RTDS->MQTT pub), partially in the cloud (MQTT sub -> DB)
rtds_to_db.receive_repeatedly(1)  # number defines sleep period between the steps of writing RTDS values to DB


sys.exit()

rtds_to_controller()  # read measurements from RTDS -> write measurements to DB

sys.exit()
db_to_rtds()
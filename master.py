import pmu_to_db
import sys

pmu_to_db.receive_repeatedly()
sys.exit()

rtds_to_controller()  # read measurements from RTDS -> write measurements to DB

sys.exit()
db_to_rtds()
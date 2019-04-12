import numpy as np
import sys
import psycopg2
from pypower.api import *
from sql_queries import sqlquery_measurement_read
from decimal import Decimal
from datetime import datetime, timedelta
from pprint import pprint as pp

from RPI1.RPI1 import dbname


def db_connection(dbname):
    """
    Connection to the database of SAU
    :param: str of db name
    :return: instance pf connection from psycopg2 pointing at dbname from input.
    """
    try:
        global conn
        conn = psycopg2.connect("dbname='" + dbname + "' user='postgres' host='134.130.169.25' password='postgres'")
        print("DB: " + dbname + " connected.")
        return(conn)
    except:
        print("I am unable to connect to the database. STOP.")
        sys.exit(0)


def case_load_static():
    """
    Based on pypower/matpower format
    :return: ppc format of the networktolopoly data
    """

    conn = db_connection(dbname)
    cursor = conn.cursor()
    np.set_printoptions(suppress=True)

    ppc = {"version": '2'}

    # -----  only PF Data  -----#
    # system MVA base (base kV accoring to nodes)
    cursor.execute(
        "SELECT pbase FROM networktopology.grid_general;")
    baseVA = cursor.fetchall()
    baseMVA = int(baseVA[0][0]) / 1000000
    ppc["baseMVA"] = baseMVA

    # nodes
    cursor.execute(
        "SELECT * FROM networktopology.nodes "
        "ORDER BY networktopology.nodes.id;")
    nodes = cursor.fetchall()

    global nodes_n # global value of number of nodes
    nodes_n = len(nodes)

    # nodes matrix initialization
    nodes_matrix = np.zeros((nodes_n,13))
    baseKV = float(nodes[0][3]) / 1000 ## for now, baseKV as the first node. Need to be improved when there are trafos

    # assigning basic/default values for nodes
    for i in np.arange(0,len(nodes)):
        nodes_matrix[i][0] = i+1 # bus_i
        nodes_matrix[i][1] = 1  # bus type - all PQ types as default
        nodes_matrix[0][1] = 3 # first bus as default slack
        nodes_matrix[i][6] = 1 # area
        nodes_matrix[i][7] = 1 # Vm, pu
        nodes_matrix[i][9] = baseKV # baseKV !!!!
        nodes_matrix[i][10] = 1 # zone
        nodes_matrix[i][11] = nodes[i][5] # Vmax
        nodes_matrix[i][12] = nodes[i][4] # Vmin

    # integrate loads to nodes - only one power value (P?)
    cursor.execute(
        "SELECT networktopology.injections.node_id, "
        "networktopology.injections.nominals,"
        "networktopology.injections.injection_type,"
        "networktopology.injections.nominalpf,"
        "networktopology.injections.status, "
        
        "networktopology.control_parameters.id, "
        "networktopology.control_parameters.const_p_up,"
        "networktopology.control_parameters.const_p_down,"
        "networktopology.control_parameters.const_q_up,"
        "networktopology.control_parameters.const_q_down "
        "FROM networktopology.injections "

        "INNER JOIN networktopology.nodes "
        "ON networktopology.injections.node_id = networktopology.nodes.node_id "
        
        "INNER JOIN networktopology.control_parameters "
        "ON networktopology.injections.id = networktopology.control_parameters.id "

        "WHERE networktopology.injections.injection_type=1 "
        "OR networktopology.injections.injection_type=2 "
        "ORDER BY networktopology.injections.injection_id; "

    ) # 1,2 are industrial and residential loads
    loads = cursor.fetchall()
    global loads_n  # global value of number of nodes
    loads_n = len(loads)

    loads_flexible_n = 0
    for n in loads:
        if float(n[6]) == float(n[7]) and float(n[8]) == float(n[9]): # i.e. if load is fixed
            nodes_matrix[int(n[0]),2]=float(n[6]) # P load
            nodes_matrix[int(n[0]),3]=float(n[8]) # Q load
            loads_flexible_n = loads_flexible_n+1
        else:
            pass

    # gen matrix building + integrate flexible loads as negative generators to the gen matrix
    cursor.execute(
        "SELECT networktopology.injections.node_id, "
        "networktopology.injections.nominals,"
        "networktopology.injections.injection_type,"
        "networktopology.injections.nominalpf,"
        "networktopology.injections.status, "

        "networktopology.control_parameters.id, "
        "networktopology.control_parameters.const_p_up,"
        "networktopology.control_parameters.const_p_down,"
        "networktopology.control_parameters.const_q_up,"
        "networktopology.control_parameters.const_q_down "
        "FROM networktopology.injections "

        "INNER JOIN networktopology.nodes "
        "ON networktopology.injections.node_id = networktopology.nodes.node_id "

        "INNER JOIN networktopology.control_parameters "
        "ON networktopology.injections.id = networktopology.control_parameters.id "

        "WHERE networktopology.injections.injection_type=3 "
        "OR networktopology.injections.injection_type=5 "
        "ORDER BY networktopology.injections.injection_id; "

    ) # 3 are PV generators, 5 are flexible loads-negative generators
    generators = cursor.fetchall()
    gen_matrix = np.zeros((len(generators), 21)) # all loads are flexible, will be placed as negative generators

    for i in np.arange(0,len(generators)): # loop for generators only
        gen_matrix[i][0] = int(generators[i][0])+1
        # nodes_matrix[int(generators[i][0])-1][1] = 2 # definition of PV buses in nodes_matrix where the generators are
        # gen_matrix[i][1] = 0 # current P=0, Q=0 does not matter in OPF ---- np.sqrt(np.abs(np.power(float(generators[i][3]),2)-1))
        gen_matrix[i][3] = float(generators[i][8])  # Qmax !!!
        gen_matrix[i][4] = float(generators[i][9])  # Qmin !!!
        gen_matrix[i][5] = 1 # Vm
        gen_matrix[i][6] = baseMVA # "total MVA base of machine, defaults to baseMVA"
        gen_matrix[i][7] = 1 if generators[i][4]==True else 0 # status
        gen_matrix[i][8] = float(generators[i][6])  # Pmax !!!
        gen_matrix[i][9] = float(generators[i][7])  # Pmin=0 - generators

    # branches/lines matrix
    cursor.execute(
        "SELECT "
        "networktopology.lines.node1, "
        "networktopology.lines.node2, "
        "networktopology.lines.length, "
        "networktopology.ltype.amax, "
        "networktopology.ltype.r_pos_m, "
        "networktopology.ltype.x_pos_m, "
        "networktopology.ltype.g_pos_m, "
        "networktopology.ltype.b_pos_m, "
        "networktopology.lines.status "
        "FROM networktopology.lines "
        "INNER JOIN networktopology.ltype "
        "ON networktopology.lines.l_code"
        "=networktopology.ltype.l_code "
        "ORDER BY networktopology.lines.id;")
    lines = cursor.fetchall()
    global n_lines
    n_lines = len(lines)

    baseZ = np.power(baseKV,2) / baseMVA

    lines_matrix = np.zeros((n_lines,13))
    for i in np.arange(0,len(lines_matrix)):
        lines_matrix[i][0] = lines[i][0]+1 # fbus +1 because the nodeid are indeces that start from 0
        lines_matrix[i][1] = lines[i][1]+1 # tbus
        length = int(lines[i][2]) # in meters
        lines_matrix[i][2] = length * float(lines[i][4]) # r_relative in ohm/m
        lines_matrix[i][3] = length * float(lines[i][5]) # x
        lines_matrix[i][4] = length * float(lines[i][7]) # b
        lines_matrix[i][5] = float(lines[i][3]) # rateA in power units
        lines_matrix[i][6] = float(lines[i][3]) # rateB
        lines_matrix[i][7] = float(lines[i][3]) # rateC
        lines_matrix[i][10] = 1 if lines[i][8] == True else 0 # status
        lines_matrix[i][11] = -360  # angmin
        lines_matrix[i][12] = 360  # angmax

    ppc["bus"] = nodes_matrix
    ppc["gen"] = gen_matrix
    ppc["branch"] = lines_matrix

    # -----  OPF Data  -----
    # generator cost data  - loads could be implemented as negative gens
    # 1 startup shutdown n x1 y1 ... xn yn
    # 2 startup shutdown n c(n-1) ... c0 ---- f(p) = cn*p^n + ... + c1*p + c0

    cursor.execute(
        "SELECT networktopology.control_parameters.id, "
        "networktopology.injections.injection_type, "
        "networktopology.control_parameters.cost_a1, "
        "networktopology.control_parameters.cost_b1, "
        "networktopology.control_parameters.t_constant_opf "
        "FROM networktopology.control_parameters "
        "INNER JOIN networktopology.injections "
        "ON networktopology.injections.id = networktopology.control_parameters.id "
        "WHERE networktopology.control_parameters.t_constant_opf != '0' "
        "AND networktopology.control_parameters.cost_a1 != '0' "
        "AND networktopology.control_parameters.cost_b1 != '0' "
        "ORDER BY networktopology.injections.id;") # ordered by injections.id to align with generators bus ppc.gen
    generators_cost = cursor.fetchall()

    gen_cost_matrix = np.zeros((len(generators_cost), 6)) # if 2x then second time for reactive power costs
    for i in np.arange(0,len(generators_cost)):
        gen_cost_matrix[i][0] = 2 # type of the model (2 i.e. with polynominal cost function)
        gen_cost_matrix[i][1] = 0 # startup cost
        gen_cost_matrix[i][2] = 0 # shutdown cost
        gen_cost_matrix[i][3] = 2 # just one (+constant) polynominal so far
        k = -1 if generators_cost[i][1] == 1 or generators_cost[i][1] == 2 else 1
        gen_cost_matrix[i][4] = k*int(generators_cost[i][2]) # c1
        gen_cost_matrix[i][5] = 0 # c0
        # should be same structure for reactive power costs
        # !!! SEPARATE COSTS FOR Q DO NOT WORK in pypower solver, even for the test cases e.g. case9Q (in matlab it works)

        # !!! flexible load cost data as negative gens - BUG WITH PRINTING RESULTS PRINTPF HOWEVER LOOKS LIKE
        # THE CALCULATIONS WORKS
    ppc["gencost"] = gen_cost_matrix

    smug = np.array(generators)[:,[5,0]]

    conn.close()
    return ppc, smug

def case_load_dynamic(ppc):
    """
    Loads measurements from the database, from state estiamtion (1).
    Modifies the input to the opf i.e. pypower or other solver.
    Namely, modifies the P and Q constraints of the controllable generators according to "weather conditions"
    :param: timestamp, ppc_db
    :return: stuff, flags
    """

    conn = db_connection(dbname)
    cursor = conn.cursor()

    # time format: '2017-08-22 16:03:00.xxxxx'
    time2 = datetime.utcnow()
    diff = timedelta(hours = 5)
    time1 = time2 - diff
    #print("time1:", time1, " Time2: ", time2)

    # nodes.id, inj.resource_id, phase, value, accuracy
    Pmax_meas = sqlquery_measurement_read("28", str(time1), str(time2))  # Pmax
    Pmin_meas = sqlquery_measurement_read("29", str(time1), str(time2))  # Pmin
    Qmax_meas = sqlquery_measurement_read("30", str(time1), str(time2))  # Qmax
    Qmin_meas = sqlquery_measurement_read("31", str(time1), str(time2))  # Qmin

    try:
        cursor.execute(Pmax_meas)
        Pmax_rows = np.array(cursor.fetchall())

        cursor.execute(Pmin_meas)
        Pmin_rows = np.array(cursor.fetchall())

        cursor.execute(Qmax_meas)
        Qmax_rows = np.array(cursor.fetchall())

        cursor.execute(Qmin_meas)
        Qmin_rows = np.array(cursor.fetchall())

        print("reading real measurements: ", Pmax_rows, Pmin_rows, Qmax_rows, Qmin_rows)

    except psycopg2.OperationalError as e:
        print('Unable to execute query!\n{0}').format(e)
        sys.exit()
    finally:
        conn.close()

    c1 = Qmax_rows[:,[0,2]]
    c2 = np.transpose(np.matrix(Qmin_rows[:,2]))
    c3 = np.transpose(np.matrix(Pmax_rows[:,2]))
    c4 = np.transpose(np.matrix(Pmin_rows[:,2]))
    constr_in_order = np.concatenate((c1, c2, c3, c4),axis=1)  # nodeid, Qmax, Qmin, Pmax, Pmin
    print("constr_in_order:", constr_in_order)

    gen_buses = ppc['gen'][:,0]

    for c in constr_in_order:
        idx = np.argwhere(gen_buses==int(c[0,0])+1)
        # origin = ppc_db['gen'][idx,[3,4,8,9]] # Depends on the data input, this part for p.u. ones
        # multiply = np.absolute(c[:,1:].astype(np.float)) # make absolute values in order to avoid sign change for Q
        ppc['gen'][idx,[3,4,8,9]] = c[:,1:].astype(np.float)

    print('Dynamic data load: PVs and WPPs generators constraints updated in ppc, based on the "weather measurements."')

    return(ppc)
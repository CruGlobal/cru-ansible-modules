#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
from ansible.module_utils.basic import AnsibleModule
# from ansible.error import AnsibleError
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
import math
import time
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)
# import datetime
# from datetime import datetime, date, time, timedelta

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.0.1'}

DOCUMENTATION = '''
---
module: srvctl
short_description: Give Ansible srvctl functionality.

notes: database current state and expected state are returned.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # To start | stop a database or instance from Ansible using srvctl
    - name: start database
      srvctl:
        db: {{ dest_db_name }}
        cmd: stop
        obj: instance
        inst: 2
        stopt: immediate
        param: force
        ttw: 7
        debugging: /dir/to/debug.log    Note: (1)
      when: master_node                 Note: (2)


    values:
       db: database name
      cmd: [ start | stop ]
      obj: [ database | instance ]
     inst: [ valid instance number ]
    stopt: (stop options): [ normal | immediate | abort ]
           (start options): [ open | mount | nomount | restrict | read only | read write | write ]
    param: [ eval | force | verbose ]
      ttw: time to wait (in min) for status change after executing the command. Default 5.
debugging: True | False | directory

    Notes:
        (1) True -  add debug info to msg output retruned when Ansible module completes
            False - no debugging info
            /dir/to/output.log - give absolute path to debugging log including log name.
                debugging info will be appeneded to the file as they execute.

        (2) Use when master_node else it may try to execute on all nodes simultaneously.

        (3) It's possible to start instance nomount, mount etc. but not to
            alter instance mount, or open. To open the instance using the srvctl module
            you must stop the instance then start instance mount, or start instance open.
            It is possible to "sqlplus> alter database mount" or "alter database open".
            The status change will then be reflected in crsstat.

'''


# Global variables
debugme  = False
# module parameters
vdb_name = ""
vcmd     = ""
vobj     = ""
vinst    = ""
vstopt   = ""
vparam   = ""
default_ttw = 5
# Time to wait in seconds beteen db state checks
loop_sleep_time = 4
# domain pattern used to strip off hostname
vdomain  = ".ccci.org"
# environmentals
grid_home = ""
oracle_home = ""
oracle_sid = ""
node_number = ""
thishost = ""
vall_hosts = []
ansible_facts = {}
# info
msg = ""
custom_err_msg = ""
# debugging
global_debug_msg = ""
debug_log = ""
truism = [True,False,'true','false','Yes','yes']
debug_dir = ""
utils_dir = os.path.expanduser("~/.utils")
module = None


def get_def_dir():
    """Read the ~/.utils file to get the default log_dir"""
    if os.path.isfile(utils_dir):
        with open(utils_dir, 'r') as f:
            for line in f:
                if 'log_dir' in line:
                    debug_dir = line.split('=')[1].strip()
                    debug_dir = debugdir + "debug.log"


def add_to_msg(msg_str):
    """Add some info to the ansible_facts output message"""
    global msg
    global debug_msg

    if msg:
        msg = msg + tmpmsg
    else:
        msg = tmpmsg


def debugg(info_str):
    """Add debugging info to msg if debugging is True"""
    global debugme

    if debug_dir:
        debug_to_file(info_str)
    elif debugme:
        add_to_msg(info_str)


def debug_to_file(debug_str):
    """Write debugging to file"""
    global debugme
    global debug_dir

    if debug_dir:
        with open(debug_dir, 'a') as f:
            f.write(debug_str)


def db_registered(db_name):
    """Given a db name find out if it's registered to srvctl"""
    debugg("db_registered()...starting...")

    ora_home = get_orahome_procid(db_name)
    if not ora_home:
        ora_home = get_orahome_oratab(db_name)

    if ora_home:
        debugg("db_registered() with srvctl, ora_home: %s" % (ora_home))
    else:
        debugg("db_registered() with srvctl no, no ora_home!")

    if ora_home:
        try:
            cmd_str = "%s/bin/srvctl status database -d %s" % (ora_home,db_name)
            process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
            output, code = process.communicate()
        except:
            custom_err_msg = 'Error [db_registered()]: Finding db status. cmd_str: %s ' % (cmd_str)
            custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            debugg(custom_err_msg)
            raise Exception (custom_err_msg)

        tmp = output.strip()
        # PRCD-1120 - PRCD is an Error message
        if 'PRCD' in tmp:
            debugg("db_registered() returning 'false'")
            return("false")
        else:
            debugg("db_registered() returning 'true'")
            return("true")
    else:
        debugg("db_registered() no ora_home so returning 'false'")
        return("false")


def get_hostname():
    """Return the hostame"""
    global host_name
    global vdomain
    debugg("get_hostname()..starting...")

    try:
      cmd_str = "/bin/hostname | /bin/sed 's/" + vdomain + "//'"
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        custom_err_msg = 'Error [get_hostname()]: retrieving hostname. cmd_str: %s ' % (cmd_str)
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("get_hostname(): ERROR: cmd_str=%s custom_err_msg=%s" % (cmd_str, custom_err_msg))
        raise Exception (custom_err_msg)

    tmp_hostname = output.strip()
    debugg("get_hostname() exiting...returning %s" % (tmp_hostname))
    return(tmp_hostname)


def get_gihome():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""

    global grid_home
    global module

    debugg("get_gihome()...start...")

    try:
        cmd_str = "/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error [ get_gihome() ]: retrieving GRID_HOME. Error running cmd: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("get_gihome() ERROR : cmd_str=%s custom_err_msg=%s" % (cmd_str, custom_err_msg) )
        raise Exception (custom_err_msg)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         custom_err_msg = "No GRID_HOME found. Is this a RAC, or Cluster database?"
         # custom_err_msg = custom_err_msg + ' Error[ get_gihome() ]: No output returned after running cmd : %s' % (cmd_str)
         # custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
         module.fail_json(msg=custom_err_msg,ansible_facts={},changed=False)

    debugg("get_gihome()...exiting... returning grid_home=%s" % (grid_home))
    return(grid_home)


def get_node_num():
    """Return current node number, single digit (int)"""
    global grid_home
    global debugme

    if not is_rac():
        debugg("get_node_num()...start...but not a rac..returning empty string..")
        return("")

    debugg("get_node_num()..starting...")

    if not grid_home:
        grid_home = get_gihome()

    cmd_str = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"

    try:
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       custom_err_msg = ' Error[ get_node_num() ]: retrieving node_number '
       custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       debugg("get_node_num() ERROR: cmd_str=%s custom_err_msg=%s" % (cmd_str,cust_err_msg))
       raise Exception (custom_err_msg)

    if output.strip()[-1].isdigit() :
        node_number = int(output.strip()[-1])
    else:
        node_number = int(output.strip())

    debugg("get_node_num() executed this cmd: %s and determined node #: %s full output: %s" % (cmd_str, node_number, output))
    debugg("get_node_num() exit...returning node_number=%s" % (tmp_msg))

    return(node_number)


def get_orahome_oratab(db_name):
    """Return database Oracle home from /etc/oratab"""
    global my_err_msg
    debugg("get_orahome_oratab(%s)....starting..." % (db_name))

    try:
        cmd_str = "cat /etc/oratab | grep -m 1 " + db_name + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       custom_err_msg = ' Error [get_orahome_oratab()]: retrieving oracle_home cmd_str: %s' % (cmd_str)
       custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       debugg("get_orahome_oratab() ERROR: cmd_str=%s cust_err_msg=%s" % (cmd_str, cust_err_msg))
       raise Exception (custom_err_msg)

    ora_home = output.strip()

    if not ora_home:
        custom_exit_msg = 'Error[ get_orahome_oratab(db_name) ] ora_home null after f(x) execution for db_name: %s.' % (db_name)
        sys.exit(custom_exit_msg)

    debugg("get_orahome_oratab() Exit...rerturning ora_home=%s" % (ora_home))
    return(ora_home)


def get_orahome_procid(db_name):
    """Get database Oracle Home from the running process."""

    debugg("get_orahome_procid()...starting...")
    # get the pmon process id for the running database.
    # 10189  tstdb1
    cmd_str = "pgrep -lf _pmon_" + db_name + " | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed"

    try:
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
      custom_err_msg = 'Error[ get_orahome_procid() ]: running pgrep -lf _pmon_%s' % (db_name)
      custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
      debugg("get_orahome_procid() ERROR: cmd_str=%s cust_err_msg=%s" % (cmd_str,cust_err_msg))
      raise Exception (custom_err_msg)

    # if the database is down, but there may be an entry in /etc/oratab so try this:
    if not output:
        debugg("get_orahome_procid() cmd_str=% returned nothing for %s...trying oratab: get_orahome_oratab()" % (cmd_str,db_name))
        tmp_orahome = get_orahome_oratab(db_name)
        if tmp_orahome:
            debugg("get_orahome_procid() called get_orahome_oratab() returning oracle_home=%s" % (tmp_orahome))
            return(tmp_orahome)
        else:
            custom_exit_msg = "Error retrieving oracle_home. No process id found and no /etc/oratab entry found for database: %s" % (db_name)
            debugg("get_orahome_procid() couldn't get ORACLE_HOME nor could get_orahome_oratab custom_exit_msg=%s" % (custom_exit_msg))
            sys.exit(custom_exit_msg)

    # if the cmd_str completed successfully
    try:
        # ['10189', 'tstdb1']
        vprocid = output.split()[0]
    except:
        custom_err_msg = 'Error[ get_orahome_procid(db_name) ] error parsing process id for database: %s Full output: [%s]' % (db_name, output.strip())
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("get_orahome_procid() returned output trying to get pmon process id for the running database, but something happened splitting the output.")
        debugg("custom_err_msg=%s" % (custom_err_msg))
        raise Exception (custom_err_msg)

    # get Oracle home the db process is running out of
    # (0, ' /app/oracle/12.1.0.2/dbhome_1/')
    cmd_str = "sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle//' "

    try:
        os.environ['USER'] = 'oracle'
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = "Error[ get_orahome_procid() ]: retriving oracle_home using processid: %s for database: %s and cmd_str: %s " % (vprocid,db_name,cmd_str)
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("get_orahome_procid() ERROR getting the ORACLE_HOME a the PID was running from")
        debugg("custom_err_msg=%s" % (custom_err_msg))
        raise Exception (custom_err_msg)

    ora_home = output.strip()
    debugg("get_orahome_procid() exiting....returning oracle_home=%s" % (ora_home))
    return(ora_home)


def get_db_state(db_name):
    """
    Return the status of the database on all nodes.
    list of strings with the status of the db on each node['INTERMEDIATE','ONLINE','OFFLINE']
    This function takes the db name as input with, or without the instance number attached
    """
    global grid_home
    global debugme
    custom_err_msg = ""
    node_status = []
    tmp_cmd = ""

    debugg("get_db_state()...starting....")

    if not grid_home:
         grid_home = get_gihome()

    if not grid_home:
        custom_err_msg = "Error[ get_db_state() ]: error determining grid_home from get_gihome() call. grid_home returned value: [%s]" % (grid_home)
        sys.exit(custom_err_msg)

    # check for special cases ASM and MGMTDB and see if db_name has digit (instance number), if so delete it. If not use it.
    if "ASM" in db_name:
        cmd_str = grid_home + "/bin/crsctl status resource ora.asm | grep STATE"
    elif "MGMTDB" in db_name:
        cmd_str = grid_home + "/bin/crsctl status resource ora.mgmtdb | grep STATE"
    elif db_name[-1].isdigit() :
        cmd_str = grid_home + "/bin/crsctl status resource ora." + db_name[:-1] + ".db | grep STATE"  # these give state of each node
    else:
        cmd_str = grid_home + "/bin/crsctl status resource ora." + db_name + ".db | grep STATE"

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = ' Error[ get_db_state() ]: running crsctl to get database: %s state. cmd_str: [%s]' % (db_name,cmd_str)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("get_db_state() ERROR: cmd_str=%s custom_err_msg=%s" % (cmd_str, custom_err_msg))
        raise Exception (custom_err_msg)

    #  possible outputs:
    # STATE=INTERMEDIATE on tlorad01, INTERMEDIATE on tlorad02 ['STATE=OFFLINE', ' OFFLINE']   ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']  ['ONLINE on tlorad01', 'OFFLINE']  ['INTERMEDIATE', ' INTERMEDIATE on tlorad02']
    node_status = output.strip().split(",")

    i = 0
    for item in node_status:
        if "=" in item:
            node_status[i]=item.split("=")[1].strip()
        if " on " in node_status[i]:
            host_name = node_status[i].split(" on ")[1].strip()
            node_status[i] = node_status[i].split(" on ")[0].strip()
        else:
            node_status[i] = node_status[i].strip()
        i += 1

    debugg(" get_db_state() exit. status %s" % (str(node_status)))


    # this function returns a list of strings with host by index : index 0 = node 1, index 1 = node 2
    #                                              node1         node2
    # with the status of both (all) nodes: ie. ['INTERMEDIATE', 'OFFLINE']
    return(node_status)


def wait_for_it(vdb_name, vobj, vexp_state, vttw, vinst):
    """Compare current database (vdb_name) status of all nodes
       to expected state (vstatus) looping in 2 second intervals
       until state is reached or until time runs out (ttw min)"""

    global msg
    global debugme
    global vall_hosts
    global ansible_facts
    global global_debug_msg
    # take current time and add 5 (vttw) minutes (60 * 5)
    # this will be time to stop if database expected state isn't reached.
    timeout =  time.time() + (60 * int(vttw))

    debugg("wait_for_it() ...starting.... with vdb_name: [%s], vobj: [%s], vexp_state: [%s], vttw: [%s], vinst: [%s]" % (vdb_name,vobj,vexp_state,vttw,vinst))

    if vobj.lower() == "database":

        try:
          current_state = get_db_state(vdb_name)
          while (not all(item == vexp_state['exp_state'] for item in current_state) and (time.time() < timeout)):
            time.sleep(int(loop_sleep_time))
            current_state = get_db_state(vdb_name)
        except:
            custom_err_msg = 'Error[ wait_for_it() ]: waiting for %s state to reach: %s current state: %s ' % (vobj,vexp_state['exp_state'], str(current_state) )
            custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            debugg("wait_for_it() ERROR[1] waiting to reach expected state...custom_err_msg=%s" % (custom_err_msg))
            raise Exception (custom_err_msg)

        if vexp_state['meta']:

            current_meta_state = get_db_meta_state(vdb_name)

            try:
                while (not all(item == vexp_state['meta'] for item in current_meta_state.values()) and (time.time() < timeout)):
                    time.sleep(int(loop_sleep_time))
                    current_meta_state = get_db_meta_state(vdb_name)
            except:
                custom_err_msg = 'Error[ wait_for_it() ]: waiting for %s current_meta_state: %s to change to expected: %s last current_meta_state: %s host_name_key: %s current time: %s time.out: %s' % (vobj,str(current_meta_state[host_name_key]), str(vexp_state['meta']), str(current_meta_state[host_name_key]), host_name_key, str(time.time()), str(timeout))
                custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
                debugg("wait_for_it() ERROR[2] in meta wait. custom_err_msg=%s" % (custom_err_msg))
                raise Exception (custom_err_msg)

    # else shutting down or starting an instance
    elif vobj.lower() == "instance":
      debugg("wait_for_it():: else shutting down or starting an instance")
      current_state = []

      # index of the instance to check
      vindex = int(vinst) - 1

      try:
        current_state = get_db_state(vdb_name)
        while (vexp_state['exp_state'] != current_state[vindex]) and (time.time() < timeout):
          time.sleep(int(loop_sleep_time))
          current_state = get_db_state(vdb_name)
      except:
          custom_err_msg = 'Error[ wait_for_it() ]: error - waiting for %s state to change to %s last checked state: %s' % (vobj, vexp_state['exp_state'], current_state[vindex])
          custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
          debugg("wait_for_it() ERROR[3] during second wait.. custom_err_msg=%s" % (custom_err_msg))
          raise Exception (custom_err_msg)

      # once instance state is reached, check vexp_state['meta'] state is reached. (it's a little slower)
      if vexp_state['meta']:

          current_meta_state = {}

          debugg("debug message: wait_for_it(%s, %s, %s, %s) vexp_state[meta] loop." % (vdb_name, str(vexp_state), vttw, str(vinst)))

          host_name_key = vall_hosts[vindex]

          try:
              current_meta_state = get_db_meta_state(vdb_name)
              while (vexp_state['meta'] != current_meta_state[host_name_key]) and (time.time() < timeout):
                  time.sleep(int(loop_sleep_time))
                  current_meta_state = get_db_meta_state(vdb_name)
          except:
              custom_err_msg = 'Error[ wait_for_it() ]: waiting for %s current_meta_state: %s to change to expected: %s last current_meta_state: %s host_name_key: %s current time: %s time.out: %s' % (vobj, current_meta_state[host_name_key], vexp_state['meta'], current_meta_state[host_name_key], host_name_key, str(time.time()), str(timeout))
              custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
              debugg("wait_for_it() ERROR[4] during second wait.. custom_err_msg=%s" % (custom_err_msg))
              raise Exception (custom_err_msg)

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      custom_err_msg = " Error[ wait_for_it() ]: time out occurred waiting for %s %s state to change executing: %s. Time to wait (ttw): %s. Additional info vexp_state: %s and actual current_state: %s vinst: %s current_meta_state: %s" % ( vobj, vdb_name, vcmd, str(vttw), str(vexp_state), str(current_state), str(vinst), str(current_meta_state) )
      custom_err_msg = custom_err_msg + msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
      debugg("wait_for_it() Timeout waiting for expected db state to be reached. custom_err_msg=%s" % (custom_err_msg))
      raise Exception (custom_err_msg)
    else:
        i = 0
        for ahost in vall_hosts:
            ansible_facts[ahost] = {'expected_state': vexp_state['exp_state'], 'current_state': current_state[i], 'current_meta_state': current_meta_state[ahost], 'expected_meta_state': vexp_state['meta']}
            i += 1
        debugg("wait_for_it() adding info to ansible_facts[%s] = %s" % (ahost,ansible_facts[ahost]))
        return(0)


def is_opt_valid(vopt,vcmd,majver):
    """Check that a given -stopoption | -startoption is valid. return 0 valid, 1 invalid."""
    debugg("is_opt_valid() Check that a given -stopoption | -startoption is valid. return 0 valid, 1 invalid. vopt=%s vcmd=%s majver=%s" % (vopt,vcmd,majver))
    # 0 valid, 1 invalid. NORMAL, TRANSACTIONAL LOCAL (not used), IMMEDIATE, or ABORT
    # This is a limited list. The full functionality of srvctl start/stop options is beyond this module.
    valid_stop_12c = ('normal','immediate','abort','local','transactional')
    valid_start_12c = ('open','mount','restrict','nomount','"read only"','write','"read write"') # ,'force'
    # https://docs.oracle.com/cd/E11882_01/rac.112/e41960/srvctladmin.htm#i1009484
    # https://docs.oracle.com/cd/E11882_01/server.112/e16604/ch_twelve042.htm#SQPUG125
    valid_stop_11g = ('normal','immediate','abort','transactional')
    # https://docs.oracle.com/cd/E11882_01/rac.112/e41960/srvctladmin.htm#i1009256
    # https://docs.oracle.com/cd/E11882_01/server.112/e16604/ch_twelve045.htm#SQPUG128
    valid_start_11g = ('open','mount','restrict','nomount','"read only"','write','"read write"','force') # ,'force'

    if majver == "12":
        if vcmd.lower() == "start":
            if vopt in valid_start_12c:
                return 0
        elif vcmd.lower() == "stop":
            if vopt in valid_stop_12c:
                return 0
    elif majver == "11":
        if vcmd.lower() == "start":
            if vopt in valid_start_11g:
                return 0
        elif vcmd.lower() == "stop":
            if vopt in valid_stop_11g:
                return 0

    debugg("is_opt_valid() exiting....returning 1")
    return 1


def is_rac():
    """Determine if a host is running RAC or Single Instance"""
    global err_msg
    debugg("is_rac()...start...")
    # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )
    vproc = run_command("ps -ef | grep lck | grep -v grep | wc -l")

    if int(vproc) > 0:
      # if > 0 "lck" processes running, it's RAC
      debugg("is_rac() exit...returning..True")
      return True
    else:
      debugg("is_rac() exit...returning..False")
      return False


def exec_db_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam=""):
    """Execute 11g srvctl command against a database """
    global grid_home
    global oracle_home
    global node_number
    global msg
    global debugme
    global oracle_sid
    vforce = ""

    debugg("exec_db_srvctl_11_cmd()...starting...")
    set_environmentals(vdb_name)

    if vparam and "force" in vparam:
        vforce = "-f"
    elif vparam and "force" not in vparam:
        tmpmsg = " Unknown parameter for 11g database: %s . Parameter ignored." % (vparam)
        add_to_msg(tmpmsg)

    if vstopt and vforce:
        cmd_str = "%s/bin/srvctl %s %s -d %s -o %s %s" % (oracle_home,vcmd,vobj,vdb_name,vstopt,vforce)
    elif vstopt and not vforce:
        cmd_str = "%s/bin/srvctl %s %s -d %s -o %s" % (oracle_home,vcmd,vobj,vdb_name,vstopt)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s" % (oracle_home,vcmd,vobj,vdb_name)

    debugg("def exec_db_srvctl_11_cmd(vdb_name=%s, vcmd=%s, vobj=%s, vstopt=%s, vparam=%s cmd_str=%s)" % (vdb_name,vcmd,vobj,vstopt,vparam,cmd_str))

    try:
        os.environ['USER'] = 'oracle'
        os.environ['ORACLE_HOME'] = oracle_home
        os.environ['ORACLE_SID'] = oracle_sid
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = "Error[ exec_db_srvctl_12_cmd() ]: executing srvctl command against %s %s. cmd_str: [%s] oracle_home: %s oracle_sid: %s" % (vobj,vdb_name,vcmd,oracle_home,oracle_sid)
        custom_err_msg = custom_err_msg + cmd_str
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    debugg("code %s output %s" % (code,output))

    return 0


def exec_inst_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst):
    """Execute 11g srvctl command against an instance"""
    global module
    global grid_home
    global oracle_home
    global node_number
    global oracle_sid
    global debugme
    global msg

    debugg("exec_inst_srvctl_11_cmd()...start....")
    set_environmentals(vdb_name)

    if vstopt and vparam:
        if vparam and "force" in vparam:
            vforce = "-f"
        elif vparam and "force" not in vparam:
            tmpmsg = " Unknown parameter for 11g database: %s . Parameter ignored." % (vparam)
            add_to_msg(tmpmsg)
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -o %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vstopt,vforce)
    elif vstopt:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -o %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vstopt)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst))

    debugg("exec_inst_srvctl_11_cmd() compiled cmd_str=%s" % (cmd_str))
    try:
        os.environ['USER'] = 'oracle'
        os.environ['ORACLE_HOME'] = oracle_home
        os.environ['ORACLE_SID'] = oracle_sid
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except: # Exception as e:
        custom_err_msg = 'Error[ exec_inst_srvctl_12_cmd() ]: executing srvctl command %s on %s %s with -%soption %s ' % (cmd_str, vobj, vdb_name, vcmd, vstopt)
        custom_err_msg = custom_err_msg + " " + cmd_str
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("exec_inst_srvctl_11_cmd() ERROR: cust_err_msg=%s" % (cust_err_msg))
        raise Exception (my_err_msg)

    debugg("code=%s output=%s cmd_str=%s" % (code,output,cmd_str))

    return 0


def exec_inst_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst):
    """Execute 12c srvctl command against an instance"""
    global module
    global grid_home
    global oracle_home
    global node_number
    global oracle_sid
    global debugme
    debugg("exec_inst_srvctl_12_cmd()...start...")
    set_environmentals(vdb_name)

    if vparam and vstopt:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -%soption %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vcmd,vstopt,vparam)
    elif vstopt and not vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -%soption %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vcmd,vstopt)
    elif vparam and not vstopt:
        cmd_str = cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -%s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vcmd,vparam)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s "  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst))
    debugg("exec_inst_srvctl_12_cmd() compiled cmd_str=%s" % (cmd_str))
    try:
        os.environ['USER'] = 'oracle'
        os.environ['ORACLE_HOME'] = oracle_home
        os.environ['ORACLE_SID'] = oracle_sid
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except: # Exception as e:
        custom_err_msg = 'Error[ exec_inst_srvctl_12_cmd() ]: executing srvctl command %s on %s %s with -%soption %s ' % (cmd_str, vobj, vdb_name, vcmd, vstopt)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("exec_inst_srvctl_12_cmd() ERROR: custom_err_msg=%s" % (custom_err_msg))
        raise Exception (my_err_msg)

    return 0


def exec_db_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam=""):
    """Execute 12c srvctl command against a database """
    global grid_home
    global oracle_home
    global node_number
    global debugme
    global oracle_sid
    debugg("exec_db_srvctl_12_cmd()...start....")
    set_environmentals(vdb_name)

    if vstopt and vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -%soption %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vcmd,vstopt,vparam)
    elif vstopt and not vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -%soption %s"  % (oracle_home,vcmd,vobj,vdb_name,vcmd,vstopt)
    elif vparam and not vstopt:
        cmd_str = "%s/bin/srvctl %s %s -d %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vparam)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name)
    debugg("exec_db_srvctl_12_cmd() compiled cmd_str=%s" % (cmd_str))
    try:
        os.environ['USER'] = 'oracle'
        os.environ['ORACLE_HOME'] = oracle_home
        os.environ['ORACLE_SID'] = oracle_sid
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = "Error[ exec_db_srvctl_12_cmd() ]: executing srvctl command against %s %s. cmd_str: [%s] oracle_home: %s oracle_sid: %s" % (vobj,vdb_name,vcmd,oracle_home,oracle_sid)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        debugg("exec_db_srvctl_12_cmd() ERROR: custom_err_msg=%s" % (custom_err_msg))
        raise Exception (custom_err_msg)

    debugg("exec_db_srvctl_12_cmd()....exiting...success")
    return 0


def set_environmentals(db_name):
    """Set program global variables grid_home, node_number, oracle_home, thishost (hostname), a list of all hosts (vall_hosts) and oracle_sid"""
    global grid_home
    global node_number
    global oracle_home
    global oracle_sid
    global thishost
    global vall_hosts
    debugg("set_environmentals()...start....setting global variables..")
    # collect environmental information needed to proceed.
    if not grid_home:
        grid_home = get_gihome()
        debugg("set_environmentals()..grid_home..")
    if not node_number and is_rac():
        node_number = get_node_num()
        debugg("set_environmentals()..node_number..")
    if not oracle_home:
        oracle_home = get_orahome_procid(db_name)
        debugg("set_environmentals()..oracle_home..")
    if not thishost:
        thishost = get_hostname()
        debugg("set_environmentals()..thishost..")
    if not vall_hosts:
        vall_hosts = list_all_hosts()
        debugg("set_environmentals()..vall_hosts..")
    if not oracle_sid:
        if is_rac():
            oracle_sid = db_name + str(node_number)
        else:
            oracle_sid = db_name
        debugg("set_environmentals()..oracle_sid..")

    return 0


def get_expected_state(vcmd, vstopt, majver):
    """Return dictionary object with the expected state based on object : ( instance | database ) and
       command ( start | stop ). meta ( mount, nomount etc. )."""
    global debugme
    debugg("get_expected_state()....start....determining what state to expect given the command")
    tmp_exp_state = {}

    # # only
    # if not vstopt and vcmd == "stop" and majver == "11":
    #     add_to_msg("no option specified for stop of 11g database, or instance. immediate assumed.")
    #     vstopt = "immediate"
    # elif majver == "11" and vcmd == "start" and vstopt != "open":
    #     add_to_msg("no option specified for start of 11g database, or instance. open assumed.")
    #     vstopt = "open"

    if vcmd.lower() == "stop":
        tmp_exp_state = {'exp_state': 'OFFLINE', 'meta': 'Instance Shutdown'}
    elif vcmd.lower() == "start":
        if vstopt:
          if vstopt.lower() == "nomount":
              tmp_exp_state = {'exp_state': 'INTERMEDIATE', 'meta': 'Dismounted'}
          elif vstopt.lower() ==  "mount":         # crsstat output : ora.tstdb.db   database   C ONLINE     INTERMEDIATE tlorad01     0  0 Mounted (Closed)
              tmp_exp_state = {'exp_state': 'INTERMEDIATE', 'meta': 'Mounted (Closed)'} # INTERMEDIATE
          elif vstopt.lower() == "open":
              tmp_exp_state = {'exp_state': 'ONLINE', 'meta': 'Open'}
          elif vstopt.lower() == '"read only"':
              tmp_exp_state = {'exp_state': 'ONLINE', 'meta': 'Open,Readonly'}
          elif vstopt.lower() == '"read write"':
              tmp_exp_state = {'exp_state': 'ONLINE', 'meta': 'Open'}
          elif vstopt.lower() == "restrict":
              tmp_exp_state = {'exp_state': 'INTERMEDIATE', 'meta': 'Restricted Access'}
        elif vstopt is None:
            tmp_exp_state = {'exp_state': 'ONLINE', 'meta': 'Open'}
                # Return dictionary with {state: value, meta: value}
    debugg("get_expected_state()...exit...expect states: %s" % (str(tmp_exp_state)))
    return (tmp_exp_state)


def get_db_meta_state(vdb_name):
    """return dictionary with key=host value=database current state. example: {'tlorad01': 'Instance Shutdown', 'tlorad02': 'Open'}
       Possible meta states: 'Open', 'Instance Shutdown', 'Mounted (Closed)'', 'Dismounted', 'Open,Readonly', 'Restricted Access' """
    global grid_home
    global debugme
    global thishost
    global vall_hosts

    debugg("get_db_meta_state()...start...")

    tmp_meta_state = {}

    if not thishost:
        vhostname = get_hostname()

    if not grid_home:
        grid_home = get_gihome()

    if vdb_name[-1].isdigit():
        vdb_name = local_db[:-1]

    if not vall_hosts:
        vall_hosts = list_all_hosts()

    for vhost in vall_hosts:

        cmd_str = grid_home + "/bin/crsctl status resource ora." + vdb_name + ".db -v -n " + vhost + " | grep STATE_DETAILS | cut -d '=' -f 2"

        try:
            process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
            output, code = process.communicate()
        except:
            custom_err_msg = ' Error[ get_db_meta_state() ]: retrieving STATE_DETAILS for local_db: %s using cmd_str: %s' % (local_db, cmd_str)
            custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            debugg("get_db_meta_state() ERROR: cmd_str=%s custom_err_msg=%s" % (cmd_str,custom_err_msg))
            raise Exception (custom_err_msg)

        meta_state = output.strip()

        tmp_meta_state[vhost] = meta_state

    debugg("get_db_meta_state() exit...returning dictionary of states: %s" % (str(tmp_meta_state)))
    return(tmp_meta_state)


def list_all_hosts():
    """Return a list of strings containing all nodes in the cluster with domain stripped off.
       [tlorad01,tlorad02]"""
    global all_nodes
    global grid_home
    debugg("list_all_hosts()...starting...")

    if not grid_home:
        grid_home = get_gihome()

    cmd_str = grid_home + "/bin/olsnodes -i | /bin/awk '{ print $1}'"

    try:
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       custom_err_msg = 'Error[ list_all_hosts() ]: retrieving a list of all hosts in the cluster. cmd_str: %s' % (cmd_str)
       custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       raise Exception (custom_err_msg)

    tmp_list = output.strip().split('\n')
    debugg("list_all_hosts()...exit....returning list of nodes in the cluster=%s" % (str(tmp_list)))
    return(tmp_list)


def extract_maj_version(ora_home):
    """Given an oracle_home string extract the major version number (i.e. 11, 12)"""
    debugg("extract_maj_version()...starting...")
    all_items = ora_home.split("/")

    for item in all_items:
        item.strip()
        if item and item[0].isdigit():
            major_ver = item.split(".")[0]
            debugg("extract_maj_version() exit...returning major_ver=%s" % (major_ver))
            return(major_ver)

    debugg("extract_maj_version() exit....ERROR...")
    return 1


# ===================================================================================================
# ==============================================  MAIN ==============================================
# ===================================================================================================
# Note use -eval with srvctl command to implement Ansible --check ??

def main ():
  """ Execute srvctl commands """
  # global vars
  global grid_home
  global node_number
  global oracle_home
  global vinst
  global default_ttw
  global debugme
  global debug_msg
  global msg
  global ansible_facts
  global truism
  global module

  # local vars
  custom_err_msg = ""
  vchanged = False
  no_action = False
  exe_results = ""

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),        # database name to run srvctl against
        cmd       = dict(required=True),        # command to execute: start | stop
        obj       = dict(required=True),        # object to operate against: database | instance
        inst      = dict(required=False),       # instance number if object is an instance
        stopt     = dict(required=False),       # -startoption / -stopoption : open | mount | nomount
        param     = dict(required=False),       # extra parameter for stop (last running instance etc.): -force
        ttw       = dict(required=False),        # Time To Wait (ttw) for srvctl command to change database state
        debugging = dict(required=False)
      ),
      supports_check_mode = False               # srvctl has '-eval' parameter. Use it to implement ???
  )

  # =============================== Start getting and checking module parameters ===================================
  # ** Note: parameters are passed as strings, even number parameters.
  # Get first 3 arguements passed from Ansible playbook. The only ones that are required.
  vdb_name      = module.params["db"]
  vcmd          = module.params["cmd"]
  vobj          = module.params["obj"]

  # Set debugging:
  vdebug        = module.params["debugging"]

  if vdebug in truism or '/' in vdebug:
      if vdebug in truism:
          debugme = True
      elif '/' in vdebug:
          debug_dir = vdebug
      elif not vdebug :
          debugme = False
      elif vdebug in ['default','Default','DEFAULT']:
          get_def_dir()
      else:
          add_to_msg("debugging parameter wasn't understood: debugging=%s" % (vdebug))


  # If db is not registered with srvctl return
  debugg("srvctl module: MAIN(): calling db_registered(%s)" % (vdb_name))
  dbreg = db_registered(vdb_name)

  if dbreg.lower() == "false":
      debugg("Database not registered with srvctl...exiting...")
      add_to_msg("Database not registered with srvctl.")
      module.exit_json(msg=msg, ansible_facts=ansible_facts , changed="False")

  # Ensure if object is an instance and this is a RAC and instance number wasn't defined raise exception
  if vobj.lower() == "instance" and is_rac():
        # See if instance number was defined.
        try:
            vinst = str(module.params["inst"])
            if vinst:
                inst_to_ck_indx = int(vinst) - 1
            else:
                sys.exit("Instance number needed for operations against an instance.")
        except:
            custom_err_msg = "ERROR[retrieving module parameters]: attempting operation against an %s but no %s number defined." % (vobj, vobj)
            custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            debugg("srvctl MAIN() ::  ERROR for instance: custom_err_msg=%s" % (custom_err_msg))
            raise Exception (custom_err_msg)

  # Else if object is a database and instance number passed ignore the instance number and tell user.
  elif vobj == "database" and vinst:

      add_to_msg("Passing an instance number when doing database operations is invalid. Instance number ignored.")

  # srvctl start | stop options (-startoption | -stopoption)
  try:
      vstopt = module.params["stopt"]
      if vstopt and vstopt in ['read only','read write']:
          # two word -startoptions | -stopoptions have to be quoted.
          vstopt = '"' + vstopt + '"'
  except:
      vstopt = ""

  # parameter for stop instance | database to cause failover or stop if no other instance running: -force
  # -force - This parameter fails the running services over to another instance. Services dont failover if -force not specified!
  try:
      vparam = module.params["param"]
  except:
      vparam = ""

  try:
      vttw = module.params["ttw"]
      if not vttw:
          vttw = default_ttw
  except:
      vttw = default_ttw

  # If debugging save current state of all variables:
  debugg("vdb_name: [%s], vcmd: [%s], vobj: [%s], vinst: [%s], vparam: [%s], vstopt: [%s], vttw: [%s], grid_home: [%s], node_number: [%s], oracle_home: [%s]" % (vdb_name,vcmd,vobj,vinst,vparam,vstopt,vttw,grid_home,node_number,oracle_home))

  # see if it's 11g database (no stopt) in 11g srvctl Commands
  tmp_str = get_orahome_procid(vdb_name)
  maj_ver = extract_maj_version(tmp_str)

  # 0 valid, 1 invalid. checked against a list of valid star and stop options per major version
  if vstopt:
      vresult = is_opt_valid(vstopt,vcmd,maj_ver)
      if vresult != 0 and maj_ver == "12":
          cust_msg = "The -%soption parameter passed (%s) was not valid for %s %s on a %s database. Error: invalid stopt option." % (vcmd,vstopt,vcmd,vobj,maj_ver)
          module.fail_json(msg=cust_msg,ansible_facts={},changed=False)
      elif vresult != 0 and maj_ver == "11":
          cust_msg = "The option parameter passed (%s) was not valid for %s %s on an %s database. Error: invalid stopt option." % (vstopt,vcmd,vobj,maj_ver)
          module.fail_json(msg=cust_msg,ansible_facts={},changed=False)

  # check if vparam given ck if its valid:
  if vparam and maj_ver == "12":
      if vparam in ["eval","force","verbose"]:
          vparam = "-" + vparam
      else:
          if vparam:
              tmp_msg = "invalid parameter for %s database ignored: [%s] " % (maj_ver, vparam)
              add_to_msg(tmp_msg)
  elif vparam and maj_ver == "11":
      if vparam != "force":
          tmp_msg = "invalid parameter for %s database ignored: [%s] " % (maj_ver, vparam)
          add_to_msg(tmp_msg)

  # set the expected object state given command and object
  vexpected_state = get_expected_state(vcmd,vstopt,maj_ver)

  # get the actual current state of the database
  current_state = get_db_state(vdb_name)

  if debugme:
      dbg_msg = "END PARAMETERS: db: %s cmd: %s obj: %s inst: %s stopt: %s param: %s ttw: %s" % (vdb_name, vcmd, vobj, vinst, vstopt, vparam, vttw)
  # ==========================================  END PARAMETERS  ===========================================

  # =========================================  START SRVCTL COMMAND  =======================================

  # If db not in future state already run srvctl command.
  if vobj.lower() == "database" and not all(item == vexpected_state['exp_state'] for item in current_state):

      debugg("maj_ver: %s vobj: %s expected_state: %s" % (maj_ver,vobj,str(vexpected_state)))

      if maj_ver == "12":
          exe_results = exec_db_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam)
      elif maj_ver == "11":
          exe_results = exec_db_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam)

      if exe_results:
          if exe_results == 0:
              vchanged = "True"

  # Else dealing with instance. Check current_state vs expected state. Run srvctl cmd if needed.
  elif vobj.lower() == "instance" and current_state[inst_to_ck_indx] != vexpected_state['exp_state']:

      if maj_ver == "12":
          exe_results = exec_inst_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst)
      elif maj_ver == "11":
          exe_results = exec_inst_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst)

      if exe_results == 0:
          vchanged = "True"

  else:
      no_action = True
          # The instance or database was already started or stopped.
      if vcmd.lower() == "start":
          vwording = "started"
      elif vcmd.lower() == "stop":
          vwording = "stopped"
      tmp_msg = "srvctl module complete. %s %s already %s. No action taken. %s current state: [%s] and expected was: %s" % (vdb_name, vobj, vwording, vdb_name, str(current_state), str(vexpected_state))
      add_to_msg(tmp_msg)


  if vchanged == "True":
      wait_results = wait_for_it(vdb_name, vobj, vexpected_state, vttw, vinst)

  if not no_action:
      if vcmd.lower() == "start":
          vwording = "started"
      elif vcmd.lower() == "stop":
          vwording = "stopped"
      tmp_msg = "srvctl module complete. %s %s %s. Expected state: %s reached." % (vdb_name, vobj, vwording, vexpected_state['exp_state'])
      add_to_msg(tmp_msg)

  module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)

if __name__ == '__main__':
    main()

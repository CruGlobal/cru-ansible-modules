#!/opt/rh/python27/root/usr/bin/python
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
        inst_no: 2
        stopt: immediate
        param: force
        ttw: 7
        debugging: True
      when: master_node                 Note: (1)

    values:
       db: database name
      cmd: [ start | stop ]
      obj: [ database | instance ]
  inst_no: [ valid instance number ]
    stopt: (stop options): [ normal | immediate | abort ]
           (start options): [ open | mount | nomount | restrict | read only | read write | write ]
    param: [ eval | force | verbose ]
      ttw: time to wait (in min) for status change after executing the command. Default 5.
debugging: debugging: True | False | directory - turns on all debugging outoput which is added to msg

    Notes:

        ** remember if running against 12c and up and you try to stop and instance you have to
           specify param: force just like executing the command manually using -force.

        (1) debugging parameter:
            True  - add debug info to msg output retruned when Ansible module completes
            False - no debugging info
            /dir/to/output.log - give absolute path to debugging log including log name.
                debugging info will be appeneded to the file as they execute.

        (2) Use when master_node else it may try to execute on all nodes simultaneously.

        (3) It's possible to start instance nomount, mount etc. but not to
            alter instance mount, or open. To open the instance using the srvctl module
            you must stop the instance then start instance mount, or start instance open.
            It is possible to "sqlplus> alter database mount" or "alter database open".
            The status change will then be reflected in crsstat.


        (4) It's possible to start instance nomount, mount etc. but not to
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
# default time to wait in minutes for db state to change
default_ttw = 5
# Time to wait in seconds beteen db state checks
loop_sleep_time = 2
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
istrue = ['True','TRUE','T','true','YES','Yes','yes','y']
rac = None


def add_to_msg(msg_str):
    """Add some info to the ansible_facts output message"""
    global msg

    if msg:
        msg = msg + " " + msg_str
    else:
        msg = msg_str


def debugg(debug_str):
    """add debugging info to msg if debugging=true"""
    global debugme

    if debugme:
        add_to_msg(debug_str)


def popen_cmd_str(cmd_str, oracle_home=None, oracle_sid=None):
    """Execute a command string and fail if necessary"""
    global module
    global msg

    try:
        os.environ['USER'] = 'oracle'
        if oracle_home:
            os.environ['ORACLE_HOME'] = oracle_home
        if oracle_sid:
            os.environ['ORACLE_SID'] = oracle_sid
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        add_to_msg("Error #1 [popen_cmd_str()]: retrieving hostname. cmd_str: %s " % (cmd_str))
        add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
        module.fail_json(msg=msg,ansible_facts={},changed=False)

    debugg("popen_cmd_str()...exit. returning output [%s]" % (output.strip()))

    return(output)


def db_registered(db_name):
    """Given a db name find out if it's registered to srvctl"""
    global oracle_home

    debugg("db_registered()...starting...db_name=%s" % (db_name))

    if not oracle_home:
        oracle_home = get_orahome_procid(db_name)

    debugg("db_registered()...calling popen_cmd_str() with >>>>>> oracle_home=%s" % (oracle_home))

    # Try Two ways to get oracle_home. If first doesn't work, try the second.
    if not oracle_home:
        oracle_home = get_orahome_procid(db_name)

    if not oracle_home:
        oracle_home = get_orahome_oratab(db_name)

    debugg("db_registered()...calling popen_cmd_str() with >>>>>> oracle_home=%s" % (oracle_home))

    # check oracle_home finally found.
    if oracle_home:
        output = popen_cmd_str("%s/bin/srvctl status database -d %s" % (oracle_home, db_name), oracle_home)
        tmp = output.strip()
        debugg("db_registered() output = %s" % (tmp))
        if 'PRCD' in tmp:
            return(False)
        else:
            return(True)
    else:
        debugg("db_registered() FALSE. db not registered with srvctl. No ")
        return(False)


def get_hostname():
    """Return the hostame"""
    global vdomain
    debugg("get_hostname()..starting...")

    output = popen_cmd_str("/bin/hostname | /bin/sed 's/" + vdomain + "//'")

    tmp_hostname = output.strip()

    debugg("get_hostname() exiting...returning %s" % (tmp_hostname))
    return(tmp_hostname)


def module_fail(msg_str=""):
    """An unrecoverable error occurred. Fail module execution"""
    global module
    global msg

    add_to_msg(msg_str)
    module.fail_json(msg=msg,ansible_facts={},changed=False)


def get_gihome():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""
    global grid_home

    output = popen_cmd_str("/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'")

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
        module_fail("SRVCTL MODULE ERROR: get_gihome() unable to determine grid_home ")

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

    output = popen_cmd_str("%s/bin/olsnodes -l -n | awk '{ print $2 }'" % (grid_home))

    debugg("get_node_num() output=%s" % (output))
    if output and output[-1].isdigit():
        node_number = int(output.strip()[-1])
    else:
        node_number = int(output.strip())

    debugg("get_node_num() executed this cmd: %s and determined node #: %s full output: %s" % (cmd_str, node_number, output))

    return(node_number)


def get_orahome_oratab(db_name):
    """Return database Oracle home from /etc/oratab"""
    global oracle_home

    debugg("get_orahome_oratab()..start....db_name=%s" % (db_name))

    debugg("get_orahome_oratab() calling popen_cmd_str()")
    output = popen_cmd_str("/bin/cat /etc/oratab | /bin/grep -m 1 " + db_name + " | /bin/grep -o -P '(?<=:).*(?<=:)' |  /bin/sed 's/\:$//g'")

    ora_home = output.strip()

    if not ora_home:
        module_fail('Error[ get_orahome_oratab(%s) ] ora_home null after f(x) execution for db_name: %s.' % (db_name,db_name))

    oracle_home = ora_home
    debugg("get_orahome_oratab()...exiting...returning ora_home=%s" % (ora_home))
    return(ora_home)


def get_orahome_procid(db_name):
    """Get database Oracle Home from the running process."""
    global oracle_home
    debugg("get_orahome_procid()....starting...db_name=%s\n" % (db_name))

    if oracle_home:
        debugg("get_orahome_procid()...oracle_home already defined. returning.")
        return

    # get the pmon process id for the running database.
    # 10189  tstdb1
    output = popen_cmd_str("/bin/pgrep -lf _pmon_" + db_name + " | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed")

    # if the database is down, but it possibly had an entry in /etc/oratab try this:
    if not output:
        debugg("get_orahome_procid()..calling...get_orahome_oratab()")
        tmp_orahome = get_orahome_oratab(db_name)
        if tmp_orahome:
            debugg("get_orahome_procid()....exiting...returning tmp_orahome=%s returned from get_orahome_oratab()" % (tmp_orahome))
            return(tmp_orahome)
        else:
            module_fail("Error retrieving oracle_home. No process id found and no /etc/oratab entry found for database: %s" % (db_name))

    try:
        # ['10189', 'tstdb1']
        vprocid = output.split()[0]
    except:
        add_to_msg("Error[ get_orahome_procid(db_name) ] error parsing process id for database: %s Full output: [%s]" % (db_name, output))
        module_fail("get_orahome_procid() failed splitting output to get procid.")

    # get Oracle home the db process is running out of
    # (0, ' /app/oracle/12.1.0.2/dbhome_1/')
    output = popen_cmd_str("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle//' ")

    ora_home = output.strip()

    if not ora_home:
        debugg("get_orahome_procid() - No ora_home to return using running process id. db may not be running...")
    else:
        debugg("get_orahome_procid()...exiting...returning ora_home=%s" % (ora_home))
    return(ora_home)


def get_db_state(db_name):
    """
    Return the status of the database on all nodes.
    list of strings with the status of the db on each node['INTERMEDIATE','ONLINE','OFFLINE']
    This function takes the db name as input with, or without the instance number attached
    """
    global grid_home
    node_status = []

    debugg("get_db_state()....starting...with db_name = %s" % (db_name))

    if not grid_home:
        grid_home = get_gihome()
        debugg("get_db_state()...grid_home wasn't set called get_gihome() => %s" % (grid_home))

    if not grid_home:
        debugg("get_db_state()..couldn't determine grid home. Exiting...")
        module_fail("Error[ get_db_state() ]: error determining grid_home from get_gihome() call. ")

    # check for special cases ASM and MGMTDB and see if db_name has digit (instance number), if so delete it. If not use it.
    if "ASM" in db_name:
        cmd_str = grid_home + "/bin/crsctl status resource ora.asm | grep STATE"
    elif "MGMTDB" in db_name:
        cmd_str = grid_home + "/bin/crsctl status resource ora.mgmtdb | grep STATE"
    elif db_name[-1].isdigit() :
        cmd_str = grid_home + "/bin/crsctl status resource ora." + db_name[:-1] + ".db | grep STATE"  # these give state of each node
    else:
        cmd_str = grid_home + "/bin/crsctl status resource ora." + db_name + ".db | grep STATE"

    debugg("get_db_state() cmd_str= %s" % (cmd_str))
    output = popen_cmd_str(cmd_str)
    debugg("get_db_state() popen_cmd_str() returned with output = %s" % (output))
    #  possible outputs:
    # STATE=INTERMEDIATE on tlorad01, INTERMEDIATE on tlorad02 ['STATE=OFFLINE', ' OFFLINE']   ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']  ['ONLINE on tlorad01', 'OFFLINE']  ['INTERMEDIATE', ' INTERMEDIATE on tlorad02']
    node_status = output.strip().split(",")
    debugg("get_db_state() output stripped node_status = %s" % (node_status))

    for i, item in enumerate(node_status, start=0):
        if "=" in item:
            node_status[i]=item.split("=")[1].strip()
        if " on " in node_status[i]:
            host_name = node_status[i].split(" on ")[1].strip()
            node_status[i] = node_status[i].split(" on ")[0].strip()
        else:
            node_status[i] = node_status[i].strip()

    debugg(" get_db_state() exit. db current status %s" % (str(node_status)))

    # this function returns a list of strings with host by index : index 0 = node 1, index 1 = node 2
    #                                              node1         node2
    # with the status of both (all) nodes: ie. ['INTERMEDIATE', 'OFFLINE']
    return(node_status)


def wait_for_it(vdb_name, vobj, vexp_state, vttw, vinst):
    """Compare current database (vdb_name) status of all nodes
       to expected state (vstatus) looping in 2 second intervals
       until state is reached or until time runs out (ttw min)"""

    global msg
    global vall_hosts
    global ansible_facts
    global loop_sleep_time
    # take current time and add 5 (vttw) minutes (60 * 5)
    # this will be time to stop if database expected state isn't reached.
    timeout =  time.time() + (60 * int(vttw))

    debugg("wait_for_it() called with vdb_name: [%s], vobj: [%s], vexp_state: [%s], vttw: [%s], vinst: [%s]" % (vdb_name,vobj,vexp_state,vttw,vinst))

    if vobj.lower() == "database":

        try:
            current_state = get_db_state(vdb_name)
            while (not all(item == vexp_state['exp_state'] for item in current_state) and (time.time() < timeout)):
                time.sleep(int(loop_sleep_time))
                current_state = get_db_state(vdb_name)
        except:
            add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            module_fail("SRVCTL MODULE ERROR [ wait_for_it() ]: waiting for %s state to reach: %s current state: %s " % ( vobj, vexp_state['exp_state'], str(current_state) ))

        if vexp_state['meta'] and is_rac():
            current_meta_state = get_db_meta_state(vdb_name)

            try:
                while (not all(item == vexp_state['meta'] for item in current_meta_state.values()) and (time.time() < timeout)):
                    time.sleep(int(loop_sleep_time))
                    current_meta_state = get_db_meta_state(vdb_name)
            except:
                add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
                module_fail('Error[ wait_for_it() ]: waiting for %s state to reach: %s current state: %s ' % (vobj,vexp_state['exp_state'], str(current_state) ))


    # else shutting down or starting an instance
    elif vobj.lower() == "instance":

      current_state = []

      # index of the instance to check
      vindex = int(vinst) - 1

      try:
        current_state = get_db_state(vdb_name)
        while (vexp_state['exp_state'] != current_state[vindex]) and (time.time() < timeout):
          time.sleep(int(loop_sleep_time))
          current_state = get_db_state(vdb_name)
      except:
        add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
        smodule_fail('Error[ wait_for_it() ]: error - waiting for %s state to change to %s last checked state: %s' % (vobj, vexp_state['exp_state'], current_state[vindex]))


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
              add_to_msg('Error[ wait_for_it() ]: waiting for %s current_meta_state: %s to change to expected: %s last current_meta_state: %s host_name_key: %s current time: %s time.out: %s' % (vobj, current_meta_state[host_name_key], vexp_state['meta'], current_meta_state[host_name_key], host_name_key, str(time.time()), str(timeout)))
              add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
              module_fail("Failed waiting on")

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      debugg(" Error[ wait_for_it() ]: time out occurred waiting for %s %s state to change executing: %s. Time to wait (ttw): %s. Additional info vexp_state: %s and actual current_state: %s vinst: %s current_meta_state: %s" % ( vobj, vdb_name, vcmd, str(vttw), str(vexp_state), str(current_state), str(vinst), str(current_meta_state) ))
      debugg(" %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
      raise Exception(msg)
    else:
        if is_rac():
            for i, ahost in enumerate(vall_hosts, start=0):
                ansible_facts[ahost] = {'expected_state': vexp_state['exp_state'], 'current_state': current_state[i], 'current_meta_state': current_meta_state[ahost], 'expected_meta_state': vexp_state['meta']}
        else:
            for i, ahost in enumerate(vall_hosts, start=0):
                ansible_facts[ahost] = {'expected_state': vexp_state['exp_state'], 'current_state': current_state[i]}
        return(0)


def is_opt_valid(vopt,vcmd,majver):
    """Check that a given -stopoption | -startoption is valid. return 0 valid, 1 invalid."""

    debugg("is_opt_valid()...starting...vopt=%s vcmd=%s majver=%s" % (vopt,vcmd,majver))
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
                return(True)
        elif vcmd.lower() == "stop":
            if vopt in valid_stop_12c:
                return(True)
    elif majver == "11":
        if vcmd.lower() == "start":
            if vopt in valid_start_11g:
                return(True)
        elif vcmd.lower() == "stop":
            if vopt in valid_stop_11g:
                return(True)
    debugg("is_opt_valid() exiting....returning False")
    return(False)


def is_rac():
    """Determine if a host is running RAC or Single Instance"""
    global err_msg
    global rac

    debugg("is_rac()...start...")

    if rac is None:
        # Determine if a host is Oracle RAC ( return 1 ) or Single Instance ( return 0 )

        vproc = popen_cmd_str("/bin/ps -ef | /bin/grep lck | /bin/grep -v grep | /bin/wc -l")

        if int(vproc) > 0:
          # if > 0 "lck" processes running, it's RAC
          debugg("is_rac() exit...returning..True")
          rac = True
          return True
        else:
          debugg("is_rac() exit...returning..False")
          rac = False
          return False

    else:

        debugg("is_rac() bypass returning rac = %s" % (rac))
        return(rac)


def exec_db_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt="", vparam=""):
    """Execute 11g srvctl command against a database:
        vdb_name - database name
        vcmd     - start | stop etc.
        vobj     - database | instance
        vstopt   - stop|start option - i.e. immediate
        vparam   - i.e. -force"""

    global grid_home
    global oracle_home
    global node_number
    global msg
    global debugme
    global oracle_sid
    vforce = ""

    debugg("exec_db_srvctl_11_cmd()...starting...")
    set_global_vars(vdb_name)

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

    debugg("exec_db_srvctl_11_cmd(vdb_name=%s, vcmd=%s, vobj=%s, vstopt=%s, vparam=%s)" % (vdb_name,vcmd,vobj,vstopt,vparam))
    debugg(cmd_str)

    output = popen_cmd_str(cmd_str, oracle_home, oracle_sid)

    debugg("exec_db_srvctl_11_cmd() code %s" % (output))


    return 0


def exec_inst_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst):
    """Execute 11g srvctl command against an instance:
            vdb_name - database name
            vcmd     - start | stop etc.
            vobj     - database | instance
            vstopt   - stop|start option - i.e. immediate
            vparam   - i.e. -force
            vinst    - db1, db2
    """
    global module
    global grid_home
    global oracle_home
    global node_number
    global oracle_sid
    global debugme
    global msg

    debugg("exec_inst_srvctl_11_cmd()...starting...vdb_name=%s vcmd=%s vobj=%s vstopt=%s vparam=%s vinst=%s" % (vdb_name, vcmd, vobj, vstopt, vparam, vinst))

    set_global_vars(vdb_name)

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

    debugg("exec_inst_srvctl_11_cmd() cmd_str=%s" % (cmd_str))

    # def popen_cmd_str(cmd_str, oracle_home=None,oracle_sid=None):
    output = popen_cmd_str(cmd_str, oracle_home, oracle_sid)

    debugg("exec_inst_srvctl_11_cmd() output %s" % (output))

    return(True)


def exec_inst_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt="None", vparam="None", vinst="None"):
    """Execute 12c srvctl command against an instance"""
    global module
    global grid_home
    global oracle_home
    global node_number
    global oracle_sid
    global debugme
    debugg("exec_inst_srvctl_12_cmd()...starting...vdb_name=%s vcmd=%s vobj=%s vstopt=%s vparam=%s vinst=%s" % (vdb_name, vcmd, vobj, vstopt, vparam, vinst))
    set_global_vars(vdb_name)

    if vparam and vstopt:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -%soption %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vcmd,vstopt,vparam)
    elif vstopt and not vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s -%soption %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vcmd,vstopt)
    elif vparam and not vstopt:
        cmd_str = cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s %s"  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst),vparam)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s -i %s%s "  % (oracle_home,vcmd,vobj,vdb_name,vdb_name,str(vinst))

    output = popen_cmd_str(cmd_str)
    debugg("exec_inst_srvctl_12_cmd()...exit...")
    return(True)


def exec_db_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt="None", vparam="None"):
    """Execute 12c srvctl command against a database """
    global grid_home
    global oracle_home
    global node_number
    global debugme
    global oracle_sid
    debugg("exec_db_srvctl_12_cmd()...starting...vdb_name=%s vcmd=%s vobj=%s vstopt=%s vparam=%s vinst=%s" % (vdb_name, vcmd, vobj, vstopt, vparam, vinst))
    set_global_vars(vdb_name)

    if vstopt and vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -%soption %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vcmd,vstopt,vparam)
    elif vstopt and not vparam:
        cmd_str = "%s/bin/srvctl %s %s -d %s -%soption %s"  % (oracle_home,vcmd,vobj,vdb_name,vcmd,vstopt)
    elif vparam and not vstopt:
        cmd_str = "%s/bin/srvctl %s %s -d %s %s"  % (oracle_home,vcmd,vobj,vdb_name,vparam)
    else:
        cmd_str = "%s/bin/srvctl %s %s -d %s"  % (oracle_home,vcmd,vobj,vdb_name)

    output = popen_cmd_str(cmd_str, oracle_home, oracle_sid)
    debugg("exec_db_srvctl_12_cmd()...exit...")
    return(True)


def set_global_vars(db_name):
    """Set program global variables grid_home, node_number, oracle_home, thishost (hostname), a list of all hosts (vall_hosts) and oracle_sid"""
    global grid_home
    global node_number
    global oracle_home
    global oracle_sid
    global thishost
    global vall_hosts
    debugg("set_global_vars()...starting...db_name=%s" % (db_name))
    # collect environmental information needed to proceed.
    if not grid_home:
        grid_home = get_gihome()
        debugg("set_global_vars() grid_home set %s" % (grid_home))
    if not node_number:
        # get_node_num handles if RAC or not
        node_number = get_node_num()
        tmp = is_rac()
        debugg("set_global_vars() node_number = %s is_rac = %s" % (node_number, tmp))
    if not oracle_home:
        oracle_home = get_orahome_procid(db_name)
        debugg("set_global_vars() oracle_home set %s" % (oracle_home))
    if not thishost:
        thishost = get_hostname()
        debugg("set_global_vars() thishost set %s" % (thishost))
    if not vall_hosts:
        vall_hosts = list_all_hosts()
        debugg("set_global_vars() vall_hosts set %s" % (vall_hosts))
    if not oracle_sid:
            if is_rac():
                oracle_sid = db_name + str(node_number)
            else:
                oracle_sid = db_name
            debugg("set_global_vars() oracle_sid set %s" % (oracle_sid))

    debugg("set_global_vars()...exiting...")


def get_expected_state(vcmd, vstopt, majver):
    """Return dictionary object with the expected state based on object : ( instance | database ) and
       command ( start | stop ). meta ( mount, nomount etc. )."""
    global debugme
    debugg("get_expected_state()...starting...")
    tmp_exp_state = {}

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
    debugg("get_expected_state()...exit...returning tmp_exp_state=%s" % (str(tmp_exp_state)))
    return (tmp_exp_state)


def get_db_meta_state(vdb_name):
    """return dictionary with key=host value=database current state. example: {'tlorad01': 'Instance Shutdown', 'tlorad02': 'Open'}
       Possible meta states: 'Open', 'Instance Shutdown', 'Mounted (Closed)'', 'Dismounted', 'Open,Readonly', 'Restricted Access' """

    global grid_home
    global debugme
    global thishost
    global vall_hosts
    tmp_meta_state = {}

    debugg("get_db_meta_state()...starting....vdb_name=%s" % (vdb_name))

    if not thishost:
        vhostname = get_hostname()
    debugg("get_db_meta_state() thishost = %s" % (thishost))

    if not grid_home:
        grid_home = get_gihome()
    debugg("get_db_meta_state() grid_home = %s" % (grid_home))

    if vdb_name[-1].isdigit():
        vdb_name = vdb_name[:-1]
    else:
        vdb_name = vdb_name
    debugg("get_db_meta_state() vdb_name = %s" % (vdb_name))

    if not vall_hosts:
        vall_hosts = list_all_hosts()
    debugg("get_db_meta_state() vall_hosts = %s" % (vall_hosts))

    for vhost in vall_hosts:

        cmd_str = grid_home + "/bin/crsctl status resource ora." + vdb_name + ".db -v -n " + vhost + " | /bin/grep STATE_DETAILS | /bin/cut -d '=' -f 2"
        debugg("get_db_meta_state() cmd_str = %s" % (cmd_str))
        output = popen_cmd_str(cmd_str)
        debugg("get_db_meta_state() popen_cmd_str() returning output = %s " % (output))
        meta_state = output.strip()
        debugg("get_db_meta_state() tmp_meta_state[%s] = %s " % (vhost, meta_state))
        tmp_meta_state[vhost] = meta_state

    debugg("get_db_meta_state() tmp_meta_state = %s " % (str(tmp_meta_state)))
    debugg("get_db_meta_state()...exit....")
    return(tmp_meta_state)


def list_all_hosts():
    """Return a list of strings containing all nodes in the cluster with domain stripped off.
       [tlorad01,tlorad02]"""
    global all_nodes
    global grid_home
    debugg("list_all_hosts()..start...")

    if not grid_home:
        grid_home = get_gihome()
        debugg("list_all_hosts()..grid_home=%s" % (grid_home))

    cmd_str = grid_home + "/bin/olsnodes -i | /bin/awk '{ print $1}'"

    output = popen_cmd_str(cmd_str)

    tmp_list = output.strip().split('\n')

    debugg("list_all_hosts()..exit...tmp_list=%s" % (str(tmp_list)))
    return(tmp_list)


def extract_maj_version(ora_home):
    """Given an oracle_home string extract the major version number (i.e. 11, 12)"""
    debugg("extract_maj_version()..start...ora_home=%s" % (ora_home))
    all_items = ora_home.split("/")

    for item in all_items:
        item.strip()
        if item and item[0].isdigit():
            major_ver = item.split(".")[0]
            debugg("extract_maj_version()...exit..returning major_ver=%s" % (major_ver))
            return(major_ver)
    debugg("extract_maj_version()...exit..returning no major_ver from all_items=%s" % (str(all_items)))
    return(1)


# ===================================================================================================
#                                          MAIN
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
  global msg
  global ansible_facts
  global module

  # local vars
  custom_err_msg = ""
  vchanged = False
  no_action = False

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),        # database name to run srvctl against
        cmd       = dict(required=True),        # command to execute: start | stop
        obj       = dict(required=True),        # object to operate against: database | instance
        inst_no   = dict(required=False),       # instance number if object is an instance
        stopt     = dict(required=False),       # -startoption / -stopoption : open | mount | nomount
        param     = dict(required=False),       # extra parameter for stop (last running instance etc.): -force
        ttw       = dict(required=False),        # Time To Wait (ttw) for srvctl command to change database state
        debugging = dict(required=False)        # Turn on debugging output
      ),
      supports_check_mode = False               # srvctl has '-eval' parameter. Use it to implement ???
  )

  # =============================== Start getting and checking module parameters ===================================
  # ** Note: parameters are passed as strings, even number parameters.
  # Get first 3 arguements passed from Ansible playbook. The only ones that are required.
  vdb_name      = module.params["db"]
  vcmd          = module.params["cmd"]
  vobj          = module.params["obj"]
  vdebugging    = module.params["debugging"]

  if vdebugging:
    debugme = vdebugging
    debugg("MAIN()...start....")
    debugg("vdb_name=%s vcmd=%s vobj=%s vdebugging=%s" % (vdb_name, vcmd, vobj, vdebugging))

  # If db is not registered with srvctl return
  debugg("MAIN() calling db_registered()")
  dbreg = db_registered(vdb_name)
  if not dbreg:
      add_to_msg("Database not registered with srvctl. Cannot execute command.")
      module.exit_json(msg=msg, ansible_facts=ansible_facts , changed="False")

  debugg("MAIN() DB WAS REGISTERED...Check and ensure if object is an instance and instance number wasn't defined raise exception. oracle_home = %s" % (oracle_home))

  # Ensure if object is an instance and instance number wasn't defined raise exception
  if vobj is not None and vobj.lower() == "instance":
        # See if instance number was defined.
        try:
            vinst = str(module.params["inst_no"])
            if vinst:
                inst_to_ck_indx = int(vinst) - 1
            else:
                module_fail("Instance number needed for operations against an instance.")
        except:
            add_to_msg("ERROR[retrieving module parameters]: attempting operation against an %s but no %s number defined." % (vobj, vobj))
            add_to_msg("%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            module_fail("Error determining instance.")

  # Else if object is a database and instance number passed ignore the instance number and tell user.
  elif vobj == "database" and str(vinst):
      add_to_msg("Passing an instance number when doing database operations is invalid. Instance number ignored.")

  debugg("MAIN() check srvctl start | stop options (-startoption | -stopoption)")
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
  debugg("MAIN() Check param")
  try:
      vparam = module.params["param"]
  except:
      vparam = ""

  debugg("MAIN() get ttw or use default")
  try:
      vttw = module.params["ttw"]
      if not vttw:
          vttw = default_ttw
  except:
      vttw = default_ttw

  # If debugging save current state of all variables:
  debugg("vdb_name: [%s], vcmd: [%s], vobj: [%s], vinst: [%s], vparam: [%s], vstopt: [%s], vttw: [%s], grid_home: [%s], node_number: [%s], oracle_home: [%s]" % (vdb_name,vcmd,vobj,vinst,vparam,vstopt,vttw,grid_home,node_number,oracle_home))

  # # see if it's 11g database (no stopt) in 11g srvctl Commands
  # if not oracle_home:
  #     oracle_home = get_orahome_procid(vdb_name)
  debugg("MAIN() calling extract_maj_version(%s)" % (oracle_home))
  maj_ver = extract_maj_version(oracle_home)
  debugg("MAIN() extracted maj_version = %s" % (maj_ver))

  # 0 valid, 1 invalid. checked against a list of valid star and stop options per major version
  if vstopt:
      debugg("MAIN() checking start/stop options")
      vresult = is_opt_valid(vstopt,vcmd,maj_ver)
      if not vresult and maj_ver == "12":
          cust_msg = "The -%soption parameter passed (%s) was not valid for %s %s on a %s database. Error: invalid stopt option." % (vcmd,vstopt,vcmd,vobj,maj_ver)
          module.fail_json(msg=cust_msg,ansible_facts={},changed=False)
      elif vresult and maj_ver == "11":
          cust_msg = "The option parameter passed (%s) was not valid for %s %s on an %s database. Error: invalid stopt option." % (vstopt,vcmd,vobj,maj_ver)
          module.fail_json(msg=cust_msg,ansible_facts={},changed=False)

  # check if vparam given ck if its valid:
  if vparam and maj_ver == "12":
      debugg("MAIN() checking parameters for version 12")
      if vparam in ["eval","force","verbose"]:
          vparam = "-" + vparam
      else:
          if vparam:
              tmp_msg = "invalid parameter for %s database ignored: [%s] " % (maj_ver, vparam)
              add_to_msg(tmp_msg)
  elif vparam and maj_ver == "11":
      debugg("MAIN() checking parameters for version 11")
      if vparam != "force":
          tmp_msg = "invalid parameter for %s database ignored: [%s] " % (maj_ver, vparam)
          add_to_msg(tmp_msg)

  # set the expected object state given command and object
  vexpected_state = get_expected_state(vcmd,vstopt,maj_ver)
  debugg("MAIN() vexpected_state = %s" % (str(vexpected_state)))

  # get the actual current state of the database
  current_state = get_db_state(vdb_name)
  debugg("MAIN() current_state = %s" % (current_state))

  debugg("END PARAMETERS:vdb_name: [%s], vcmd: [%s], vobj: [%s], vinst: [%s], vparam: [%s], vstopt: [%s], vttw: [%s], grid_home: [%s], node_number: [%s], oracle_home: [%s]" % (vdb_name,vcmd,vobj,vinst,vparam,vstopt,vttw,grid_home,node_number,oracle_home))
  # ==========================================  END PARAMETERS  ===========================================

  # =========================================  START SRVCTL COMMAND  =======================================
  debugg("MAIN() start srvctl command section....")

  # If db not in future state already run srvctl command.
  if vobj is not None and ( vobj.lower() == "database" and not all(item == vexpected_state['exp_state'] for item in current_state)):
      debugg("MAIN() If db not in future state already run srvctl command....maj_ver: %s vobj: %s expected_state: %s current_state = %s" % (maj_ver, vobj, str(vexpected_state),str(current_state)))

      if maj_ver in ["12","18","19"]:
          debugg("maj_ver in ['12','18','19']: call exec_db_srvctl_12_cmd() with vdb_name=%s vcmd=%s vobj=%s vstopt=%s vparam=%s" % (vdb_name, vcmd, vobj, vstopt, vparam))
          vchanged = exec_db_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam)
      elif maj_ver == "11":
          debugg("maj_ver 11: call exec_db_srvctl_12_cmd() with vdb_name=%s vcmd=%s vobj=%s vstopt=%s vparam=%s" % (vdb_name, vcmd, vobj, vstopt, vparam))
          vchanged = exec_db_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam)

  # Else dealing with instance. Check current_state vs expected state. Run srvctl cmd if needed.
  elif vobj is not None and (vobj.lower() == "instance" and current_state[inst_to_ck_indx] != vexpected_state['exp_state']):
      debugg("MAIN() Else dealing with instance. Check current_state vs expected state. Run srvctl cmd if needed....maj_ver: %s vobj: %s expected_state: %s" % (maj_ver,vobj,str(vexpected_state)))
      if maj_ver in ["12","18","19"]:
          vchanged = exec_inst_srvctl_12_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst)
      elif maj_ver == "11":
          vchanged = exec_inst_srvctl_11_cmd(vdb_name, vcmd, vobj, vstopt, vparam, vinst)

  else:
      debugg("MAIN() else...no action..")
      no_action = True
          # The instance or database was already started or stopped.
      if vcmd.lower() == "start":
          vwording = "started"
      elif vcmd.lower() == "stop":
          vwording = "stopped"
      tmp_msg = "srvctl module complete. %s %s already %s. No action taken. %s current state: [%s] and expected was: %s" % (vdb_name, vobj, vwording, vdb_name, str(current_state), str(vexpected_state))
      add_to_msg(tmp_msg)


  if vchanged:
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

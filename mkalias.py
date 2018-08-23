#!/opt/rh/python27/root/usr/bin/python
# -*- coding: utf-8 -*-
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
from ansible.module_utils._text import to_native
from ansible.module_utils._text import to_text
# from ansible.error import AnsibleError
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
import math
import time
import pexpect
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)

#Global variables
oracle_home=""
err_msg = ""
msg = ""
DebugMe = False
sleep_time = 2
default_ttw = 2
default_expected_num_reg_lsnrs = 1
grid_home = ""
node_number = ""
# number of registered listeners: currently 2 ( UNKNOWN and BLOCKED )
# [oracle@tlorad01]:tstdb1:/u01/oracle/ansible_stage/utils/tstdb/dup/2018-08-12> lsnrctl status | grep tstdb
# Service "tstdb.ccci.org" has 2 instance(s).
#   Instance "tstdb1", status UNKNOWN, has 1 handler(s) for this service...
#   Instance "tstdb1", status BLOCKED, has 1 handler(s) for this service...


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: mkalias
short_description: Given ASM diskgroup and database name it looks for the most
   recent spfile in ASM, drops the old alias and maps a new alias to this latest
   spfile.

'''

EXAMPLES = '''

    # when standing up a new database using restore, or clone etc.
    # this will look in asm for a new spfile and create an alias to it.
    - name: Map new alias to spfile
      mkalias:
        db_name: "{{ db_name }}"
        asm_dg: "{{ asm_db_name }}"
      when: master_node

'''


def get_grid_home():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""

    global grid_home
    # global module

    try:
        cmd_str = "/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        custom_err_msg = 'Error [ get_gihome() ]: retrieving GRID_HOME. Error running cmd: %s' % (cmd_str)
        custom_err_msg = custom_err_msg + " %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         custom_err_msg = ' Error[ get_gihome() ]: No output returned after running cmd : %s' % (cmd_str)
         custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
         raise Exception (custom_err_msg)

    return(grid_home)


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global node_number

    if not grid_home:
        grid_home = get_grid_home()

    try:
      cmd_str = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"
      process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = err_msg + ' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    node_number = int(output.strip())

    return(node_number)


def get_dbhome(vdb):
    """Return database home as recorded in /etc/oratab"""
    global ora_home

    cmd_str = "cat /etc/oratab | grep -m 1 %s | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'" % (vdb)

    try:
        process = subprocess.Popen([cmd_str], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_msg = my_msg + ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
       raise Exception (my_msg)

    ora_home = output.strip()

    if not ora_home:
        my_msg = ' Error[get_dbhome()]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
        my_msg = my_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_msg, sys.exc_info()[2])
        raise Exception (my_msg)

    return(ora_home)


def get_orahome_procid(vdb):
    """Get database Oracle Home from the running process."""
    global msg

    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
        cmd_str = "pgrep -lf _pmon_%s | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed" % (vdb)
        vproc = str(commands.getstatusoutput(cmd_str)[1])
    except:
        err_cust_err_msg = 'Error: get_orahome_procid() - pgrep lf pmon: (%s)' % (sys.exc_info()[0])
        err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    # if the database isnt running (no process id)
    # try getting oracle_home from /etc/oratab
    if not vproc:
        tmp_home = get_dbhome(vdb)
        if tmp_home:
            return tmp_home
        else:
            exit_msg = "Error determining oracle_home for database: %s all attempts failed! (proc id, srvctl, /etc/oratab)"
            sys.exit(exit_msg)

    # ['10189', 'tstdb1']
    vprocid = vproc.split()[0]

    # get Oracle home the db process is running out of
    # (0, ' /app/oracle/12.1.0.2/dbhome_1/')
    try:
      vhome = str(commands.getstatusoutput("sudo ls -l /proc/" + vprocid + "/exe | awk -F'>' '{ print $2 }' | sed 's/\/bin\/oracle$//' ")[1])
    except:
      custom_err_msg = 'Error[ get_orahome_procid() ]:  (%s)' % (sys.exc_info()[0])
      err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    ora_home = vhome.strip()

    # msg = msg + "exiting get_orahome_procid(%s) returning: ora_home: %s" % (vdb,ora_home)

    return(ora_home)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Check the lsnrctl state using command line """
    global msg
    global err_msg
    global grid_home

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name         = dict(required=True),
        asm_dg          = dict(required=True)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb          = module.params["db_name"]
    vasm_dg      = module.params["asm_dg"]

    if not grid_home:
        grid_home = get_grid_home()

    node_num = get_node_num()

    if not db_name[-1].isdigit():
        oracle_sid = db_name + node_num
    else:
        oracle_sid = db_name

    try:
        os.environ['USER'] = 'oracle'
        os.environ['ORACLE_HOME'] = grid_home
        os.environ['ORACLE_SID'] = oracle_sid
        cmd_str = "ls -l %s/%s/parameterfile/" % (vasm_dg,vdb)
    except:
        custom_err_msg = 'Error[ lsnr_wait() ]: waiting for %s database to register with lsnrctl. current_count %s < expected_num_reg_lsnrs %s and time.time() %s < timeout %s oracle_home %s msg: %s' % (vdb,current_count,expected_num_reg_lsnrs,time.time(),timeout,oracle_home,msg)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    msg = msg + "module lsnr_up exiting. For %s current_count %s < expected_num_reg_lsnrs %s ttw %s current time %s < timeout %s" % (vdb,current_count,v_entries,ttw,time.time(),timeout)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()

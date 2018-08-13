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
# from datetime import datetime, date, time, timedelta
from subprocess import (PIPE, Popen)
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)

#Global variables
oracle_home=""
err_msg = ""
msg = ""
DebugMe = False
sleep_time = 2
default_ttw = 5
# number of registered listeners: currently 2 ( UNKNOWN and BLOCKED )
# [oracle@tlorad01]:tstdb1:/u01/oracle/ansible_stage/utils/tstdb/dup/2018-08-12> lsnrctl status | grep tstdb
# Service "tstdb.ccci.org" has 2 instance(s).
#   Instance "tstdb1", status UNKNOWN, has 1 handler(s) for this service...
#   Instance "tstdb1", status BLOCKED, has 1 handler(s) for this service...
num_regs = 2


ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: rdssnap
short_description: Manually create an AWS RDS Snapshot.

'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - name wait for database to register with local listener
      lsnr_up:
        db_name: "{{ db_name }}"
        ttw: 5
      when: master_node

    Notes:
        This module checks for two instances of the database to register with
        the local listener and then returns.

        It uses the following test:
            snrctl status | grep %s | grep Instance | wc -l

            looking for :

                Instance "tstdb1", status UNKNOWN, has 1 handler(s) for this service...
                Instance "tstdb1", status BLOCKED, has 1 handler(s) for this service...

                one is the database registering with lsnrctl and the other is the listener.ora entry's registering.
'''


def get_dbhome(local_vdb):
    """Return database home as recorded in /etc/oratab"""
    global my_msg
    global ora_home

    cmd_str = "cat /etc/oratab | grep -m 1 " + local_vdb + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"

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
    global oracle_home

    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_" + vdb + " | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed")[1])
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

    return(oracle_home)


def num_listeners(vdb):
    """Return the number of listeners"""
    global oracle_home

    if not oracle_home:
      oracle_home = get_orahome_procid(vdb)

    if vdb[-1].isdigit():
        vdb = vdb[:-1]

    try:
      tmp_cmd = "%s/bin/lsnrctl status | grep %s | grep Instance | wc -l" % (oracle_home,vdb)
    except:
      err_msg = ' Error trying to concatenate the following: vdb_inst_id: [ %s ] and vdb_snap_id: [ %s ]' % (vdb_inst_id,vdb_snap_id)
      err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        err_msg = ' Error [1]: orafacts module get_meta_data() output: %s' % (output)
        err_msg = err_msg + "%s, %s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    num_listeners = output.strip()

    return (num_listeners)

# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Check the lsnrctl state using command line """
    global msg
    global err_msg
    global oracle_home
    global sleep_time
    global default_ttw
    global num_regs

    ansible_facts={}

    module = AnsibleModule(
      argument_spec = dict(
        db_name         = dict(required=True),
        ttw             = dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdb  = module.params["db_name"]
    vttw = module.params["ttw"]

    # See if a snapshot name was passed in
    # if not get the timestamp to create one
    if vdb[-1].isdigit():
        vdb = vdb[:-1]

    if not vttw:
        ttw = default_ttw
    else:
        ttw = vttw

    timeout =  time.time() + (60 * int(ttw))

    try:
        current_count = num_listeners(vdb)
        while (current_count < num_regs ) and (time.time() < timeout):
            time.sleep(int(sleep_time))
            current_count = num_listeners(vdb)
    except:
        custom_err_msg = 'Error[ lsnr_wait() ]: waiting for %s database to register with lsnrctl.' % (vdb)
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    msg = "lsnr_up exiting. %s is up and %s listeners registered with lsnrctl. ttw %s timeout %s" % (vdb,current_count,ttw,timeout)

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts={} , changed=True)

# code to execute if this program is called directly
if __name__ == "__main__":
   main()

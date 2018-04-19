#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import subprocess
import sys
import os
import json
import re                           # regular expression
import math
import time
from subprocess import (PIPE, Popen)
import datetime
from datetime import datetime, date, time, timedelta

# Global variables
vcmd = ""
vobj = ""
vdb = ""
err_msg = ""
grid_home = ""
oracle_home = ""
vchanged = ""
node_number = ""
# Time to Wait (ttw) for Status in min
myttw = 4

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: sourcefacts
short_description: Get Oracle Database facts from a remote database.
(remote database = a database not in the group being operated on)

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - name: start database
      srvctl:
        cmd: start
        obj: database
         db: tstdb
      become_user: "{{ remote_user }}"
      register: src_facts

    values:
      cmd: [ start | stop ]
      obj: [ database | instance ]
       db: database name

'''

def get_gihome():
    """Determine the Grid Home directory"""

    # gi_home=str(commands.getstatusoutput("dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}'")[1])
    try:
      process = subprocess.Popen(["dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    return((output.strip()).replace('/bin', '')) #/app/12.1.0.2/grid


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home

    if not grid_home:
        get_gihome()

    try:
      process = subprocess.Popen([grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    node_number = output.strip()

    return(node_number)


def get_orahome(loclal_vdb):
    """Return database home as recorded in /etc/oratab"""

    try:
        process = subprocess.Popen(["cat /etc/oratab | grep -m 1 " + loclal_vdb + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_orahome() retrieving ORACLE_HOME from /etc/oratab : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    return(output.strip())


def get_db_status(local_vdb):
    """Return the status of the database on every node"""
    global grid_home

    dbStatus = {}

    if not grid_home:
        get_gihome()

    try:
      # for Python3 look at subprocess.check_output
      # t_status=str(commands.getstatusoutput(vgihome + "/bin/crsctl status resource ora." + vdb + ".db | grep STATE")[1])
      process = subprocess.Popen([ grid_home + "/bin/crsctl status resource ora." + local_vdb + ".db | grep STATE"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' Error: get_db_status() : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    node_status=output.strip().split(",")             #  ['STATE=OFFLINE', ' OFFLINE']
    node_status[1]=node_status[1].strip()       # removes space in front of " OFFLINE"
    node_status[0]=node_status[0].split("=")[1] # splits STATE and OFFLINE and returns OFFLINE

    # Returning an array of states. one state for each node.
    return(node_status)


def wait_for_status(vdb, vstatus):
    """Compare database status of both nodes to expected status. Loop in 5 second intervals until state obtained"""
    start_time =  datetime.now()

    while not all(item == vstatus for item in get_db_status(vdb)) and (datetime.now() - start_time) > myttw:
        time.sleep(2)


# ===================================================================================
#                              MAIN
# ===================================================================================
def main ():
  """ Execute srvctl commands """

  global grid_home
  global node_number
  global oracle_home
  msg = ""

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),
        cmd       = dict(required=True),
        obj       = dict(required=True)
      ),
      supports_check_mode = False
  )

  # Get arguements passed from Ansible playbook
  vdb  = module.params["db"]
  vcmd = module.params["cmd"]
  vobj = module.params["obj"]

  vchanged=False
  ansible_facts={}
  grid_home=get_gihome()
  node_number=get_node_num()
  oracle_home=get_orahome(vdb)

  if vobj == 'database':
    vopt = '-d'
  elif vobj == 'instance':
    vopt = '-i'
    vdb = vdb + "1"

  if vcmd == 'stop':
    exp_status="OFFLINE"
  elif vcmd == 'start':
    exp_status="ONLINE"


  tmp="parameters passed : %s %s %s %s %s %s %s" % (vdb, vcmd, vobj, grid_home, node_number, oracle_home, get_db_status(vdb))

  current_db_status = get_db_status(vdb)

  # if all nodes are NOT already in the state we're looking for the db execute svcttl the command
  if not all(item == exp_status for item in current_db_status):

    # Execute the srvctl command
    try:
      temp = str(commands.getstatusoutput("export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " " + vopt + " " + vdb)[1])
    except:
      err_msg = err_msg + ' Error: executing srvctl cmd : cmd %s vobj %s db %s opt %s sysinfo: %s' % (vcmd, vobj, vdb, vopt, sys.exc_info()[0])
      module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    # Once the command is executed wait for the proper state
    wait_for_status(vdb, exp_status)

    if vcmd == "start":
        vcmd = "started"
    elif vcmd == "stop":
        vcmd = "stopped"

    msg = "srvctl module complete. Database: " + vdb + " " + vcmd + "."
    vchanged = "True"

  else:

    msg = "database already " + current_status + " no action taken."
    vchanged = "False"


  module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)

if __name__ == '__main__':
    main()

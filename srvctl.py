#!/opt/rh/python27/root/usr/bin/python
# scl enable python27 bash
# export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
#
# Module for Ansible to execute srvctl commands
#
#
# Written by : Cru Ansible Module development team
#
#  To use your cutom module via command line pass it in to the playbook using:
#  --module-path custom_modules
#
# When the module is ready for deployment, put it in the module libarary
#
# This module will execute Oracle srvctl commands on an Oracle host
#
# For programming:
# ansible-playbook restore_db.yml -i cru_inventory --extra-vars="hosts=test_rac dest_db_name=testdb source_host=plorad01 source_db_name=jfprod" --tags "srvctl" --step -vvv
#
# The srvctl functionality to include: (to be checked off when implemented)
#  [ ]  1) srvctl start database
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
# [..] various imports
# from ansible.module_utils.basic import AnsibleModule
#
# Last updated Thursday 22, 2018    Sam Kohler
#
from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import commands
import json
import sys
import os
import os.path
import subprocess
import time
from datetime import datetime
from subprocess import PIPE,Popen
import re
from datetime import datetime
from datetime import timedelta

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


# Global variables
vcmd = ""
vobj = ""
vdb = ""
err_msg = ""
gi_home = ""
ora_home = ""
vchanged = ""
nodenum = ""
# Time to Wait (ttw) for Status in min
myttw = 4

def get_gihome():
    """Determine the Grid Home directory"""

    # gi_home=str(commands.getstatusoutput("dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}'")[1])
    try:
      process = subprocess.Popen(["dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    return(gi_home.strip())


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global gi_home

    if gi_home is null:
        get_gihome()

    try:
      process = subprocess.Popen([gi_home + "bin/olsnodes -l -n | awk '{ print $2 }'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    return(nodenum.strip())


def get_orahome(vdb):
    """Return database home as recorded in /etc/oratab"""
    global ora_home

    try:
        process = subprocess.Popen(["cat /etc/oratab | grep -m 1 tstdb | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_orahome() retrieving ORACLE_HOME from /etc/oratab : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    ora_home = output.strip()
    return(ora_home)


def get_db_status(vdb):
    """Return the status of the database on every node"""
    dbStatus = {}

    vgihome = get_gihome()

    try:
      # for Python3 look at subprocess.check_output
      # t_status=str(commands.getstatusoutput(vgihome + "/bin/crsctl status resource ora." + vdb + ".db | grep STATE")[1])
      process = subprocess.Popen([ vgihome + "/bin/crsctl status resource ora." + vdb + ".db | grep STATE"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' Error: get_db_status() : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    node_status=output.strip().split(",")             #  ['STATE=OFFLINE', ' OFFLINE']
    node_status[1]=node_status[1].strip()       # removes space in front of " OFFLINE"
    node_status[0]=node_status[0].split("=")[1] # splits STATE and OFFLINE and returns OFFLINE
    i = 0
    while i < len(node_status):
        dbStatus[i]=node_status[i]
        i += 1

    return(dbStatus)


def wait_for_status(vnodes,vstatus):
    """Compare database status of both nodes to expected status. Loop in 5 second intervals until state obtained"""
    vduration =  time.time() + datetime.timedelta(minutes=myttws)
    nodematches = [False] * len(vnodes) # Each node assumed not to match the status we're looking for (vstatus)
    oneholdout = True   # as long as there is one node that doesn't match oneholdout=True

    # until all nodes match the status, or we reach duration start time plus ttw defined a the top
    while oneholdout and datetime.now() < vduration:
        i = 0
        for i < len(vnodes):
            if vnodes[i] != vstatus:
                nodematches[i] = False
            else:
                nodematches[i] = True

        # built in all() checks for all True
        if all(nodematches):
            oneholdout = False
        else:
            time.sleep(3)


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Execute srvctl commands """
    global gi_home
    global ora_home
    global nodenum

    ansible_facts={}

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = 'srvctl'

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
      argument_spec  = dict(
        cmd        =dict(required=True),
        obj        =dict(required=True),
        db         =dict(required=True)
      ),
      supports_check_mode=True,
    )

    # Get arguements passed from Ansible playbook
    vcmd = module.params.get('cmd')
    vobj = module.params.get('obj')
    vdb  = module.params.get('db')

    if vobj is 'database':
        vopt = '-d'
    elif vobj is 'instance':
        vopt = '-i'
        vdb = vdb + "1"

    if vcmd is 'stop':
        exp_status="OFFLINE"
    elif vcmd is 'start':
        exp_status="ONLINE"

    gi_home = get_gihome(vdb)
    ora_home = get_orahome(vdb)
    nodenum = get_node_num()

    # before executing a command get the db status
    current_status = get_db_status(vdb)

    if current_status != exp_sstatus:
        # Execute the srvctl command
        try:
            temp = str(commands.getstatusoutput("export ORACLE_SID=" + vdb + nodenum "; export ORACLE_HOME=" + ora_home + "; " + vdbhome + "/bin/srvctl " + vcmd + " " + vobj + " " + vopt + " " + vdb)[1])
        except:
            err_msg = err_msg + ' Error: srvctl module : cmd %s vobj %s db %s opt %s sysinfo: %s' % (vcmd, vobj, vdb, vopt, sys.exc_info()[0])
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


    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

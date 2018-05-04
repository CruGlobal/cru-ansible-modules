#!/opt/rh/python27/root/usr/bin/python

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
from subprocess import (PIPE, Popen)
# import datetime
# from datetime import datetime, date, time, timedelta

# Global variables
debugme = True
vcmd = ""
vobj = ""
vdb = ""
vinst = 0
msg=""
my_err_msg = ""
grid_home = ""
oracle_home = ""
vchanged = ""
node_number = ""
debugme_msg=""
# time to wait if status doesn't happen by this time abort
ttw_default = 5

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
        obj: instance
        db: tstdb
        inst: 2
        ttw: 7
      become_user: "{{ remote_user }}"
      register: src_facts

    values:
      cmd: [ start | stop ]
      obj: [ database | instance ]
       db: database name
     inst: 2
      ttw: time to wait (min) for status change after executing the command. Default 4.

'''


def get_gihome():
    """Determine the Grid Home directory"""
    global my_err_msg
    global grid_home

    # gi_home=str(commands.getstatusoutput("dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}'")[1])
    try:
       process = subprocess.Popen(["dirname $( ps -eo args | grep ocssd.bin | grep -v grep | awk '{print $1}')"], stdout=PIPE, stderr=PIPE, shell=True)
       output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error: srvctl module get_gihome() error - retrieving GRID_HOME excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    grid_home = (output.strip()).replace('/bin', '')

    return(grid_home) #/app/12.1.0.2/grid


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global my_err_msg
    global node_number
    global debugme_msg
    global msg
    vcmd = ""

    if not grid_home:
        get_gihome()

    try:
      vcmd = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"
      process = subprocess.Popen([vcmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    node_number = output

    if debugme:
        debugme_msg = debugme_msg + "node_status: %s " % (node_number)

    return(node_number)


def get_orahome(local_vdb):
    """Return database home as recorded in /etc/oratab"""
    global my_err_msg

    try:
        process = subprocess.Popen(["cat /etc/oratab | grep -m 1 " + local_vdb + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    return(output.strip())


def get_db_status(local_vdb):
    """Return the status of the database on every node"""
    global grid_home
    global debugme_msg
    global my_err_msg

    node_status = []
    # local_inst = int(local_inst)

    if not grid_home:
        grid_home = get_gihome()

    try:
      process = subprocess.Popen([ grid_home + "/bin/crsctl status resource ora." + local_vdb + ".db | grep STATE"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error: srvctl module get_db_status() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    node_status=output.split(",")                  #  ['STATE=OFFLINE', ' OFFLINE'] ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']

    i = 0
    for item in node_status:
      node_status[i]=item.strip()                # removes space in front of " OFFLINE"
      if "STATE=" in item:
          temp_item=item.split("=")[1]             # splits STATE and OFFLINE and returns OFFLINE
          if "ONLINE" in temp_item:
              node_status[i]=temp_item.split(" ")[0].strip().rstrip()
          else:
              node_status[i]=temp_item
      elif "ONLINE" in item:
          node_status[i]=item.split(" ")[1].rstrip()       # ['', 'ONLINE', 'on', 'tlorad02\n']
      i += 1

    if debugme:
        debugme_msg = debugme_msg + "node_status: %s " % (node_status)

    return(node_status)



def wait_for_status(vdb, vstatus, vttw, vinst):
    """Compare database status of both nodes to expected status. Loop in 5 second intervals until state obtained"""
    global vcmd
    global my_err_msg
    # take current time and add 5 minutes (5*60)
    # this will be the stop time
    timeout =  time.time() + (60 * int(ttw))
    current_status = []
    vindex = int(vinst) - 1
    if vinst == 0:
        vobj = "database"
    else:
        vobj = "instance "

    # If vinst is 0 we're shutting down / starting up the whole db, not an instance ** different comparison.
    if vinst == 0:
      try:
        while not all(item == vstatus for item in get_db_status(vdb)) and (time.time() < timeout):
          time.sleep(2)
      except:
          my_err_msg = my_err_msg + ' Error: srvctl module wait_for_status() error - waiting for complete database status to change to %s excpetion: %s' % (vstatus, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)
    else:
      try:
        current_status = get_db_status(vdb)
        while (vstatus != current_status[vindex]) and (time.time() < timeout):
          time.sleep(2)
          current_status = get_db_status(vdb)
      except:
          my_err_msg = my_err_msg + ' Error: srvctl module wait_for_status() error - waiting for instance status to change to %s excpetion: %s' % (vstatus, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      my_err_msg = my_err_msg + " Error: srvctl module wait_for_status() timed out waiting for %s %s status to change during %s. Time to wait was %s." % (vobj, vdb, vcmd, vttw)
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)
    else:
      return(0)


# ===================================================================================
#                              MAIN
# ===================================================================================
def main ():
  """ Execute srvctl commands """

  global grid_home
  global node_number
  global oracle_home
  global vcmd
  global vinst
  global my_err_msg
  msg = ""
  cmd_strng=""

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),
        cmd       = dict(required=True),
        obj       = dict(required=True),
        ttw       = dict(required=False),
        inst      = dict(required=False)
      ),
      supports_check_mode = False
  )

  # Get arguements passed from Ansible playbook
  vdb  = module.params["db"]
  vcmd = module.params["cmd"]
  vobj = module.params["obj"]

  # vobj == "instance":
  # set to 0 if none passed
  try:
    vinst = int(module.params["inst"])
  except:
    vinst = 0

  try:
    ttw = module.params["ttw"]
  except:
    ttw=ttw_default
    msg="Time to wait (ttw) not passed. Using default: 5 min."

  vchanged=False
  ansible_facts={}
  grid_home=get_gihome()
  node_number=get_node_num()
  oracle_home=get_orahome(vdb)

  # if operating on an instance but inst num not specified throw error
  if vobj == "instance" and vinst == 0:
      my_err_msg = my_err_msg + "Inst value not optional when executing commands against an instance. Please add valid instance number (ie. inst: 1) and try again."
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)

  # set ansible flags
  if vobj == 'database':
    vopt = '-d'
  elif vobj == 'instance':
    vopt1 = '-d'
    vopt2 = '-i'

  # load expected status based on command
  if vcmd == 'stop':
    exp_status="OFFLINE"
  elif vcmd == 'start':
    exp_status="ONLINE"

  if debugme:
    my_err_msg = my_err_msg + " parameters passed: vdb: %s vcmd: %s vobj: %s grid_home: %s node_number: %s oracle_home: %s get_db_status(): %s ttw: %s original status array: %s msg: %s" % (vdb, vcmd, vobj, grid_home, node_number, oracle_home, get_db_status(vdb), ttw, debugme_msg, msg)
    msg = msg + my_err_msg

  # if all nodes are NOT already in the state we're looking for execute srvctl command
  if not all(item == exp_status for item in get_db_status(vdb)):

    # If command against a database
    if vobj == "database":

      if debugme:
          my_err_msg = my_err_msg + "[99] export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " " + vopt + " " + vdb
          msg = msg + my_err_msg

      # Execute the srvctl command for stop / start database
      try:
        cmd_strng = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " " + vopt + " " + vdb
        process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
      except:
          my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s meta sysinfo: %s' % (vcmd, vobj, vopt, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)

    # If command against an instance
    elif vobj == "instance":

      inst_cmd = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " " + vopt1 + " " + vdb + " " + vopt2 + " " + vdb + str(vinst)

      # if stopping an instance and the other instance is offline you must use force option
      if vinst == 1:
        # index for inst 2 = 1
        other_inst = 1
      else:
        # index for inst 1 = 0
        other_inst = 0

      current_status = get_db_status(vdb)

      # if the instance is not already started or stopped execute the command
      if current_status[vinst-1] != exp_status:

        if (current_status[other_inst] == "OFFLINE") and (vcmd == "stop"):
          inst_cmd = inst_cmd + " -force"
        elif (current_status[other_inst] == "ONLINE") and (vcmd == "stop"):
          inst_cmd = inst_cmd + " -failover"

        if debugme:
          my_err_msg = my_err_msg + inst_cmd + (" >>> current_status contains : %s  current_status[other_inst] : %s <<<" % (current_status, current_status[other_inst]))
          msg = msg + my_err_msg

        # Execute the srvctl command for stop / start database
        try:
          process = subprocess.Popen([inst_cmd], stdout=PIPE, stderr=PIPE, shell=True)
          output, code = process.communicate()
        except: # Exception as e:
          my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s %s meta sysinfo: %s' % (vcmd, vobj, vopt1, vopt2, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)

        if debugme:
          my_err_msg = my_err_msg + ("output of inst_cmd : %s" % output)

        #Once the command is executed wait for the proper state
        whatstatus = wait_for_status(vdb, exp_status, ttw, vinst)

        vchanged = "True"

        if vcmd == "start":
          vcmd = "started"
        elif vcmd == "stop":
          vcmd = "stopped"

        if debugme:
          my_err_msg = my_err_msg + "after wait : whatstatus : %s and exp_status : %s " % (whatstatus, exp_status)
          msg = msg + "srvctl module complete. %s db %s. cmd %s meta %s extra: %s actual command : %s" % (vdb, exp_status, vcmd, my_err_msg, debugme_msg, cmd_strng) + my_err_msg

      else:
        # The instance was already started or stopped.
        vchanged = False
        if vcmd == "start":
          vcmd = ("already started. No action taken.")
        elif vcmd == "stop":
          vcmd = ("already stopped. No action taken.")

    msg = msg + "srvctl module complete. %s db %s. actual command %s" % (vdb, vcmd, cmd_strng)


  # Database already stopped, or started)
  else:

    current_db_status = get_db_status(vdb)

    if debugme:
      msg = msg + "Request to %s %s database already %s . No action taken. meta: %s " % (vcmd, vdb, current_db_status, debugme_msg)
    else:
      msg = msg + "Request to %s %s database already %s . No action taken." % (vcmd, vdb, current_db_status)

    vchanged = "False"


  module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)

if __name__ == '__main__':
    main()

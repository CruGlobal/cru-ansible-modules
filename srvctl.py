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
from __builtin__ import any as exists_in  # exist_in(word in x for x in mylist)
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
default_ttw = 5                                                                     # time to wait (in minutes) if status doesn't happen by this time abort
valid_inst_stopopts=( "normal", "transactional", "local", "immediate", "abort" )    # local only valid for instance
valid_startopts=("open", "mount", "nomount", "force", "restrict", "recover", "read only", "read write") # all good for instance, and db
valid_srvctl_options=("eval", "force", "verbose")                                   # eval only valid srvctl start option for database

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
        db: tstdb
        cmd: stop
        obj: instance
        inst: 2
        stopt: immediate
        opt: force
        ttw: 7
      become_user: "{{ remote_user }}"
      register: src_facts
      when: master_node (2)

    values:
       db: database name
      cmd: [ start | stop ]
      obj: [ database | instance ]
     inst: [ valid instance number ]
    stopt: (stop options): [ normal | immediate | transactional | abort | local (1) ]
           (start options): [ open | mount | nomount | force | restrict | recover ]
      opt: [ eval | force | verbose ]
      ttw: time to wait (in min) for status change after executing the command. Default 5.

      (1) local option only available for instance
      (2) when master_node else it may try to execute on all nodes

'''

def get_gihome():
    """Determine the Grid Home directory"""
    global grid_home

    try:
      process = subprocess.Popen(["/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        err_msg = err_msg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (err_msg), changed=False)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         err_msg = err_msg + ' Error[4]: srvctl module get_gihome() error - retrieving grid_home : %s output: %s' % (grid_home, output)
         err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
         raise Exception (err_msg)

    return(grid_home)


def get_node_num():
    """Return current node number to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global my_err_msg
    global node_number
    global debugme_msg
    global msg
    tmp_cmd = ""

    if not grid_home:
        grid_home = get_gihome()

    try:
      tmp_cmd = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error[3]: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    node_number = output.strip()

    if debugme:
        debugme_msg = debugme_msg + "get_node_num() this node #: %s " % (node_number)

    return(node_number)


def get_orahome(local_vdb):
    """Return database home as recorded in /etc/oratab"""
    global my_err_msg

    tmp_cmd = "cat /etc/oratab | grep -m 1 " + local_vdb + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"
    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    ora_home = output.strip()

    if not ora_home:
        my_err_msg = my_err_msg + ' Error[2]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
        my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
        raise Exception (my_err_msg)

    return(ora_home)


def get_db_status(local_vdb):
    """
    Return the status of the database on the node it runs onself.
    The db name can be passed with, or without the instance number attached
    """
    global grid_home
    global msg
    global debugme
    err_msg = ""
    node_status = []
    tmp_cmd = ""

    if not grid_home:
        grid_home = get_gihome()

    if not grid_home:
        err_msg = err_msg + ' Error[5]: orafacts module get_db_status() error - retrieving local_grid_home: %s' % (grid_home)
        err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
        raise Exception (err_msg)

    if "ASM" in local_vdb:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.asm | grep STATE"
    elif "MGMTDB" in local_vdb:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.mgmtdb | grep STATE"
    elif local_vdb[-1].isdigit() :
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_vdb[:-1] + ".db | grep STATE"
    else:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_vdb + ".db | grep STATE"

    if debugme:
        msg = msg + tmp_cmd

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = err_msg + ' Error[7]: srvctl module get_db_status() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    node_status=output.strip().split(",")                  #  ['STATE=OFFLINE', ' OFFLINE']      ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']      ['ONLINE on tlorad01', 'OFFLINE']\r\n",

    i = 0
    for item in node_status:
      if "STATE=" in item:
          node_status[i]=item.split("=")[1].strip()            # splits STATE and OFFLINE and returns status 'OFFLINE'
          if "ONLINE" in node_status[i]:
              node_status[i] = node_status[i].strip().split(" ")[0].strip().rstrip()
      elif "ONLINE" in item:
          node_status[i]=item.strip().split(" ")[0].strip().rstrip()
      elif "OFFLINE" in item:
          node_status[i]=item.strip().rstrip()
      i += 1


    return(node_status)


def wait_for_status(vdb, vstatus, vttw, vinst):
    """Compare database status of both nodes to expected status (vstatus). Loop in 2 second intervals until state obtained"""

    global my_err_msg
    # take current time and add 5 minutes (5*60)
    # this will be the stop time if state isn't reached
    timeout =  time.time() + (60 * int(vttw))
    current_status = []
    if vinst == 0:
        vobj = "database"
    else:
        vobj = "instance "

    # If vinst is 0 we're shutting down / starting up the whole db, not an instance ** different comparison.
    if vinst == 0:
      try:
        while not all(item == vstatus for item in get_db_status(vdb) and (time.time() < timeout)):
          time.sleep(2)
      except:
          my_err_msg = my_err_msg + ' Error[8]: srvctl module wait_for_status() error - waiting for complete database status to change to %s excpetion: %s' % (vstatus, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)
    else:
      vindex = int(vinst) - 1
      try:
        current_status = get_db_status(vdb)
        while (vstatus != current_status[vindex]) and (time.time() < timeout):
          time.sleep(2)
          current_status = get_db_status(vdb)
      except:
          my_err_msg = my_err_msg + ' Error[9]: srvctl module wait_for_status() error - waiting for instance status to change to %s excpetion: %s' % (vstatus, sys.exc_info()[0])
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      my_err_msg = my_err_msg + " Error[10]: srvctl module wait_for_status() timed out waiting for %s %s status to change during %s. Time to wait was %s." % (vobj, vdb, vcmd, vttw)
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)
    else:
      return(0)


# ===================================================================================================
#                                          MAIN
# ===================================================================================================

def main ():
  """ Execute srvctl commands """

  global grid_home
  global node_number
  global oracle_home
  global vcmd
  global vinst
  global my_err_msg
  ansible_facts={}
  msg = ""
  cmd_strng=""
  vinst=0

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),
        cmd       = dict(required=True),
        obj       = dict(required=True),
        inst      = dict(required=False),
        stopt     = dict(required=False),
        opt       = dict(required=False),
        ttw       = dict(required=False)
      ),
      supports_check_mode = False
  )

  # Get arguements passed from Ansible playbook
  vdb      = module.params["db"]
  vcmd     = module.params["cmd"]
  vobj     = module.params["obj"]

  # if instance specified, but none given error
  vinst = module.params["inst"]
  if vobj == "instance" and not vinst:
      my_err_msg = my_err_msg + "ERROR[11]: '%s' option requires valid instance number." % (vobj, vcmd)
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)
  elif vobj == "database" and vinst is not None:
      msg = msg + " Passing an instance number when doing database operations is invalid. Instance parameter ignored."
      vinst=0

  # Time to Wait parameter
  tmpttw = module.params["ttw"]
  if tmpttw is None:
      ttw=default_ttw
      msg="Time to wait (ttw) not passed. Using default: 5 min."
  else:
      ttw=tmpttw

  # startoption / stopoption
  vstopt = module.params["stopt"]
  if not vstopt and vcmd == "stop":
      # stop database no parameter passed, set default
      vstopt = "-stopoption immediate"
  # else if parameter isn't NULL, see if it's valid stop option
  # valid database stoptoptions are a subset of instance stoptoptions
  elif vcmd == "stop" and exists_in(vstopt in x for x in valid_inst_stopopts):
      if vobj == "database" and vstopt == "local":
          msg = msg + "WARNING[1]: %s option invalid during %s %s. Option ignored." % (vstopt, vobj, vcmd)
      else:
          vstopt = "-stopoption " + vstopt
  # else see if its a valid start option: open, mount, nomount
  elif vstopt and (vcmd == "start" and exists_in(vstopt in x for x in valid_startopts)):
      vstopt = "-startoption " + vstopt
  elif vstopt:
      msg = msg + "WARNING[2]: '%s' invalid option to use with %s. Option ignored." % (vstopt, vcmd)

  # srvctl options. If it's valid use it
  # stop "eval", "force", "verbose" for start only "eval"
  vopt = module.params["opt"]
  if vopt and exists_in(vopt in x for x in valid_srvctl_options):
      if vcmd == "stop":
          vopt = "-" + vopt
      elif vcmd == "start" and vopt == "eval":
          vopt = "-" + vopt
      else:
          msg = msg + "NOTICE [3]: %s option invalid for %s. Option ignored." % (vopt, vcmd)
          vopt=""

  # at this point there's been no change
  vchanged=False

  # collect information needed to proceed.
  grid_home=get_gihome()
  node_number=get_node_num()
  oracle_home=get_orahome(vdb)

  # if operating on an instance but inst num not specified throw error
  if vobj == "instance" and vinst == 0:
      my_err_msg = my_err_msg + "Inst value not optional when executing commands against an instance. Please add valid instance number (ie. inst: 1) and try again."
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)

  # load expected status based on command and options
  if vcmd == "stop":
      exp_status="OFFLINE"
  elif vcmd == 'start' and (vstopt == "nomount" or vstopt == "mount"):
      exp_status="INTERMEDIATE"
  elif vcmd == "start" or (vcmd == start and (vstopt == "read only" or vstopt == "read write")):
      exp_status="ONLINE"   # nomount = INTERMEDIATE

  if debugme:
      node_num=int(get_node_num())
      tmp_status=get_db_status(vdb)
      curr_stat=str(tmp_status[node_num])
      my_err_msg = my_err_msg + " parameters passed: vdb: %s vcmd: %s vobj: %s grid_home: %s node_number: %s oracle_home: %s get_db_status(): %s ttw: %s original status array: %s msg: %s" % (vdb, vcmd, vobj, grid_home, node_number, oracle_home, curr_stat, ttw, debugme_msg, msg)
      msg = msg + my_err_msg

  # if all nodes are NOT already in the state we're looking for execute srvctl command
  if not all(item == exp_status for item in get_db_status(vdb)):

      # Command against a database
      if vobj == "database":

          if debugme:
              my_err_msg = my_err_msg + cmd_strng
              msg = msg + my_err_msg

          # Execute the srvctl command for stop / start database
          #                                                                                                                         srvctl stop database -d tstdb -stopoption immediate
          if not vopt:
              cmd_strng = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " " + vstopt
          else:
              cmd_strng = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " " + vstopt + " " + vopt
          try:
              process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
              output, code = process.communicate()
          except:
              my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s meta sysinfo: %s' % (vcmd, vobj, vopt, sys.exc_info()[0])
              my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
              raise Exception (my_err_msg)

      # Command against an instance
      elif vobj == "instance":

          # if stopping an instance and the other instance is offline you must use force option
          if vinst == 1:
              # index for inst 2 = 1
              other_inst = 1
          else:
              # index for inst 1 = 0
              other_inst = 0

          # if the instance is not already started or stopped execute the command
          current_status = get_db_status(vdb)

          ckindx = int(vinst) - 1
          if current_status[ckindx] != exp_status:

              if (current_status[other_inst] == "OFFLINE") and (vcmd == "stop"):
                  if not vopt:
                      vopt = "-force"
                  else:
                      vopt = vopt + " -force"
              elif (current_status[other_inst] == "ONLINE") and (vcmd == "stop"):
                  if not vopt:
                      vopt = "-failover"
                  else:
                      vopt = vopt + " -failover"

              if debugme:
                  my_err_msg = my_err_msg + (" >>> current_status contains : %s  current_status[other_inst] : %s <<<" % (current_status, str(current_status[other_inst])))
                  msg = msg + my_err_msg

              # Execute the srvctl command for stop / start database
              #                                                                                                                        srvctl stop instance -d tstdb -i tstdb1 -failover
              if not vopt:
                  cmd_strng = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " -i " + vdb + str(vinst)
              else:
                  cmd_strng = "export ORACLE_SID=" + vdb + node_number + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " -i " + vdb + str(vinst) + " " + vopt

              try:
                  process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
                  output, code = process.communicate()
              except: # Exception as e:
                  my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s %s meta sysinfo: %s' % (vcmd, vobj, vopt1, vopt2, sys.exc_info()[0])
                  my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
                  raise Exception (my_err_msg)

              if debugme:
                  my_err_msg = my_err_msg + ("output of inst_cmd : %s cmd_string %s" % (output,cmd_strng))

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

          else: # else instance in state already. No action required

              current_db_status = get_db_status(vdb)

              if debugme:
                  msg = msg + "[DB1] Request to %s %s database already %s . No action taken. meta: %s " % (vcmd, vdb, str(current_db_status), debugme_msg)
              else:
                  msg = msg + "[DB2] Request to %s %s database already %s . No action taken." % (vcmd, vdb, str(current_db_status))

              # The instance was already started or stopped.
              vchanged = False
              if vcmd == "start":
                  vcmd = ("instance already started. No action taken.")
              elif vcmd == "stop":
                  vcmd = ("instance already stopped. No action taken.")

              msg = msg + "srvctl module complete. %s db %s. actual command %s" % (vdb, vcmd, cmd_strng)

              vchanged = "False"

              module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)

  # Database already stopped, or started)
  else:

      current_db_status = get_db_status(vdb)

      if debugme:
          msg = msg + "[DB3] Request to %s %s database already %s . No action taken. meta: %s " % (vcmd, vdb, str(current_db_status), debugme_msg)
      else:
          msg = msg + "[DB4] Request to %s %s database already %s . No action taken." % (vcmd, vdb, str(current_db_status))

      # The instance was already started or stopped.
      vchanged = False
      if vcmd == "start":
          vcmd = ("already started. No action taken.")
      elif vcmd == "stop":
          vcmd = ("already stopped. No action taken.")

      msg = msg + "srvctl module complete. %s db %s. actual command %s" % (vdb, vcmd, cmd_strng)

      vchanged = "False"


  module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)


if __name__ == '__main__':
    main()

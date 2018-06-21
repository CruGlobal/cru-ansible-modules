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

#  Notes: It's possible to get the database / instances into a screwed up state.
#  It's possible to start an instance in nomount, but then it is impossible to alter instance mount.
#  The database instances can be shutdown and then restarted in mount and open, but not altered, or modified.

# Global variables
debugme = True
vcmd = ""
vobj = ""
vdb = ""
vinst = 0
msg = ""
my_err_msg = ""
grid_home = ""
oracle_home = ""
vchanged = ""
node_number = ""
debugme_msg = ""
default_ttw = 5                                                                     # time to wait (in minutes) if status doesn't happen by this time abort
valid_inst_stopopts = ( "normal", "transactional", "local", "immediate", "abort" )    # local only valid for instance
valid_startopts = ("open", "mount", "nomount", "force", "restrict", "read only", "read write") # all good for instance, and db - "open recover" removed. https://docs.oracle.com/database/121/ADMIN/restart.htm#ADMIN5009
valid_srvctl_options = ("eval", "force", "verbose")                                   # eval only valid srvctl start option for database

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
      (3) be sure to use 'become_user: oracle' else errors due to access privileges will cause the module to fail.

   WARNING: It's possible to start instance nomount, mount etc. but not to
            alter instance mount, or open. To do this using the srvctl module
            you MUST stop the instance then start instance mount, or start instance (open).
            It is possible to "sqlplus> alter database mount" on an instance.
            The status change will then be reflected in crsstat.

'''

def get_gihome():
    """Determine the Grid Home directory"""
    global my_err_msg
    global grid_home

    try:
      process = subprocess.Popen(["/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        my_err_msg = err_my_err_msgmsg + ' get_gihome() retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (my_err_msg), changed=False)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         err_msg = err_msg + ' Error[4]: srvctl module get_gihome() error - retrieving grid_home : %s output: %s' % (grid_home, output)
         err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
         raise Exception (err_msg)

    return(grid_home)


def get_node_num():
    """Return current node number (int) to ensure that srvctl is only executed on one node (1)"""
    global grid_home
    global my_err_msg
    global node_number
    global debugme_msg
    global msg
    tmp_cmd = ""

    if not grid_home:
        grid_home = get_gihome()

    tmp_cmd = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error[3]: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    if output.strip()[-1].isdigit() :
        node_number = int(output.strip()[-1])

    if debugme:
        debugme_msg = debugme_msg + "get_node_num() this node #: %s cmd: %s output: %s" % (node_number, tmp_cmd, output)

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


def get_meta_data(local_db):
    """Return meta data for a database from crsctl status resource"""
    tokenstoget = ['TARGET', 'STATE', 'STATE_DETAILS']
    global grid_home
    global my_err_msg
    global msg
    metadata = {}

    if not grid_home:
        grid_home = get_gihome()

    tmp_cmd = "/bin/hostname | cut -d. -f1"

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error [1]: srvctl module get_orahome() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    node_name = output.strip()

    if local_db[-1].isdigit():
        local_db = local_db[:-1]

    tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_db + ".db -v -n " + node_name

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       my_err_msg = my_err_msg + ' Error [1]: srvctl module get_meta_data() output: %s' % (output)
       my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
       raise Exception (my_err_msg)

    try:
        for item in output.split('\n'):
            if item:
                vkey, vvalue = item.split('=')
                if vkey:
                    vkey = vkey.strip()
                else:
                    vkey = ""
                if vvalue:
                    vvalue = vvalue.strip()
                else:
                    vvalue = ""
                if "STATE=" in vvalue:
                    vvalue=vvalue.split("=")[1].strip()
                    if "ONLINE" in vvalue:
                        vvalue = vvalue.strip().split(" ")[0].strip().rstrip()
                elif "ONLINE" in vvalue:
                    vvalue=vvalue.strip().split(" ")[0].strip().rstrip()
                elif "OFFLINE" in vvalue:
                    vvalue=vvalue.strip().rstrip()

                if vkey in tokenstoget:
                    metadata[vkey] = vvalue
    except:
        my_err_msg = "ERROR: srvctl module get_meta_data() error - loading metadata dict: %s" % (str(metadata))
        my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
        raise Exception (my_err_msg)

    if debugme:
        msg = msg + " get_meta_data() metadata dictionary contents : %s" % (str(metadata))

    return(metadata)


def get_db_status_meta(local_db):
    """
    Return the meta status (STATE_DETAILS) of an instance
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

    # $GRID_HOME/bin/crsctl status resource ora.tstdb.db -v -n tlorad01 | grep STATE_DETAILS | cut -d "=" -f 2
    if local_db[-1].isdigit() :
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_db[:-1] + ".db -v -n tlorad01 | grep STATE_DETAILS | cut -d '=' -f 2"
    else:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + local_db + ".db -v -n tlorad01 | grep STATE_DETAILS | cut -d '=' -f 2"

    if debugme:
        msg = msg + tmp_cmd

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       err_msg = err_msg + ' Error[7]: srvctl module get_db_status_meta() error - retrieving STATE_DETAILS local_db: %s' % (local_db, sys.exc_info()[0])
       err_msg = err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
       raise Exception (err_msg)

    node_status=output.strip()                 #  Mounted (Closed)

    return(node_status)


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
          if "ONLINE" in node_status[i] or "INTERMEDIATE" in node_status[i]:
              node_status[i] = node_status[i].strip().split(" ")[0].strip().rstrip()
      elif "ONLINE" in item:
          node_status[i]=item.strip().split(" ")[0].strip().rstrip()
      elif "OFFLINE" in item:
          node_status[i]=item.strip().rstrip()
      i += 1

    if debugme:
        msg = msg + " get_db_status() exit. status %s" % (str(node_status))

    return(node_status)


def wait_for_status(vdb, vstatus, vstat_meta, vttw, vinst):
    """Compare database status of both nodes to expected status (vstatus). Loop in 2 second intervals until state obtained"""

    global my_err_msg
    global msg
    # take current time and add 5 minutes (5*60)
    # this will be the stop time if state isn't reached
    timeout =  time.time() + (60 * int(vttw))
    current_status = []
    vmeta = {}

    if vinst == 0:
        vobj = "database"
    else:
        vobj = "instance"

    # If vinst is 0 we're shutting down / starting up the whole db, not an instance ** different comparison.
    if vinst == 0:

        try:
          current_status = get_db_status(vdb)
          while (not all(item == vstatus for item in current_status) and (time.time() < timeout)):
            time.sleep(2)
            current_status = get_db_status(vdb)
        except:
            my_err_msg = my_err_msg + ' Error[8]: srvctl module wait_for_status() error - waiting for complete database status to change to %s excpetion: %s' % (vstatus, sys.exc_info()[0])
            my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
            raise Exception (my_err_msg)

    # else shutting down or starting an instance
    else:

      # Get the instances index number by subtracting 1 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
      vindex = int(vinst) - 1

      try:

        current_status = get_db_status(vdb)
        while (vstatus != current_status[vindex]) and (time.time() < timeout):
          time.sleep(2)
          current_status = get_db_status(vdb)

      except:
          my_err_msg = my_err_msg + ' Error[9]: srvctl module wait_for_status() error - waiting for instance status to change to %s last checked state: %s' % (vstatus, str(current_status))
          my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
          raise Exception (my_err_msg)

      # once instance state is reached, check meta info is reached. (it's a little slower)
      if vstat_meta:

          if debugme:
              msg = msg + "wait_for_status(%s, %s, %s, %s, %s) stat_meta loop " % (vdb, vstatus, vstat_meta, vttw, vinst)

          try:
              current_status = get_db_status_meta(vdb)
              while (vstat_meta != current_status) and (time.time() < timeout):
                  time.sleep(2)
                  current_status = get_db_status_meta(vdb)
          except:
              my_err_msg = my_err_msg + ' Error[9]: srvctl module wait_for_status() error - waiting for instance stat_meta to change to %s last seen: %s' % (vstat_meta, str(current_status[vindex]))
              my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
              raise Exception (my_err_msg)

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      my_err_msg = my_err_msg + " Error[10]: srvctl module wait_for_status() timed out waiting for %s %s status to change during %s. Time to wait was %s. Additional info %s and vstatus: %s status_meta: %s last checked: %s vinst: %s my_err_msg: %s" % ( vobj, vdb, vcmd, vttw, msg, vstatus, vstat_meta, current_status, vinst, my_err_msg )
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
  cmd_strng = ""
  vinst = 0
  exp_status_meta = ""
  exp_status = ""
  vstopt = ""

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

  if vinst is None:
      vinst = 0

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
      msg="Time to wait (ttw) not passed. Using default: %s." % (str(default_ttw))
  else:
      ttw=tmpttw

  vstopt = module.params["stopt"]

  if vstopt:
      orig_vstopt = vstopt
  elif not vstopt:
      if vcmd == "start" and vobj == "database":
          orig_vstopt = "open"

  if vcmd == "stop":
      if not vstopt: # stop database no parameter passed, set default (immediate)
        vstopt = "-stopoption immediate"
        orig_vstopt = "immediate"
      elif exists_in(vstopt in x for x in valid_inst_stopopts): # else if it was passed and is a valid stop option (except for "local")
        if vobj == "database" and vstopt == "local":
          msg = msg + "WARNING[1]: %s option invalid during %s %s. Option ignored." % (vstopt, vobj, vcmd)
        else:
          orig_vstopt = vstopt
          vstopt = "-stopoption " + vstopt
  elif vcmd == "start":
      if exists_in(orig_vstopt in x for x in valid_startopts): # else see if its a valid start option: open, mount, nomount
        vstopt = "-startoption " + orig_vstopt
      elif vojb == "database" and not orig_vstopt:
        vstopt = "-startoption open"
        orig_vstopt = "open"
        msg = msg + " database start requested with no startoption. Default startoption: 'open' used."
      elif vstopt:
        msg = msg + "WARNING[2]: '%s' invalid option to use with %s. Option ignored." % (vstopt, vcmd)

  # srvctl options. If it's valid use it
  # stop "eval", "force", "verbose" ||  "eval" for start only
  vopt = module.params["opt"]
  if vopt and exists_in(vopt in x for x in valid_srvctl_options):
      if vcmd == "stop":
          vopt = "-" + vopt
      elif vcmd == "start" and vopt == "eval":
          if vopt == "read only":
              vopt = "-" + '"' + "read only" + '"'
          elif vopt == "read write":
              vopt = "-" + '"' + "read wite" + '"'
          else:
              vopt = "-" + vopt
      else:
          msg = msg + "NOTICE [3]: %s option invalid for %s. Option ignored." % (vopt, vcmd)
          vopt=""

  # at this point there's been no change
  vchanged=False

  # collect information needed to proceed.
  if not grid_home:
      grid_home=get_gihome()
  if not node_number:
      node_number=get_node_num()
  if not oracle_home:
      oracle_home=get_orahome(vdb)

  # if operating on an instance but inst num not specified throw error
  if vobj == "instance" and vinst == 0:
      my_err_msg = my_err_msg + "Inst value not optional when executing commands against an instance. Please add valid instance number (ie. inst: 1) and try again."
      my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
      raise Exception (my_err_msg)

  # Set expected state (state column from crsstat) based on command and options
  # ora.tstdb.db      database       C ONLINE     INTERMEDIATE tlorad01   0  0 Restricted Access
  #                                               ^^^^^^^^^^^^                 ^^^^^^^^^^^^^^^^^
  if vcmd == "stop":
      exp_status = "OFFLINE"
      exp_status_meta = "Instance Shutdown"
  elif vcmd == "start":
      if orig_vstopt == "nomount":
          exp_status = "INTERMEDIATE"
          exp_status_meta = "Dismounted"
      elif orig_vstopt ==  "mount":
          exp_status = "INTERMEDIATE"
          exp_status_meta="Mounted (Closed)"
      elif orig_vstopt == "read only":
          exp_status = "ONLINE"
          exp_status_meta = "Open,Readonly"
      elif orig_vstopt == "read write":
          exp_status = "ONLINE"
          exp_status_meta = "Open"
      elif orig_vstopt == "open":
          exp_status = "ONLINE"
          exp_status_meta = "Open"
      elif orig_vstopt == "restrict":
          exp_status = "INTERMEDIATE"
          exp_status_meta = "Restricted Access"

  if debugme:
      if not node_number:
          node_num=int(get_node_num())
      tmp_status=get_db_status(vdb)
      if not node_number:
         indx_this_node = get_node_num()
      else:
         indx_this_node = node_number
      indx_this_node = indx_this_node - 1
      curr_stat=str(tmp_status[indx_this_node])
      my_err_msg = my_err_msg + " parameters passed: vdb: %s vcmd: %s exp_status: %s exp_status_meta: %s vobj: %s grid_home: %s node_number: %s vopt: %s oracle_home: %s get_db_status(): %s ttw: %s original status array: %s msg: %s stopt: %s" % (vdb, vcmd, exp_status, exp_status_meta, vobj, grid_home, node_number, vstopt, oracle_home, curr_stat, ttw, debugme_msg, msg, vstopt)
      msg = msg + my_err_msg

  # if database operation and all nodes are NOT already in the state we're looking for execute srvctl command
  current_status = get_db_status(vdb)
  if vobj == "database" and not all(item == exp_status for item in current_status):

          if debugme:
              my_err_msg = my_err_msg + cmd_strng
              msg = msg + my_err_msg

          # Execute the srvctl command for stop / start database
          #                                                                                                                         srvctl stop database -d tstdb -stopoption immediate
          if not vopt:
              cmd_strng = "export ORACLE_SID=" + vdb + str(node_number) + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " " + vstopt
          else:
              cmd_strng = "export ORACLE_SID=" + vdb + str(node_number) + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " " + vstopt + " " + vopt
          try:
              process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
              output, code = process.communicate()
          except:
              my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s meta sysinfo: %s' % (vcmd, vobj, vopt, sys.exc_info()[0])
              my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
              raise Exception (my_err_msg)

          #Once the command is executed wait for the proper state
          whatstatus = wait_for_status(vdb, exp_status, exp_status_meta, ttw, vinst) # exp_status_meta - not sure this is really needed. Added the code get_meta_data but not implemented. Gets db status: "Mounted (Closed)", "Open,Readonly", "Instance Shutdown", etc..
                                                                    # target, state, state details = OFFLINE, OFFLINE, Instance Shutdown or ONLINE, ONLINE, Open,Readonly, etc.
          vchanged = "True"

          if vcmd == "start":
              vcmd = "started"
          elif vcmd == "stop":
              vcmd = "stopped"

          if debugme:
              my_err_msg = my_err_msg + "after wait : whatstatus : %s and exp_status : %s " % (whatstatus, exp_status)
              msg = msg + "srvctl module complete. %s db %s. cmd %s meta %s extra: %s actual command : %s" % (vdb, exp_status, vcmd, my_err_msg, debugme_msg, cmd_strng) + my_err_msg

  # Command against an instance
  elif vobj == "instance":

          # get index for "other" instance (other than the one the cmd is against)
          if vinst == 1:
              # index for inst 2 = 1
              other_inst = 1
          else:
              # index for inst 1 = 0
              other_inst = 0

          # Get current database status, this returns status of all instances
          current_status = get_db_status(vdb)

          # Get index/instance number being checked (numbering starts at 0 for indexes so adjust down by one vs actual instance#)
          ckindx = int(vinst) - 1

          # if the instance is not already started or stopped execute the command
          # if the instance status is not already what it will be after the cmd
          if current_status[ckindx] != exp_status:

              if (current_status[other_inst] == "OFFLINE") and (vcmd == "stop"):
                  # if the force option wasn't passed (vopt is null) default to it
                  if not vopt:
                      vopt = "-force"
                  # or if options were passed, but none are force and one should be add it
                  elif "force" not in vopt:
                          vopt = vopt + " -force"
              # if stopping one instance and the other is up failover to it
              elif (current_status[other_inst] == "ONLINE") and (vcmd == "stop"):

                  if not vopt:
                      vopt = "-failover"
                  elif "failover" not in vopt:
                      vopt = vopt + " -failover"

              if debugme:
                  my_err_msg = my_err_msg + " >>> current_status contains : %s  current_status[other_inst] : %s exp_status: %s exp_status_meta: %s orig_vstopt: %s <<<" % (current_status, str(current_status[other_inst]), exp_status, exp_status_meta, orig_vstopt)
                  msg = msg + my_err_msg

              # create the command string (cmd_strng)
              # if no options to be appended to the command                                                                                      srvctl stop instance -d tstdb -i tstdb1 -failover
              if not vopt:
                  cmd_strng = "export ORACLE_SID=" + vdb + str(node_number) + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " -i " + vdb + str(vinst)
              else:
                  cmd_strng = "export ORACLE_SID=" + vdb + str(node_number) + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " -i " + vdb + str(vinst) + " " + vopt

              # Execute the srvctl command for stop / start database
              try:
                  process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
                  output, code = process.communicate()
              except: # Exception as e:
                  my_err_msg = my_err_msg + ' Error: srvctl module executing srvctl command error - executing srvctl command %s on %s with option %s %s meta sysinfo: %s' % (vcmd, vobj, vopt1, vopt2, sys.exc_info()[0])
                  my_err_msg = my_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], my_err_msg, sys.exc_info()[2])
                  raise Exception (my_err_msg)

              if debugme:
                  my_err_msg = my_err_msg + "output of inst_cmd : %s cmd_string %s exp_status: %s exp_status_meta: %s [88]" % (output, cmd_strng, exp_status, exp_status_meta)
                  msg = msg + my_err_msg

              #Once the command is executed wait for the proper state
              whatstatus = wait_for_status(vdb, exp_status, exp_status_meta, ttw, vinst) # exp_status_meta gets db status: "Mounted (Closed)", "Open,Readonly", "Instance Shutdown", etc..
                                                                                         # target, state, state details = OFFLINE, OFFLINE, Instance Shutdown or ONLINE, ONLINE, Open,Readonly, etc.
              vchanged = "True"

              if vcmd == "start":
                  vcmd = "started"
              elif vcmd == "stop":
                  vcmd = "stopped"

              if debugme:
                  my_err_msg = my_err_msg + "after wait_for_status : whatstatus : %s and exp_status : %s " % (whatstatus, exp_status)
                  msg = msg + "srvctl module complete. %s db %s. cmd %s meta %s extra: %s actual command : %s" % (vdb, exp_status, vcmd, my_err_msg, debugme_msg, cmd_strng) + my_err_msg

          else: # else instance already in state. No action required

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

              if debugme:
                  msg = msg + "srvctl module complete. %s db %s. actual command %s" % (vdb, vcmd, cmd_strng)
              else:
                  msg = msg + "srvctl module complete. %s db %s." % (vdb, vcmd)

              vchanged = "False"

              module.exit_json(msg=msg, ansible_facts=ansible_facts , changed=vchanged)

  # Database already stopped, or started)
  else:

      # current_db_status = get_db_status(vdb)

      if debugme:
          msg = msg + "[DB3] Request to %s %s database already %s . No action taken. meta: %s " % (vcmd, vdb, str(current_status), debugme_msg)
      else:
          msg = msg + "[DB4] Request to %s %s database already %s . No action taken." % (vcmd, vdb, str(current_status))

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

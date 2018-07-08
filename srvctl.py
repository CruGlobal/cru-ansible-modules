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
      become_user: "{{ remote_user }}"  Note: (1)
      register: src_facts
      when: master_node                 Note: (2)

    values:
       db: database name
      cmd: [ start | stop ]
      obj: [ database | instance ]
     inst: [ valid instance number ]
    stopt: (stop options): [ normal | immediate | abort ]
           (start options): [ open | mount | nomount ]
      opt: [ eval | force | verbose ]
      ttw: time to wait (in min) for status change after executing the command. Default 5.

      Notes:
        (1) Be sure to use 'become_user: oracle' else errors due to access privileges will cause the module to fail.
        (2) When master_node else it may try to execute on all nodes.

   WARNING: It's possible to start instance nomount, mount etc. but not to
            alter instance mount, or open. To do this using the srvctl module
            you MUST stop the instance then start instance mount, or start instance (open).
            It is possible to "sqlplus> alter database mount" on an instance.
            The status change will then be reflected in crsstat.

'''


# Global variables
# module parameters
debugme  = True
vdb_name = ""
vcmd     = ""
vobj     = ""
vinst    = -1
vstopt   = ""
vparam   = ""
default_ttw = 5
# environmentals
grid_home = ""
oracle_home = ""
node_number = ""
# info
msg = ""
custom_err_msg = ""
# debugging
debug_msg = ""


def debugging_info(new_msg):
    """Compiles debugging messages into one string."""
    global debug_msg

    if debug_msg:
        debug_msg = debug_msg + new_msg
    else:
        debug_msg = new_msg


def get_gihome():
    """Determine the Grid Home directory
       using ps -eo args
       returns string."""

    global grid_home

    try:
      process = subprocess.Popen(["/bin/ps -eo args | /bin/grep ocssd.bin | /bin/grep -v grep | /bin/awk '{print $1}'"], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
        custom_err_msg = 'Error [get_gihome()]: retrieving GRID_HOME : (%s,%s)' % (sys.exc_info()[0],code)
        module.fail_json(msg='ERROR: %s' % (custom_err_msg), changed=False)

    grid_home = (output.strip()).replace('/bin/ocssd.bin', '')

    if not grid_home:
         custom_err_msg = ' Error[get_gihome()]: srvctl module get_gihome() error - current grid_home value: [%s] full output: [%s]' % (grid_home, output)
         errcustom_err_msg_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
         raise Exception (custom_err_msg)

    return(grid_home)


def get_node_num():
    """Return current node number (int)"""
    global grid_home
    global debugme

    if not grid_home:
        grid_home = get_gihome()

    tmp_cmd = grid_home + "/bin/olsnodes -l -n | awk '{ print $2 }'"

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       custom_err_msg = ' Error[get_node_num()]: srvctl module get_node_num() error - retrieving node_number excpetion: %s' % (sys.exc_info()[0])
       custom_err_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], custom_err_msg, sys.exc_info()[2])
       raise Exception (custom_err_msg)

    if output.strip()[-1].isdigit() :
        node_number = int(output.strip()[-1])

    if debugme:
        tmp_msg = "get_node_num() executed this cmd: %s and determined node #: %s full output: %s" % (tmp_cmd, node_number, output)
        debugging_info(tmp_msg)

    return(node_number)


def get_orahome_oratab(db_name):
    """Return database Oracle home from /etc/oratab"""
    global my_err_msg

    tmp_cmd = "cat /etc/oratab | grep -m 1 " + db_name + " | grep -o -P '(?<=:).*(?<=:)' |  sed 's/\:$//g'"

    try:
        process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
        output, code = process.communicate()
    except:
       custom_err_msg = ' Error [get_orahome_oratab()]: retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       raise Exception (custom_err_msg)

    ora_home = output.strip()

    if not ora_home:
        custom_err_msg = 'Error[ get_orahome_oratab() ] ora_home null after f(x) execution.'
        custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise Exception (custom_err_msg)

    return(ora_home)


def get_orahome_procid(vdb):
    """Get database Oracle Home from the running process."""

    # get the pmon process id for the running database.
    # 10189  tstdb1
    try:
      vproc = str(commands.getstatusoutput("pgrep -lf _pmon_" + vdb + " | /bin/sed 's/ora_pmon_/ /; s/asm_pmon_/ /' | /bin/grep -v sed")[1])
    except:
      err_cust_err_msg = 'Error: get_orahome_procid() - pgrep lf pmon: (%s)' % (sys.exc_info()[0])
      err_cust_err_msg = cust_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], err_msg, sys.exc_info()[2])
      raise Exception (err_msg)

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

    return(ora_home)


def get_db_status(db_name):
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

    if not grid_home:
        grid_home = get_gihome()

    if not grid_home:
        custom_err_msg = ' Error[ get_db_status() ]: error determining grid_home from get_gihome() call. grid_home returned value: [%s]' % (grid_home)
        raise Exception (custom_err_msg)

    # check for special cases ASM and MGMTDB and see if db_name has digit (instance number), if so delete it. If not use it.
    if "ASM" in db_name:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.asm | grep STATE"
    elif "MGMTDB" in db_name:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora.mgmtdb | grep STATE"
    elif db_name[-1].isdigit() :
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + db_name[:-1] + ".db | grep STATE"  # these give state of each node
    else:
        tmp_cmd = grid_home + "/bin/crsctl status resource ora." + db_name + ".db | grep STATE"

    try:
      process = subprocess.Popen([tmp_cmd], stdout=PIPE, stderr=PIPE, shell=True)
      output, code = process.communicate()
    except:
       custom_err_msg = ' Error[ get_db_status() ]: srvctl module get_db_status() error - retrieving oracle_home excpetion: %s' % (sys.exc_info()[0])
       custom_err_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
       raise Exception (custom_err_msg)

    #  possible outputs:
    # ['STATE=OFFLINE', ' OFFLINE']   ['STATE=ONLINE on tlorad01', ' ONLINE on tlorad02']  ['ONLINE on tlorad01', 'OFFLINE']\r\n",
    node_status=output.strip().split(",")

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
        tmp_info = " get_db_status() exit. status %s" % (str(node_status))
        debugg_info(tmp_info)

    # this function returns a list of strings
    #                                              node1         node2
    # with the status of both (all) nodes: ie. ['INTERMEDIATE', 'OFFLINE']
    return(node_status)


def wait_for_it(vdb, vexp_status, vexp_status_meta, vttw, vinst):
    """Compare current database (vdb) status of all nodes
       to expected state (vstatus) looping in 2 second intervals
       until state is reached or until time runs out (ttw min)"""

    global msg
    global debugme
    # take current time and add 5 minutes (5*60)
    # this will be time to stop if database state isn't reached.
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
          while (not all(item == vexp_status for item in current_status) and (time.time() < timeout)):
            time.sleep(2)
            current_status = get_db_status(vdb)
        except:
            custom_err_msg = 'Error[ wait_for_it() ]: error vinst = 0 - waiting for database status to reach: %s current status: %s excpetion: %s' % (vexp_status, current_status, sys.exc_info()[0])
            custom_err_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
            raise Exception (custom_err_msg)

    # else shutting down or starting an instance
    else:

      # Get the instances index number by subtracting 1 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
      vindex = int(vinst) - 1

      try:

        current_status = get_db_status(vdb)
        while (vexp_status != current_status[vindex]) and (time.time() < timeout):
          time.sleep(2)
          current_status = get_db_status(vdb)

      except:
          custom_err_msg = 'Error[ wait_for_it() ]: error - waiting for instance state to change to %s last checked state: %s' % (vexp_status, str(current_status))
          custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
          raise Exception (custom_err_msg)

      # once instance state is reached, check state meta info is reached. (it's a little slower)
      # if vexp_status_meta state was passed in check for it.
      if vexp_status_meta:

          if debugme:
              msg = "wait_for_it(%s, %s, %s, %s, %s) vexp_status_meta loop " % (vdb, vstatus, vstat_meta, vttw, vinst)
              debug_info(msg)

          try:
              current_status = get_db_status_meta(vdb)
              while (vexp_status_meta != current_status) and (time.time() < timeout):
                  time.sleep(2)
                  current_status = get_db_status_meta(vdb)
          except:
              custom_err_msg = 'Error[ wait_for_it() ]: waiting for instance vexp_status_meta to change to %s last seen: %s' % (vexp_status_meta, str(current_status[vindex]))
              custom_err_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
              raise Exception (custom_err_msg)

    # Did it stop because it timed out or because it succeeded? Pass timeout info back to user, else continue
    if time.time() > timeout:
      custom_err_msg = " Error[ wait_for_it() ]: timed out waiting for %s %s status to change executing: %s. Time to wait (ttw): %s. Additional info expected status: %s and vstatus: %s status_meta: %s exp_status_meta: %s actual current status: %s vinst: %s custom_err_msg: %s" % ( vobj, vdb, vcmd, vttw, vexp_status, vexp_status_meta, current_status, vinst, custom_err_msg )
      custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
      raise Exception (custom_err_msg)
    else:
      return(0)


def is_opt_valid(vopt,vcmd):
    """Check that a given -stopoption / -startoption is valid. 0 valid 1 invalid."""
    # This is a limited list. The full functionality of srvctl start/stop options is beyond this module.
    valid_stop=('normal','immediate','abort') # 'local','transactional'
    valid_start=('open','mount') # 'nomount','force','restrict'

    if vcmd.lower() == "start":
        for item in valid_start:
            if item == vopt:
                return 0
    elif vcmd.lower() == "stop":
        for item in valid_stop:
            if item == vopt:
                return 0

    return 1


def mod_fail(vmsg,vchange):
    """Fail the module if called and pass out the error message"""
    ansible_facts={}

    if not vchange:
        vchange="False"

    if msg:
        module.fail_json(msg=vmsg,ansible_facts,changed=vchange)


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

  # local vars
  ansible_facts={}
  custom_err_msg = ""

  module = AnsibleModule(
      argument_spec = dict(
        db        = dict(required=True),        # database name to run srvctl against
        cmd       = dict(required=True),        # command to execute: start | stop
        obj       = dict(required=True),        # object to operate against: database | instance
        inst      = dict(required=False),       # instance number if object is an instance
        stopt     = dict(required=False),       # -startoption / -stopoption : open | mount | nomount
        param     = dict(required=False),       # extra parameter for stop (last running instance etc.): -force
        ttw       = dict(required=False)        # Time To Wait (ttw) for srvctl command to change database state
      ),
      supports_check_mode = False
  )

  # ===================== Start getting and checking module parameters ===========================
  # Get first 3 arguements passed from Ansible playbook
  vdb_name      = module.params["db"]
  vcmd          = module.params["cmd"]
  vobj          = module.params["obj"]

  # See if instance # was defined.
  try:
      vinst = module.params["inst"]
  except:
      vinst = -1 # -1 means no instance number specified.

  # Ensure if object is an instance and instance number wasn't defined raise exception
  if vobj == "instance" and vinst == -1:
      if not custom_err_msg:
          custom_err_msg = "ERROR[01]: object is : [%s] but instance number : [%s] not defined." % (vobj, vcmd)
      else:
          custom_err_msg = custom_err_msg + "ERROR[01]: '%s' option requires valid instance number." % (vobj, vcmd)
          custom_err_msg = custom_err_msg + "%s, %s, %s %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
          raise Exception (custom_err_msg)
  # Else if object is a database and instance number passed ignore the instance number and tell user.
  elif vobj == "database" and vinst != -1:
      if not msg:
          msg = " Passing an instance number when doing database operations is invalid. Instance number ignored."
      else:
          msg = msg + " Passing an instance number when doing database operations is invalid. Instance number ignored."

  # srvctl start/stop options (-startoption/-stopoption)
  try:
      vstopt = module.params["stopt"]
  except:
      vstopt = ""

  # parameter for stop instance / database to cause failover or stop if no other instance running: -force
  # -force - This parameter fails the running services over to another instance. Services dont failover if -force not specified!
  try:
      vparam = module.params["param"]
  except:
      vparam = ""

  # check if vparam is valid if passed in:
  if vparam and not vparam.lower() in ["eval","force","verbose"]:
      mod_fail("vparam not valid")
  else:
      vparam = "-" + vparam

  # Get time to Wait parameter (ttw)
  # If no ttw parameter passed use default.
  try:
      vttw = module.params["ttw"]
  except:
      vttw = default_ttw

  # if -startoption/-stopoption passed check that its valid
  if vstopt:
      vresult = is_opt_valid(vstopt,vcmd)  # 0 valid 1 invalid. NORMAL, TRANSACTIONAL LOCAL (not used), IMMEDIATE, or ABORT
      if result != 0:
          module.fail_json(msg='The stopt parameter passed was not part of the limited set of valid startoption/stopoptions this module handles.','Error: invalid stopt parameter.',changed=False)

  # at this point there's been no change to anything by the module
  vchanged=False

  # collect environmental information needed to proceed.
  if not grid_home:
      grid_home = get_gihome()        # if grid_home varaible isn't set get it.
  if not node_number:
      node_number = get_node_num()    # if the node number isn't set get it.
  if not oracle_home:
      oracle_home = get_orahome_procid(vdb)  # if oracle_home isn't set get it for this database.

  # If debugging save current state of all variables:
  if debugme:
      tmp = "vdb_name: [%s], vcmd: [%s], vobj: [%s], vinst: [%s], vparam: [%s], vstopt: [%s], vttw: [%s], grid_home: [%s], node_number: [%s], oracle_home: [%s]" % (vdb_name,vcmd,vobj,vinst,vparam,vstopt,vttw,grid_home,node_number,oracle_home)
      debug_info(tmp)

  # set the expected object state given command and object
  expected_status = set_exp_status(vcmd,vobj)

  # get the actual current state of the database
  current_status = get_db_status(vdb_name)

  # If running srvctl against a database ( comparison is different than instance )
  # check if all database instances are currently in the furture state:
  # ie. all instances already OFFLINE if stopping database. If not execute srvctl command.
  if vobj.lower() == "database" and not all(item == exp_status for item in current_status):

      # srvctl stop database -d tstdb -stopoption immediate -force   (-force for database stop parameter stops the database, its instances, its services, and any resources that depend on those services)
      vcmd = "export ORACLE_SID=" + vdb + str(node_number) + "; export ORACLE_HOME=" + oracle_home + "; " + oracle_home + "/bin/srvctl " + vcmd + " " + vobj + " -d " + vdb + " -" + vcmd + "option " + vstopt + " " + vparam

      try:
          process = subprocess.Popen([cmd_strng], stdout=PIPE, stderr=PIPE, shell=True)
          output, code = process.communicate()
      except:
              custom_err_msg = "Error: srvctl module executing srvctl command against database. vcmd: [%s]" % (vcmd)
              custom_err_msg = custom_err_msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
              raise Exception (custom_err_msg)

      #Once the command is executed wait for the database to reach the expected state
      whatstatus = wait_for_it(vdb, exp_status, exp_status_meta, ttw, vinst) # exp_status_meta - not sure this is really needed. Added the code get_meta_data but not implemented. Gets db status: "Mounted (Closed)", "Open,Readonly", "Instance Shutdown", etc..
                                                                                     # target, state, state details = OFFLINE, OFFLINE, Instance Shutdown or ONLINE, ONLINE, Open,Readonly, etc.
      vchanged = "True"
